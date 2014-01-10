#ifndef G_LOG_DOMAIN
# define G_LOG_DOMAIN "metacd-http"
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>

#include <metautils/lib/metautils.h>
#include <metautils/lib/metacomm.h>
#include <cluster/lib/gridcluster.h>
#include <server/network_server.h>
#include <server/transport_http.h>
#include <resolver/hc_resolver.h>
#include <meta2v2/meta2v2_remote.h>
#include <meta2v2/meta2_utils.h>
#include <meta2v2/autogen.h>
#include <meta2v2/generic.h>
#include <meta2/remote/meta2_services_remote.h>

#include <glib.h>

static struct http_request_dispatcher_s *dispatcher = NULL;
static struct network_server_s *server = NULL;

static gchar *nsname = NULL;
static struct hc_resolver_s *resolver = NULL;
static struct grid_lbpool_s *lbpool = NULL;

static struct grid_task_queue_s *admin_gtq = NULL;
static GThread *admin_thread = NULL;


// REPLY building --------------------------------------------------------------

static void
_append_status(GString *gstr, gint code, const gchar *msg)
{
	g_string_append_printf(gstr,
			"\"status\":%d,\"message\":\"%s\"", code, msg);
}

static GString*
_create_status(gint code, const gchar *msg)
{
	GString *gstr = g_string_sized_new(256);
	g_string_append_c(gstr, '{');
	_append_status(gstr, code, msg);
	g_string_append_c(gstr, '}');
	return gstr;
}

static GString*
_create_status_error(GError *e)
{
	GString *gstr;
	if (e) {
		gstr = _create_status(e->code, e->message);
		g_error_free(e);
	} else {
		gstr = _create_status(500, "unknown error");
	}
	return gstr;
}

static void
_append_url(GString *gstr, struct hc_url_s *url)
{
	if (!url)
		g_string_append_printf(gstr, "\"URL\":null");
	else
		g_string_append_printf(gstr,
				"\"URL\":{\"ns\":\"%s\",\"ref\":\"%s\",\"path\":\"%s\"}",
				none(hc_url_get(url, HCURL_NS)),
				none(hc_url_get(url, HCURL_REFERENCE)),
				none(hc_url_get(url, HCURL_PATH)));
}

static enum http_rc_e
_reply_no_handler(struct http_reply_ctx_s *rp)
{
	rp->set_body(NULL, 0);
	rp->set_status(404, "No handler found");
	rp->finalize();
	return HTTPRC_DONE;
}

static enum http_rc_e
_reply_json(struct http_reply_ctx_s *rp, int code, const gchar *msg, GString *gstr)
{
	rp->set_status(code, msg);
	if (gstr) {
		rp->set_body_gstr(gstr);
		rp->set_content_type("application/json");
	} else {
		rp->set_body(NULL, 0);
	}
	rp->finalize();
	return HTTPRC_DONE;
}

static enum http_rc_e
_reply_soft_error(struct http_reply_ctx_s *rp, GError *err)
{
	return _reply_json(rp, 200, "OK", _create_status_error(err));
}

static enum http_rc_e
_reply_format_error(struct http_reply_ctx_s *rp, GError *err)
{
	return _reply_json(rp, 400, "Bad request", _create_status_error(err));
}

static enum http_rc_e
_reply_method_error(struct http_reply_ctx_s *rp)
{
	return _reply_json(rp, 405, "Method not allowed", NULL);
}

static enum http_rc_e
_reply_success_json(struct http_reply_ctx_s *rp, GString *gstr)
{
	return _reply_json(rp, 200, "OK", gstr);
}


// URL parsing -----------------------------------------------------------------

struct url_action_s
{
	const gchar *token;
	GError* (*feed) (const gchar *v);
};

static GError*
_parse_args_pair(gchar **t, struct url_action_s *pa)
{
	gchar *escaped = g_uri_unescape_string(t[1], NULL);

	for (;;++pa) {
		if (!pa->token) {
			GError *e = pa->feed ? pa->feed(escaped) : NEWERROR(400,
					"Unexpected URI token [%s]", t[0]);
			g_free(escaped);
			return e;
		}
		else if (!g_ascii_strcasecmp(t[0], pa->token)) {
			GError *e = pa->feed ? pa->feed(escaped) : NULL;
			g_free(escaped);
			return e;
		}
	}

	g_free(escaped);
	g_assert_not_reached();
	return NULL;
}

static GError*
_parse_url(gchar **t, struct url_action_s *actions)
{
	GError *e = NULL;
	for (; !e && *t ; t+=2) {
		if (!*(t+1))
			e = NEWERROR(400, "Invalid URI");
		else
			e = _parse_args_pair(t, actions);
	}
	return e;
}

static GError*
_split_and_parse_url(const gchar *uri, struct url_action_s *actions)
{
	gchar **tokens = g_strsplit(uri, "/", 0);

	if (!tokens)
		return NEWERROR(500, "Internal error");
	GError *e = _parse_url(tokens, actions);
	g_strfreev(tokens);
	return e;
}


// DIR handler -----------------------------------------------------------------

static GError*
_extract_dir_url(const gchar *uri, gchar **rtype, struct hc_url_s **rurl)
{
	struct hc_url_s *url = hc_url_empty();
	gchar *type = NULL;

	GError* _on_ns(const gchar *v) {
		hc_url_set(url, HCURL_NS, v);
		return NULL;
	}
	GError* _on_ref(const gchar *v) {
		hc_url_set(url, HCURL_REFERENCE, *v ? v : NULL);
		return NULL;
	}
	GError* _on_path(const gchar *v) {
		hc_url_set(url, HCURL_PATH, *v ? v : NULL);
		return NULL;
	}
	GError* _on_type(const gchar *v) {
		if (type) {
			g_free(type);
			type = NULL;
		}
		if (!*v)
			return NEWERROR(400, "Empty type");
		type = g_strdup(v);
		return NULL;
	}

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"ref", _on_ref},
		{"path", _on_path},
		{"type", _on_type},
		{NULL, NULL}
	};
	GError *e = _split_and_parse_url(uri, actions);
	if (e) {
		if (type)
			g_free(type);
		hc_url_clean(url);
		return e;
	}
	if (!type) {
		hc_url_clean(url);
		return NEWERROR(400, "Missing TYPE");
	}

	*rtype = type;
	*rurl = url;
	return NULL;
}

static GString *
_pack_m1url_list(gchar **urlv)
{
	GString *gstr = g_string_new("{");
	_append_status(gstr, 200, "OK");
	g_string_append(gstr, ",\"srv\":[");
	for (gchar **v=urlv; v && *v ;v++) {
		struct meta1_service_url_s *m1 = meta1_unpack_url(*v);
		g_string_append_printf(gstr,
				"{\"seq\":%"G_GINT64_FORMAT",\"url\":\"%s\",\"args\":\"%s\"}",
				m1->seq, m1->host, m1->args);
		meta1_service_url_clean(m1);
		if (*(v+1))
			g_string_append_c(gstr, ',');
	}
	g_string_append(gstr, "]}");
	return gstr;
}

static enum http_rc_e
action_dir_list(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	GError *err = NULL;
	gchar **urlv = NULL;
	struct hc_url_s *url = NULL;
	gchar *type = NULL;

	if (g_ascii_strcasecmp(rq->cmd, "GET"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_dir_url(uri, &type, &url)))
		return _reply_format_error(rp, err);

	err = hc_resolve_reference_service(resolver, url, type, &urlv);
	hc_url_clean(url);
	g_free(type);

	if (NULL != err)
		return _reply_soft_error(rp, err);

	GString *packed = _pack_m1url_list(urlv);
	g_strfreev(urlv);
	return _reply_success_json(rp, packed);
}

static enum http_rc_e
action_dir_link(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	GError *err = NULL;
	struct hc_url_s *url = NULL;
	gchar *type = NULL;

	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_dir_url(uri, &type, &url)))
		return _reply_format_error(rp, err);

	g_free(type);
	hc_url_clean(url);
	return _reply_soft_error(rp, NEWERROR(501, "Not implemented"));
}

static enum http_rc_e
action_dir_unlink(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	GError *err = NULL;
	struct hc_url_s *url = NULL;
	gchar *type = NULL;

	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_dir_url(uri, &type, &url)))
		return _reply_format_error(rp, err);

	g_free(type);
	hc_url_clean(url);
	return _reply_soft_error(rp, NEWERROR(501, "Not implemented"));
}

static enum http_rc_e
handler_dir(gpointer u, struct http_request_s *rq, struct http_reply_ctx_s *rp)
{
	(void) u;

	gchar *module = rq->req_uri + 1;
	if (!g_str_has_prefix(module, "dir/"))
		return HTTPRC_NEXT;

	gchar *action = module + sizeof("dir/") - 1;
	if (g_str_has_prefix(action, "list/"))
		return action_dir_list(rq, rp, action + sizeof("list/") - 1);
	if (g_str_has_prefix(action, "link/"))
		return action_dir_link(rq, rp, action + sizeof("link/") - 1);
	if (g_str_has_prefix(action, "unlink/"))
		return action_dir_unlink(rq, rp, action + sizeof("unlink/") - 1);

	return _reply_no_handler(rp);
}


// META2 handler ---------------------------------------------------------------

static GError*
_extract_m2_url(const gchar *uri, struct hc_url_s **rurl)
{
	struct hc_url_s *url = hc_url_empty();

	GError* _on_ns(const gchar *v) {
		hc_url_set(url, HCURL_NS, *v ? v : NULL);
		return NULL;
	}
	GError* _on_ref(const gchar *v) {
		hc_url_set(url, HCURL_REFERENCE, *v ? v : NULL);
		return NULL;
	}
	GError* _on_path(const gchar *v) {
		hc_url_set(url, HCURL_PATH, *v ? v : NULL);
		return NULL;
	}
	GError* _on_version(const gchar *v) {
		hc_url_set(url, HCURL_VERSION, *v ? v : NULL);
		return NULL;
	}

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"ref", _on_ref},
		{"path", _on_path},
		{"version", _on_version},
		{NULL, NULL}
	};
	GError *e = _split_and_parse_url(uri, actions);
	if (e) {
		hc_url_clean(url);
		return e;
	}
	else {
		*rurl = url;
		return NULL;
	}
}

static GError*
_single_get_v1(const gchar *m2v, struct hc_url_s *url, GSList **beans)
{
	int rc;
	GError *err = NULL;
	struct metacnx_ctx_s cnx;

	struct meta1_service_url_s *m2 = meta1_unpack_url(m2v);
	metacnx_clear(&cnx);
	rc = metacnx_init_with_url(&cnx, m2->host, &err);
	meta1_service_url_clean(m2);

	if (!rc) {
		g_prefix_error(&err, "URL error: ");
		return err;
	}

	struct meta2_raw_content_v2_s *raw = NULL;
	rc = meta2_remote_stat_content_v2(&cnx, hc_url_get_id(url),
			hc_url_get(url, HCURL_PATH), &raw, &err);
	metacnx_close(&cnx);
	metacnx_clear(&cnx);

	if (!rc) {
		g_prefix_error(&err, "M2V1 error: ");
		return err;
	}

	*beans = m2v2_beans_from_raw_content_v2("fake", raw);
	meta2_raw_content_v2_clean(raw);
	return NULL;
}

static GError*
_multiple_get_v2_or_v1(gchar **m2v, struct hc_url_s *url, GSList **beans)
{
	for (; m2v && *m2v ;++m2v) {
		struct meta1_service_url_s *m2 = meta1_unpack_url(*m2v);
		GError *err = m2v2_remote_execute_GET(m2->host, NULL, url, 0, beans);
		meta1_service_url_clean(m2);

		if (!err) // M2V2 success
			return NULL;

		if (err->code == 404) {
			GRID_INFO("M2V2 error : probably a M2V1");
			g_clear_error(&err);
			err = _single_get_v1(*m2v, url, beans);
		}
		else {
			GRID_INFO("M2V2 error : (%d) %s", err->code, err->message);
			g_prefix_error(&err, "M2V2 error: ");
		}

		if (!err) // M2V1 success
			return NULL;
		if (err->code >= 400)
			return err;
		g_clear_error(&err);
	}

	return NEWERROR(500, "No META2 replied");
}

static void
_json_BEAN_only(GString *gstr, GSList *l, gconstpointer selector,
		void (*code)(GString*,gpointer))
{
	gboolean first = TRUE;

	for (; l ;l=l->next) {
		if (DESCR(l->data) != selector)
			continue;
		if (!first)
			g_string_append_c(gstr, ',');
		first = FALSE;
		code(gstr, l->data);
	}
}

static void
_json_alias_only(GString *gstr, GSList *l)
{
	void code(GString *g, gpointer bean) {
		g_string_append_printf(g,
				"{\"name\":\"%s\","
				"\"ver\":%"G_GINT64_FORMAT","
				"\"ctime\":%"G_GINT64_FORMAT","
				"\"system_metadata\":\"%s\","
				"\"header\":\"",
				ALIASES_get_alias(bean)->str,
				ALIASES_get_version(bean),
				ALIASES_get_ctime(bean),
				ALIASES_get_mdsys(bean)->str);
		metautils_gba_to_hexgstr(g, ALIASES_get_content_id(bean));
		g_string_append(g, "\"}");
	}
	_json_BEAN_only(gstr, l, &descr_struct_ALIASES, code);
}

static void
_json_headers_only(GString *gstr, GSList *l)
{
	void code(GString *g, gpointer bean) {
		g_string_append(g, "{\"id\":\"");
		metautils_gba_to_hexgstr(g, CONTENTS_HEADERS_get_id(bean));
		g_string_append_printf(g, "\",\"hash\":\"");
		metautils_gba_to_hexgstr(g, CONTENTS_HEADERS_get_hash(bean));
		g_string_append_printf(g, "\",\"size\":%"G_GINT64_FORMAT"}",
				CONTENTS_HEADERS_get_size(bean));
	}
	_json_BEAN_only(gstr, l, &descr_struct_CONTENTS_HEADERS, code);
}

static void
_json_contents_only(GString *gstr, GSList *l)
{
	void code(GString *g, gpointer bean) {
		g_string_append(g, "{\"hdr\":\"");
		metautils_gba_to_hexgstr(g, CONTENTS_get_content_id(bean));
		g_string_append_printf(g,
				"\",\"chunk\":\"%s\",\"pos\":\"%s\"}",
				CONTENTS_get_chunk_id(bean)->str,
				CONTENTS_get_position(bean)->str);
	}
	_json_BEAN_only(gstr, l, &descr_struct_CONTENTS, code);
}

static void
_json_chunks_only(GString *gstr, GSList *l)
{
	void code(GString *g, gpointer bean) {
		g_string_append_printf(g, "{\"id\":\"%s\",\"hash\":\"",
				CHUNKS_get_id(bean)->str);
		metautils_gba_to_hexgstr(g, CHUNKS_get_hash(bean));
		g_string_append_printf(g, "\",\"size\":%"G_GINT64_FORMAT"}",
				CHUNKS_get_size(bean));
	}
	_json_BEAN_only(gstr, l, &descr_struct_CHUNKS, code);
}

static GError *
_resolve_m2_and_get(struct hc_resolver_s *r, struct hc_url_s *u, GSList **beans)
{
	gchar **m2v = NULL;
	GError *err;

	if (NULL != (err = hc_resolve_reference_service(r, u, "meta2", &m2v))) {
		g_prefix_error(&err, "Resolution error: ");
		return err;
	}

	if (!*m2v)
		err = NEWERROR(CODE_CONTAINER_NOTFOUND, "No meta2 located");
	else
		err =  _multiple_get_v2_or_v1(m2v, u, beans);

	g_strfreev(m2v);
	return err;
}

static enum http_rc_e
action_m2_list(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) rq, (void) rp, (void) uri;
	return HTTPRC_ABORT;
}

static enum http_rc_e
action_m2_get(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	struct hc_url_s *url = NULL;
	GError *err;

	if (g_ascii_strcasecmp(rq->cmd, "GET"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_m2_url(uri, &url)))
		return _reply_format_error(rp, err);

	if (!hc_url_has(url, HCURL_NS) || !hc_url_has(url, HCURL_REFERENCE)
			|| !hc_url_has(url, HCURL_PATH)) {
		hc_url_clean(url);
		return _reply_format_error(rp, NEWERROR(400, "Partial URL"));
	}

	GSList *beans = NULL;
	if (NULL != (err = _resolve_m2_and_get(resolver, url, &beans))) {
		hc_url_clean(url);
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	// Prepare the header
	GString *gstr = g_string_sized_new(512);
	g_string_append_c(gstr, '{');
	_append_status(gstr, 200, "OK");
	g_string_append_c(gstr, ',');
	_append_url(gstr, url);
	g_string_append(gstr, ",\"aliases\":[");
	_json_alias_only(gstr, beans);
	g_string_append(gstr, "],\"headers\":[");
	_json_headers_only(gstr, beans);
	g_string_append(gstr, "],\"contents\":[");
	_json_contents_only(gstr, beans);
	g_string_append(gstr, "],\"chunks\":[");
	_json_chunks_only(gstr, beans);
	g_string_append_len(gstr, "]}", 2);

	_bean_cleanl2(beans);
	hc_url_clean(url);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
handler_m2(gpointer u, struct http_request_s *rq, struct http_reply_ctx_s *rp)
{
	(void) u;

	gchar *module = rq->req_uri + 1;
	if (!g_str_has_prefix(module, "m2/"))
		return HTTPRC_NEXT;

	gchar *action = module + sizeof("m2/") - 1;
	if (g_str_has_prefix(action, "get/"))
		return action_m2_get(rq, rp, action + sizeof("get/") - 1);
	if (g_str_has_prefix(action, "list/"))
		return action_m2_list(rq, rp, action + sizeof("list/") - 1);

	return _reply_no_handler(rp);
}


// Load-Balancing handler (stateless) ------------------------------------------

static GError*
_extract_lb_args(const gchar *args, gchar **rns, gchar **rtype)
{
	gchar *ns = NULL, *type = NULL;

	void _clean(gchar **ps) { if (*ps) g_free(*ps); *ps = NULL; }
	void _repl(gchar **ps, const gchar *v) { _clean(ps); *ps = g_strdup(v); }
	GError* _on_ns(const gchar *v) { _repl(&ns, v); return NULL; }
	GError* _on_type(const gchar *v) { _repl(&type, v); return NULL; }

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"type", _on_type},
		{NULL, NULL}
	};
	GError *err = _split_and_parse_url(args, actions);
	if (!err && (!ns || !*ns))
		err = NEWERROR(400, "Missing namespace");
	if (!err && (!type || !*type))
		err = NEWERROR(400, "Missing service type");
	if (err) {
		_clean(&ns);
		_clean(&type);
		return err;
	}
	*rtype = type;
	*rns = ns;
	return NULL;
}

static GString *
_lb_pack_and_free_srvinfo_list(const gchar *ns, const gchar *type, GSList *svc)
{
	GString *gstr = g_string_sized_new(512);
	gchar straddr[128];

	g_string_append_c(gstr, '{');
	_append_status(gstr, 200, "OK");
	g_string_append_printf(gstr, ",\"ns\":\"%s\",\"type\":\"%s\",\"srv\":[", ns, type);

	for (GSList *l = svc; l ;l=l->next) {
		if (l != svc)
			g_string_append_c(gstr, ',');
		struct service_info_s *si = l->data;
		grid_addrinfo_to_string(&(si->addr), straddr, sizeof(straddr));
		g_string_append_printf(gstr, "{\"addr\":\"%s\"}", straddr);
	}

	g_string_append(gstr, "]}");
	g_slist_free_full(svc, (GDestroyNotify)service_info_clean);
	return gstr;
}

static enum http_rc_e
action_lb_sl(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *args)
{
	gchar *ns = NULL, *type = NULL;
	struct service_info_s *si = NULL;

	GError *err;
	if (g_ascii_strcasecmp(rq->cmd, "GET"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_lb_args(args, &ns, &type)))
		return _reply_format_error(rp, err);

	struct grid_lb_iterator_s *iter = grid_lbpool_get_iterator(lbpool, type);
	if (!iter) {
		g_free(ns);
		g_free(type);
		return _reply_soft_error(rp, NEWERROR(460, "Type not managed"));
	}

	if (!grid_lb_iterator_next(iter, &si)) {
		g_free(ns);
		g_free(type);
		service_info_clean(si);
		return _reply_soft_error(rp, NEWERROR(CODE_POLICY_NOT_SATISFIABLE,
					"Type not available"));
	}

	GString *gstr = _lb_pack_and_free_srvinfo_list(ns, type,
			g_slist_prepend(NULL, si));

	g_free(ns);
	g_free(type);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
handler_lb(gpointer u, struct http_request_s *rq, struct http_reply_ctx_s *rp)
{
	(void) u;

	gchar *module = rq->req_uri + 1;
	if (!g_str_has_prefix(module, "lb/"))
		return HTTPRC_NEXT;

	gchar *action = module + sizeof("lb/") - 1;
	if (g_str_has_prefix(action, "sl/"))
		return action_lb_sl(rq, rp, action + sizeof("sl/") - 1);

	return _reply_no_handler(rp);
}


// Conscience handler ----------------------------------------------------------

struct cs_args_s
{
	gchar *ns;
	gchar *type;
};

static void
_cs_args_clear(struct cs_args_s *args)
{
	if (args->ns)
		g_free(args->ns);
	if (args->type)
		g_free(args->type);
}

static GError *
_extract_cs_url(const gchar *uri, struct cs_args_s *args)
{
	struct cs_args_s a;
	GError* _on_ns(const gchar *v) {
		metautils_str_replace(&(a.ns), v);
		return NULL;
	}
	GError* _on_type(const gchar *v) {
		metautils_str_replace(&(a.type), v);
		return NULL;
	}
	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"type", _on_type},
		{NULL, NULL}
	};

	memset(&a, 0, sizeof(struct cs_args_s));
	GError *e = _split_and_parse_url(uri, actions);
	if (e) {
		_cs_args_clear(&a);
		return e;
	}
	memcpy(args, &a, sizeof(struct cs_args_s));
	return NULL;
}

static void
_append_one_tag(GString* gstr, struct service_tag_s *tag)
{
	g_string_append_printf(gstr, "\"%s\":", tag->name);
	switch (tag->type) {
		case STVT_I64:
			g_string_append_printf(gstr, "%"G_GINT64_FORMAT, tag->value.i);
			return;
		case STVT_REAL:
			g_string_append_printf(gstr, "%f", tag->value.r);
			return;
		case STVT_BOOL:
			g_string_append(gstr, tag->value.b ? "true" : "false");
			return;
		case STVT_STR:
			g_string_append_printf(gstr, "\"%s\"", tag->value.s);
			return;
		case STVT_BUF:
			g_string_append_printf(gstr, "\"%.*s\"",
					(int) sizeof(tag->value.buf), tag->value.buf);
			return;
		case STVT_MACRO:
			g_string_append_printf(gstr, "\"${%s}${%s}\"",
					tag->value.macro.type, tag->value.macro.param);
			return;
	}
}

static void
_append_all_tags(GString *gstr, GPtrArray *tags)
{
	if (!tags || !tags->len)
		return;

	guint i, max;
	for (i=0,max=tags->len; i<max; ++i) {
		if (i)
			g_string_append_c(gstr, ',');
		_append_one_tag(gstr, tags->pdata[i]);
	}
}

static GString*
_cs_pack_and_free_srvinfo_list(GSList *svc)
{
	GString *gstr = g_string_sized_new(64 + 32 * g_slist_length(svc));
	gchar straddr[128];

	g_string_append_c(gstr, '{');
	_append_status(gstr, 200, "OK");
	g_string_append(gstr, ",\"srv\":[");

	for (GSList *l = svc; l ;l=l->next) {
		if (l != svc)
			g_string_append_c(gstr, ',');
		struct service_info_s *si = l->data;
		grid_addrinfo_to_string(&(si->addr), straddr, sizeof(straddr));
		g_string_append_printf(gstr,
				"{\"addr\":\"%s\",\"score\":%d,\"tags\":{",
				straddr, si->score.value);
		_append_all_tags(gstr, si->tags);
		g_string_append(gstr, "}}");
	}

	g_string_append(gstr, "]}");
	g_slist_free_full(svc, (GDestroyNotify)service_info_clean);
	return gstr;
}

static enum http_rc_e
action_cs_list(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	struct cs_args_s args = {NULL, NULL};
	GError *err = NULL;
	(void) rq;

	// URI unpacking + sanity checks
	if (g_ascii_strcasecmp(rq->cmd, "GET"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_cs_url(uri, &args)))
		return _reply_format_error(rp, err);
	if (!args.ns) {
		_cs_args_clear(&args);
		return _reply_format_error(rp, NEWERROR(400, "Missing namespace"));
	}
	if (!args.type) {
		_cs_args_clear(&args);
		return _reply_format_error(rp, NEWERROR(400, "Missing service type"));
	}

	// Action now
	GSList *services = list_namespace_services2(args.ns, args.type, &err);
	_cs_args_clear(&args);
	if (NULL != err) {
		g_slist_free_full(services, (GDestroyNotify)service_info_clean);
		g_prefix_error(&err, "Agent error: ");
		return _reply_soft_error(rp, err);
	}

	return _reply_success_json(rp, _cs_pack_and_free_srvinfo_list(services));
}

static enum http_rc_e
action_cs_reg(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	return HTTPRC_ABORT;
}

static enum http_rc_e
action_cs_lock(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	return HTTPRC_ABORT;
}

static enum http_rc_e
action_cs_unlock(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	return HTTPRC_ABORT;
}

static enum http_rc_e
action_cs_clear(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	struct cs_args_s args = {NULL, NULL};
	GError *err = NULL;

	// URI unpacking + sanity checks
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_cs_url(uri, &args)))
		return _reply_format_error(rp, err);
	if (!args.ns) {
		_cs_args_clear(&args);
		return _reply_format_error(rp, NEWERROR(400, "Missing namespace"));
	}
	if (!args.type) {
		_cs_args_clear(&args);
		return _reply_format_error(rp, NEWERROR(400, "Missing service type"));
	}

	// Action!
	gboolean rc = clear_namespace_services(args.ns, args.type, &err);
	_cs_args_clear(&args);
	if (!rc) {
		g_prefix_error(&err, "Agent error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, _create_status(200, "OK"));
}

static enum http_rc_e
handler_cs(gpointer u, struct http_request_s *rq, struct http_reply_ctx_s *rp)
{
	(void) u;

	gchar *module = rq->req_uri + 1;
	if (!g_str_has_prefix(module, "cs/"))
		return HTTPRC_NEXT;

	gchar *action = module + sizeof("cs/") - 1;
	if (g_str_has_prefix(action, "list/"))
		return action_cs_list(rq, rp, action + sizeof("list/") - 1);
	if (g_str_has_prefix(action, "reg/"))
		return action_cs_reg(rq, rp, action + sizeof("reg/") - 1);
	if (g_str_has_prefix(action, "lock/"))
		return action_cs_lock(rq, rp, action + sizeof("lock/") - 1);
	if (g_str_has_prefix(action, "unlock/"))
		return action_cs_unlock(rq, rp, action + sizeof("unlock/") - 1);
	if (g_str_has_prefix(action, "clear/"))
		return action_cs_clear(rq, rp, action + sizeof("clear/") - 1);

	return _reply_no_handler(rp);
}


// -----------------------------------------------------------------------------

static enum http_rc_e
handler_check(gpointer u, struct http_request_s *rq, struct http_reply_ctx_s *rp)
{
	(void) u;
	if (rq->req_uri[0] != '/') {
		rp->set_status(400, "Bad request");
		rp->finalize();
		return HTTPRC_DONE;
	}
	return HTTPRC_NEXT;
}


// Administrative tasks --------------------------------------------------------

static void
_task_expire_resolver(struct hc_resolver_s *r)
{
	guint count = hc_resolver_expire(r, time(0));
	if (count)
		GRID_DEBUG("Expired %u resolver entries", count);
}

static void
_task_reload_lbpool(struct grid_lbpool_s *p)
{
	GError *err;

	if (NULL != (err = gridcluster_reconfigure_lbpool(p))) {
		GRID_NOTICE("LBPOOL : reconfigure error : (%d) %s", err->code, err->message);
		g_clear_error(&err);
	}

	if (NULL != (err = gridcluster_reload_lbpool(p))) {
		GRID_NOTICE("LBPOOL : reload error : (%d) %s", err->code, err->message);
		g_clear_error(&err);
	}
}


// MAIN callbacks --------------------------------------------------------------

static void
_main_error(GError *err)
{
	GRID_ERROR("Action failure : (%d) %s", err->code, err->message);
	g_clear_error(&err);
	grid_main_set_status(1);
}

static void
grid_main_action(void)
{
	GError *err = NULL;

	if (NULL != (err = network_server_open_servers(server))) {
		_main_error(err);
		return;
	}

	if (!(admin_thread = grid_task_queue_run(admin_gtq, &err))) {
		g_prefix_error(&err, "Admin thread startup failure: ");
		_main_error(err);
		return;
	}

	if (NULL != (err = network_server_run(server))) {
		_main_error(err);
		return;
	}
}

static struct grid_main_option_s *
grid_main_get_options(void)
{
	static struct grid_main_option_s options[] = {
		{NULL, 0, {.i=0}, NULL}
	};

	return options;
}

static void
grid_main_set_defaults(void)
{
}

static void
grid_main_specific_fini(void)
{
	if (admin_thread) {
		grid_task_queue_stop(admin_gtq);
		g_thread_join(admin_thread);
		admin_thread = NULL;
	}
	if (admin_gtq) {
		grid_task_queue_destroy(admin_gtq);
		admin_gtq = NULL;
	}
	if (server) {
		network_server_close_servers(server);
		network_server_stop(server);
		network_server_clean(server);
		server = NULL;
	}
	if (dispatcher) {
		http_request_dispatcher_clean(dispatcher);
		dispatcher = NULL;
	}
	if (lbpool) {
		grid_lbpool_destroy(lbpool);
		lbpool = NULL;
	}
	if (resolver) {
		hc_resolver_destroy(resolver);
		resolver = NULL;
	}
}

static gboolean
grid_main_configure(int argc, char **argv)
{
	static struct http_request_descr_s all_requests[] = {
		{ "check", handler_check },
		{ "lb", handler_lb },
		{ "m2", handler_m2 },
		{ "cs", handler_cs },
		{ "dir", handler_dir },
		{ NULL, NULL }
	};

	if (argc != 2) {
		GRID_ERROR("Invalid parameter, expected : IP:PORT NS");
		return FALSE;
	}

	nsname = g_strdup(argv[1]);
	dispatcher = transport_http_build_dispatcher(NULL, all_requests);
	server = network_server_init();
	resolver = hc_resolver_create();
	lbpool = grid_lbpool_create(nsname);

	admin_gtq = grid_task_queue_create("admin");
	grid_task_queue_register(admin_gtq, 1,
			(GDestroyNotify)_task_expire_resolver, NULL, resolver);
	grid_task_queue_register(admin_gtq, 10,
			(GDestroyNotify)_task_reload_lbpool, NULL, lbpool);
	grid_task_queue_fire(admin_gtq);

	network_server_bind_host_lowlatency(server, argv[0],
			dispatcher, transport_http_factory);

	return TRUE;
}

static const char *
grid_main_get_usage(void)
{
	return "IP:PORT NS";
}

static void
grid_main_specific_stop(void)
{
	if (admin_gtq)
		grid_task_queue_stop(admin_gtq);
	if (server)
		network_server_stop(server);
}

static struct grid_main_callbacks main_callbacks =
{
	.options = grid_main_get_options,
	.action = grid_main_action,
	.set_defaults = grid_main_set_defaults,
	.specific_fini = grid_main_specific_fini,
	.configure = grid_main_configure,
	.usage = grid_main_get_usage,
	.specific_stop = grid_main_specific_stop,
};

int
main(int argc, char ** argv)
{
	return grid_main(argc, argv, &main_callbacks);
}

