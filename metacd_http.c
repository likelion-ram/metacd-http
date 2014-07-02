#ifndef G_LOG_DOMAIN
# define G_LOG_DOMAIN "metacd.http"
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
#include <json.h>

#ifndef RESOLVD_DEFAULT_TTL_SERVICES
#define RESOLVD_DEFAULT_TTL_SERVICES 3600
#endif

#ifndef RESOLVD_DEFAULT_MAX_SERVICES
#define RESOLVD_DEFAULT_MAX_SERVICES 200000
#endif

#ifndef RESOLVD_DEFAULT_TTL_CSM0
#define RESOLVD_DEFAULT_TTL_CSM0 0
#endif

#ifndef RESOLVD_DEFAULT_MAX_CSM0
#define RESOLVD_DEFAULT_MAX_CSM0 0
#endif

static struct http_request_dispatcher_s *dispatcher = NULL;
static struct network_server_s *server = NULL;

static gchar *nsname = NULL;
static struct hc_resolver_s *resolver = NULL;
static struct grid_lbpool_s *lbpool = NULL;

static struct grid_task_queue_s *admin_gtq = NULL;
static GThread *admin_thread = NULL;

// Configuration
static gint lb_refresh_delay = 10;
#define METACD_LB_ENABLED (lb_refresh_delay >= 0)

static guint dir_low_ttl =  RESOLVD_DEFAULT_TTL_SERVICES;
static guint dir_low_max =  RESOLVD_DEFAULT_MAX_SERVICES;
static guint dir_high_ttl = RESOLVD_DEFAULT_TTL_CSM0;
static guint dir_high_max = RESOLVD_DEFAULT_MAX_CSM0;

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
		if (!pa->token) { // reached the last expectable token, none matched
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
action_dir_flush_low(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	hc_resolver_flush_services(resolver);
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_dir_flush_high(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	hc_resolver_flush_csm0(resolver);
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_dir_set_max_high(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	hc_resolver_set_max_csm0(resolver, atoi(uri));
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_dir_set_max_low(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	hc_resolver_set_max_services(resolver, atoi(uri));
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_dir_set_ttl_high(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	hc_resolver_set_ttl_csm0(resolver, atoi(uri));
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_dir_set_ttl_low(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;
	if (g_ascii_strcasecmp(rq->cmd, "POST"))
		return _reply_method_error(rp);
	hc_resolver_set_ttl_services(resolver, atoi(uri));
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_dir_status(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	(void) uri;

	if (g_ascii_strcasecmp(rq->cmd, "GET"))
		return _reply_method_error(rp);

	struct hc_resolver_stats_s s;
	memset(&s, 0, sizeof(s));
	hc_resolver_info(resolver, &s);

	GString *gstr = g_string_new("{");
	g_string_append_printf(gstr, " \"clock\":%lu,", s.clock);
	g_string_append_printf(gstr, " \"csm0\":{"
			"\"count\":%"G_GINT64_FORMAT",\"max\":%u,\"ttl\":%lu},",
			s.csm0.count, s.csm0.max, s.csm0.ttl);
	g_string_append_printf(gstr, " \"meta1\":{"
			"\"count\":%"G_GINT64_FORMAT",\"max\":%u,\"ttl\":%lu}",
			s.services.count, s.services.max, s.services.ttl);
	g_string_append_c(gstr, '}');
	return _reply_success_json(rp, gstr);
}


// META2 handler ---------------------------------------------------------------

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

static void
_json_dump_all_beans(GString *gstr, struct hc_url_s *url, GSList *beans)
{
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
}

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
	GError* _on_size(const gchar *v) {
		hc_url_set_option(url, "size", *v ? v : NULL);
		return NULL;
	}
	GError* _on_stgpol(const gchar *v) {
		hc_url_set_option(url, "stgpol", *v ? v : NULL);
		return NULL;
	}
	GError* _on_verpol(const gchar *v) {
		hc_url_set_option(url, "verpol", *v ? v : NULL);
		return NULL;
	}

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"ref", _on_ref},
		{"path", _on_path},
		{"version", _on_version},
		{"size", _on_size},
		{"stgpol", _on_stgpol},
		{"verpol", _on_verpol},
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

static GError *
_resolve_m2_and_do(struct hc_resolver_s *r, struct hc_url_s *u,
		GError* (*hook) (struct meta1_service_url_s *m2))
{
	gchar **m2v = NULL;
	GError *err;

	if (NULL != (err = hc_resolve_reference_service(r, u, "meta2", &m2v))) {
		g_prefix_error(&err, "Resolution error: ");
		return err;
	}

	if (!*m2v)
		err = NEWERROR(CODE_CONTAINER_NOTFOUND, "No meta2 located");
	else {
		for (; m2v && *m2v ;++m2v) {
			struct meta1_service_url_s *m2 = meta1_unpack_url(*m2v);
			err = hook(m2);
			meta1_service_url_clean(m2);

			if (!err)
				goto exit;

			GRID_INFO("M2V2 error : (%d) %s", err->code, err->message);
			g_prefix_error(&err, "M2V2 error: ");

			if (err->code >= 400)
				goto exit;
			g_clear_error(&err);
		}
		if (!err)
			err = NEWERROR(500, "No META2 replied");
	}
exit:
	g_strfreev(m2v);
	return err;
}

static GError*
_single_get_v1(struct meta1_service_url_s *m2, struct hc_url_s *url, GSList **beans)
{
	int rc;
	GError *err = NULL;
	struct metacnx_ctx_s cnx;

	metacnx_clear(&cnx);
	rc = metacnx_init_with_url(&cnx, m2->host, &err);

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

static enum http_rc_e
action_m2_list(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	// TODO manage snapshot ?

	GError *err;
	GSList *beans = NULL;
	GError* hook(struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_LIST(m2->host, NULL, url, 0, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(1024);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_get(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	if (NULL == hc_url_get_option_value(url, "size"))
		return _reply_format_error(rp, NEWERROR(400, "Missing size"));

	GError *err;
	GSList *beans = NULL;
	GError* hook(struct meta1_service_url_s *m2) {
		GError *e = m2v2_remote_execute_GET(m2->host, NULL, url, 0, &beans);
		if (!e || e->code == 404)
			return e;
		GRID_INFO("M2V2 error : probably a M2V1");
		g_clear_error(&e);
		return _single_get_v1(m2, url, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_beans(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	gchar *end = NULL;
	const gchar *strpol = hc_url_get_option_value(url, "policy");
	const gchar *strsize = hc_url_get_option_value(url, "size");
	gint64 size = g_ascii_strtoll(strsize, &end, 10);

	GError *err;
	GSList *beans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_BEANS(m2->host, NULL, url, strpol, size, 0, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_create(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		struct m2v2_create_params_s param = {
			hc_url_get_option_value(url, "stgpol"),
			hc_url_get_option_value(url, "verpol"),
		};
		return m2v2_remote_execute_CREATE(m2->host, NULL, url, &param);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_destroy(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_DESTROY(m2->host, NULL, url, 0);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_open(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_OPEN(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_close(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_CLOSE(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_has(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_HAS(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_dedup(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	gboolean first = TRUE;
	GString *gstr = g_string_new("{\"\":[");
	GError* hook (struct meta1_service_url_s *m2) {
		gchar *msg = NULL;
		GError *e = m2v2_remote_execute_DEDUP(m2->host, NULL, url, 0, &msg);
		if (msg) {
			if (first) {
				g_string_append_c(gstr, ',');
				first = FALSE;
			}
			g_string_append_c(gstr, '"');
			g_string_append(gstr, msg); // TODO escpae this!
			g_string_append_c(gstr, '"');
		}
		return e;
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_string_free(gstr, TRUE);
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	g_string_append(gstr, "]}");
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_purge(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GSList *beans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_PURGE(m2->host, NULL, url, FALSE, 10.0, 10.0, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_exitelection(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_EXITELECTION(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_stgpol(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	const gchar *stgpol = hc_url_get_option_value(url, "stgpol");
	if (!stgpol)
		return _reply_format_error(rp, NEWERROR(400, "Missing storage policy"));

	GSList *beans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_STGPOL(m2->host, NULL, url, stgpol, &beans);
	}
	GError *err;
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_touch(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError* hook (struct meta1_service_url_s *m2) {
		if (hc_url_has(url, HCURL_PATH))
			return m2v2_remote_touch_content(m2->host, NULL, url);
		else
			return m2v2_remote_touch_container_ex(m2->host, NULL, url, 0);
	}
	GError *err;
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_copy(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_COPY(m2->host, NULL, url, "NYI");
	}
	GError *err;
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	return _reply_success_json(rp, NULL);
}

typedef GError* (*jbean_mapper) (struct json_object*, gpointer*);

static GError*
_alias2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError*
_header2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError*
_content2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError*
_chunk2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError *
_jarray_to_beans (GSList **out, struct json_object *jv, jbean_mapper map)
{
	if (!json_object_is_type(jv, json_type_array))
		return NEWERROR(400, "Invalid JSON, exepecting array of beans");
	int vlen = json_object_array_length (jv);
	for (int i=0; i<vlen ;++i) {
		struct json_object *j = json_object_array_get_idx (jv, i);
		if (!json_object_is_type (j, json_type_object))
			return NEWERROR(400, "Invalid JSON for a bean");
		gpointer bean = NULL;
		GError *err = map(j, &bean);
		g_assert((bean != NULL) ^ (err != NULL));
		if (err)
			return err;
		*out = g_slist_prepend(*out, bean);
	}
	return NULL;
}

static GError *
_jbody_to_beans(GSList **beans, struct json_object *jbody, const gchar *k)
{
	if (!json_object_is_type(jbody, json_type_object))
		return NEWERROR(400, "Bad format");

	struct json_object *jbeans = json_object_object_get(jbody, k);
	if (!jbeans)
		return NEWERROR(400, "Bad format, no bean");
	if (!json_object_is_type(jbody, json_type_object)) {
		json_object_put (jbeans);
		return NEWERROR(400, "Bad format");
	}

	static gchar* title[] = { "alias", "header", "content", "chunk", NULL };
	static jbean_mapper mapper[] = { _alias2bean, _header2bean, _content2bean,
		_chunk2bean };

	GError *err = NULL;
	gchar **ptitle;
	jbean_mapper *pmapper;
	for (ptitle=title,pmapper=mapper; *ptitle ;++ptitle,++pmapper) {
		struct json_object *jv = json_object_object_get (jbeans, *ptitle);
		if (!jv)
			continue;
		err = _jarray_to_beans(beans, jv, *pmapper);
		json_object_put (jv);
		if (err != NULL)
			break;
	}

	json_object_put (jbeans);
	return err;
}

static GError *
_m2_json_put(struct hc_url_s *url, struct json_object *jbody, GSList **out)
{
	GSList *ibeans = NULL, *obeans = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans(&ibeans, jbody, "beans"))) {
		_bean_cleanl2 (ibeans);
		ibeans = NULL;
		return err;
	}

	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_PUT(m2->host, NULL, url, ibeans, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (ibeans);
	g_assert((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static GError *
_m2_json_spare(struct hc_url_s *url, struct json_object *jbody, GSList **out)
{
	GSList *notin = NULL, *broken = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans(&notin, jbody, "notin"))) {
		_bean_cleanl2 (notin);
		_bean_cleanl2 (broken);
		return err;
	}
	if (NULL != (err = _jbody_to_beans(&broken, jbody, "broken"))) {
		_bean_cleanl2 (notin);
		_bean_cleanl2 (broken);
		return err;
	}

	GSList *obeans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_SPARE(m2->host, NULL, url,
				hc_url_get_option_value(url, "stgpol"),
				notin, broken, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (broken);
	_bean_cleanl2 (notin);
	g_assert((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static GError *
_m2_json_append(struct hc_url_s *url, struct json_object *jbody, GSList **out)
{
	GSList *ibeans = NULL, *obeans = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans(&ibeans, jbody, "beans"))) {
		_bean_cleanl2 (ibeans);
		ibeans = NULL;
		return err;
	}

	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_APPEND(m2->host, NULL, url, ibeans, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (ibeans);
	g_assert((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static GError *
_m2_json_overwrite(struct hc_url_s *url, struct json_object *jbody)
{
	(void) url, (void) jbody;
	return NEWERROR(500, "Not implemented");
}

static enum http_rc_e
action_m2_put(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_put(url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (err)
		return _reply_soft_error(rp, err);

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_spare(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_spare(url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (err)
		return _reply_soft_error(rp, err);

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_append(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_append(url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (err)
		return _reply_soft_error(rp, err);

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_overwrite(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_overwrite(url, jbody);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (!err)
		return _reply_success_json(rp, NULL);
	else
		return _reply_soft_error(rp, err);
}

struct m2_action_s
{
	const gchar *method;
	const gchar *prefix;
	enum http_rc_e (*hook) (struct http_request_s *rq,
			struct http_reply_ctx_s *rp, struct hc_url_s *url);
	unsigned int expect_path;
} m2_actions[] = {
	{ "GET",  "get/",          action_m2_get,          1},
	{ "GET",  "beans/",        action_m2_beans,        1},
	{ "GET",  "list/",         action_m2_list,         0},
	{ "GET",  "has/",          action_m2_has,          0},
	{ "POST", "create/",       action_m2_create,       0},
	{ "POST", "destroy/",      action_m2_destroy,      0},
	{ "POST", "open/",         action_m2_open,         0},
	{ "POST", "close/",        action_m2_close,        0},
	{ "POST", "purge/",        action_m2_purge,        0},
	{ "POST", "dedup/",        action_m2_dedup,        0},
	{ "POST", "stgpol/",       action_m2_stgpol,       0},
	{ "POST", "exitelection/", action_m2_exitelection, 0},
	{ "POST", "touch/",        action_m2_touch,        0},
	{ "POST", "copy/",         action_m2_copy,         0},
	{ "POST", "put/",          action_m2_put,          1},
	{ "POST", "spare/",        action_m2_spare,        1},
	{ "POST", "append/",       action_m2_append,       1},
	{ "POST", "overwrite/",    action_m2_overwrite,    1},
	{ NULL, NULL, NULL, 0}
};

static enum http_rc_e
action_meta2(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	for (struct m2_action_s *pa = m2_actions; pa->prefix ;++pa) {
		if (!g_str_has_prefix(uri, pa->prefix))
			continue;

		uri += strlen(pa->prefix);
		if (0 != strcmp(rq->cmd, pa->method))
			return _reply_method_error(rp);

		GError *err;
		struct hc_url_s *url = NULL;
		if (NULL != (err = _extract_m2_url(uri, &url)))
			return _reply_format_error(rp, err);

		if (!hc_url_has(url, HCURL_NS) || !hc_url_has(url, HCURL_REFERENCE)) {
			hc_url_clean(url);
			return _reply_format_error(rp, NEWERROR(400, "Missing NS/REF in url"));
		}
		if (pa->expect_path && !hc_url_has(url, HCURL_PATH)) {
			hc_url_clean(url);
			return _reply_format_error(rp, NEWERROR(400, "Missing PATH in url"));
		}

		enum http_rc_e e = pa->hook(rq, rp, url);
		hc_url_clean(url);
		return e;
	}
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

	if (!METACD_LB_ENABLED) {
		GError *err = NEWERROR(CODE_UNAVAILABLE,
				"Load-balancer disabled by configuration");
		return _reply_json(rp, CODE_UNAVAILABLE,
				"Service unavailable", _create_status_error(err));
	}

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

// Misc. handlers --------------------------------------------------------------

struct action_s
{
	const gchar *prefix;
	enum http_rc_e (*hook) (struct http_request_s *rq,
			struct http_reply_ctx_s *rp, const gchar *uri);
} actions[] = {
	{"lb/sl/",               action_lb_sl},

	{"m2/",                  action_meta2},

	{"dir/list/",            action_dir_list},
	{"dir/link/",            action_dir_link},
	{"dir/unlink/",          action_dir_unlink},
	{"dir/status/",          action_dir_status},
	{"dir/flush/high/",      action_dir_flush_high},
	{"dir/flush/low/",       action_dir_flush_low},
	{"dir/set/ttl/high/",    action_dir_set_ttl_high},
	{"dir/set/ttl/low/",     action_dir_set_ttl_low},
	{"dir/set/max/high/",    action_dir_set_max_high},
	{"dir/set/max/low/",     action_dir_set_max_low},

	{"cs/list/",             action_cs_list},
	{"cs/reg/",              action_cs_reg},
	{"cs/lock/",             action_cs_lock},
	{"cs/unlock/",           action_cs_unlock},
	{"cs/clear/",            action_cs_clear},

	{NULL,NULL}
};

static enum http_rc_e
handler_action(gpointer u, struct http_request_s *rq, struct http_reply_ctx_s *rp)
{
	(void) u;
	for (struct action_s *pa = actions; pa->prefix ;++pa) {
		if (g_str_has_prefix(rq->req_uri + 1, pa->prefix))
			return pa->hook(rq, rp, rq->req_uri + 1 + strlen(pa->prefix));
	}
	return _reply_no_handler(rp);
}


// Administrative tasks --------------------------------------------------------

static void
_task_expire_resolver(struct hc_resolver_s *r)
{
	hc_resolver_set_now(r, time(0));
	guint count = hc_resolver_expire(r);
	if (count)
		GRID_DEBUG("Expired %u resolver entries", count);
	count = hc_resolver_purge(r);
	if (count)
		GRID_DEBUG("Purged %u resolver ", count);
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
		{"LbRefresh", OT_INT, {.i=&lb_refresh_delay},
			"Interval between load-balancer service refreshes (seconds)\n"
				"\t\t-1 to disable, 0 to never refresh"},
		{"DirLowTtl", OT_UINT, {.u=&dir_low_ttl},
			"Directory 'low' (meta1) TTL for cache elements"},
		{"DirLowMax", OT_UINT, {.u=&dir_low_max},
			"Directory 'low' (meta1) MAX cached elements"},
		{"DirHighTtl", OT_UINT, {.u=&dir_high_ttl},
			"Directory 'high' (cs+meta0) TTL for cache elements"},
		{"DirHighMax", OT_UINT, {.u=&dir_high_max},
			"Directory 'high' (cs+meta0) MAX cached elements"},
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
		{ "action", handler_action },
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

	if (resolver) {
		hc_resolver_set_ttl_csm0(resolver, dir_high_ttl);
		hc_resolver_set_max_csm0(resolver, dir_high_max);
		hc_resolver_set_ttl_services(resolver, dir_low_ttl);
		hc_resolver_set_max_services(resolver, dir_low_max);
		GRID_INFO("RESOLVER limits HIGH[%u/%u] LOW[%u/%u]",
				dir_high_max, dir_high_ttl, dir_low_max, dir_low_ttl);
	}

	admin_gtq = grid_task_queue_create("admin");
	grid_task_queue_register(admin_gtq, 1,
			(GDestroyNotify)_task_expire_resolver, NULL, resolver);
	if (METACD_LB_ENABLED)
		grid_task_queue_register(admin_gtq, (guint)lb_refresh_delay,
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

