/*
Metacd-http, a http proxy for redcurrant's services
Copyright (C) 2014 Jean-Francois Smigielski

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

enum {
	TOK_NS   = 0x0001,
	TOK_REF  = 0x0002,
	TOK_PATH = 0x0004,
	TOK_TYPE = 0x0008,

	TOK_SIZE    = 0x0010,
	TOK_ACTION  = 0x0020,
	TOK_VERSION = 0x0040,
	TOK_STGPOL  = 0x0080,
	TOK_VERPOL  = 0x0100,
	TOK_TAGK    = 0x0200,
	TOK_TAGV    = 0x0400,
	TOK_STGCLS  = 0x0800,
	TOK_KEY     = 0x1000,
};

enum {
	FLAG_NOEMPTY = 0x0001,
};

struct req_uri_s {
	const gchar *original;
	gchar *path;
	gchar *query;
	gchar *fragment;
};

struct req_args_s {
	// path tokens
	gchar *ns;
	gchar *ref;
	gchar *path;
	gchar *type;

	// query arguments
	gchar *tagk;
	gchar *tagv;
	gchar *size;
	gchar *action;
	gchar *version;
	gchar *stgpol;
	gchar *verpol;
	gchar *stgcls;
	gchar *key;

	struct hc_url_s *url;

	// The portion of the URI after the prefix
	const gchar *uri;

	struct req_uri_s *req_uri;
	struct http_request_s *rq;
	struct http_reply_ctx_s *rp;

	guint32 flags;
};

struct req_action_s {
	const gchar *method;
	const gchar *prefix;
	enum http_rc_e (*hook) (const struct req_args_s * args);

	guint32 path;
	guint32 query;
	guint32 query_opt;
};

struct url_action_s {
	const gchar *token;
	gchar **to;
};

//------------------------------------------------------------------------------

static void
_req_uri_extract_components (const gchar * str, struct req_uri_s *uri)
{
	gchar *pq = strchr (str, '?');
	gchar *pa = pq ? strchr (pq, '#') : strchr (str, '#');

	uri->original = str;
	if (pq || pa)
		uri->path = g_strndup (uri->original, (pq ? pq : pa) - str);
	else
		uri->path = g_strdup (uri->original);

	if (pq) {
		if (pa)
			uri->query = g_strndup (pq + 1, pa - pq);
		else
			uri->query = g_strdup (pq + 1);
	} else
		uri->query = g_strdup("");

	if (pa)
		uri->fragment = g_strdup (pa + 1);
	else
		uri->fragment = g_strdup("");
}

static void
_req_uri_free_components (struct req_uri_s *uri)
{
	metautils_str_clean (&uri->path);
	metautils_str_clean (&uri->query);
	metautils_str_clean (&uri->fragment);
}

//------------------------------------------------------------------------------

static GError *
_parse_pair (gchar ** t, struct url_action_s *pa)
{
	if (0 == strlen (t[0]))
		return BADREQ ("empty key");
	if (0 == strlen (t[1]))
		return BADREQ ("empty value");

	gchar *escaped = g_uri_unescape_string (t[1], NULL);

	for (;; ++pa) {
		if (!pa->token) {		// reached the last expectable token, none matched
			GError *e = BADREQ ("Unexpected URI token [%s]", t[0]);
			g_free (escaped);
			return e;
		} else if (!g_ascii_strcasecmp (t[0], pa->token)) {
			metautils_str_reuse (pa->to, escaped);
			return NULL;
		}
	}

	g_free (escaped);
	g_assert_not_reached ();
	return NULL;
}

static GError *
_req_path_extract_tokens (struct req_args_s *args)
{
	struct url_action_s actions[] = {
		{"ns", &args->ns},
		{"ref", &args->ref},
		{"path", &args->path},
		{"type", &args->type},
		{NULL, NULL}
	};

	gchar **tokens = g_strsplit (args->uri, "/", 0);
	if (!tokens)
		return NEWERROR (500, "Internal error");

	GError *e = NULL;
	for (gchar ** t = tokens; !e && *t; t += 2) {
		if (!*(t + 1))
			e = NEWERROR (400, "Invalid URI");
		else
			e = _parse_pair (t, actions);
	}
	g_strfreev (tokens);
	return e;
}

static GError *
_req_query_extract_args (struct req_args_s *args)
{
	struct url_action_s actions[] = {
		{"action", &args->action},
		{"tagk", &args->tagk},
		{"tagv", &args->tagv},
		{"size", &args->size},
		{"stgcls", &args->stgcls},
		{"stgpol", &args->stgpol},
		{"verpol", &args->verpol},
		{"version", &args->version},
		{"key", &args->key},
		{NULL, NULL}
	};

	gchar **pairs = g_strsplit (args->req_uri->query, "&", 0);
	if (!pairs)
		return NEWERROR (500, "Internal error");

	GError *e = NULL;
	for (gchar **pair = pairs; !e && *pair; pair++) {
		if (strchr(*pair, '=')) {
			gchar **kv = g_strsplit(*pair, "=", 2);
			e = _parse_pair (kv, actions);
			g_strfreev(kv);
		} else {
			gchar *kv[3] = { *pair, "", NULL };
			e = _parse_pair (kv, actions);
		}
	}
	g_strfreev (pairs);
	return e;
}

static GError *
_req_path_check_tokens (struct req_args_s *args, guint32 flags)
{
#define PRESENCE(F,T) do { \
	if ((flags & TOK_##F) && !args->T) \
		return BADREQ("Missing %s", #F); \
} while (0)

	if (args->ns)
		hc_url_set (args->url, HCURL_NS, args->ns);
	if (args->ref)
		hc_url_set (args->url, HCURL_REFERENCE, args->ref);
	if (args->path)
		hc_url_set (args->url, HCURL_PATH, args->path);

	PRESENCE (NS, ns);
	PRESENCE (REF, ref);
	PRESENCE (PATH, path);
	PRESENCE (TYPE, type);

	// All the fields are present ... now check their value
	if (flags & TOK_NS || args->ns) {
		if (!validate_namespace (hc_url_get (args->url, HCURL_NSPHYS)))
			return NEWERROR (CODE_NAMESPACE_NOTMANAGED, "Invalid NS");
	}
	if (flags & TOK_TYPE) {
		if (!validate_srvtype (args->type))
			return NEWERROR (CODE_NAMESPACE_NOTMANAGED, "Invalid TYPE");
	}
	return NULL;
#undef PRESENCE
}

static GError *
_req_query_check_tokens (struct req_args_s *args,
		guint32 mandatory, guint32 optional)
{
#define PRESENCE(F,T) do { \
	if ((mandatory & TOK_##F) && !args->T) \
		return BADREQ("Missing %s", #F); \
	if (!((mandatory|optional) & TOK_##F) && args->T) \
		return BADREQ("Unexpected %s", #F); \
} while (0)
	PRESENCE (TAGK, tagk);
	PRESENCE (TAGV, tagv);
	PRESENCE (SIZE, size);
	PRESENCE (ACTION, action);
	PRESENCE (VERSION, version);
	PRESENCE (VERPOL, verpol);
	PRESENCE (STGPOL, stgpol);
	PRESENCE (STGCLS, stgcls);
	PRESENCE (KEY, key);
	return NULL;
#undef PRESENCE
}

static void
_req_path_clear_tokens (struct req_args_s *args)
{
	metautils_str_clean (&args->ns);
	metautils_str_clean (&args->ref);
	metautils_str_clean (&args->path);
	metautils_str_clean (&args->type);

	metautils_str_clean (&args->size);
	metautils_str_clean (&args->tagk);
	metautils_str_clean (&args->tagv);
	metautils_str_clean (&args->action);
	metautils_str_clean (&args->version);
	metautils_str_clean (&args->stgpol);
	metautils_str_clean (&args->verpol);
	metautils_str_clean (&args->stgcls);
	metautils_str_clean (&args->key);

	if (args->url)
		hc_url_clean (args->url);
	memset (args, 0, sizeof (struct req_args_s));
}

//------------------------------------------------------------------------------

static enum http_rc_e
req_args_call (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	struct req_uri_s *uri, const gchar * path, struct req_action_s *actions)
{
	gboolean _boolhdr (const gchar * n) {
		return metautils_cfg_get_bool (
			(gchar *) g_tree_lookup (rq->tree_headers, n), FALSE);
	}

	gboolean matched = FALSE;
	for (struct req_action_s * pa = actions; pa->prefix; ++pa) {

		if (!g_str_has_prefix (path, pa->prefix))
			continue;
		matched = TRUE;
		if (0 != strcmp (rq->cmd, pa->method))
			continue;

		struct req_args_s args;
		memset (&args, 0, sizeof (struct req_args_s));
		args.url = hc_url_empty ();
		args.uri = path + strlen (pa->prefix);
		args.req_uri = uri;
		args.rq = rq;
		args.rp = rp;
		if (_boolhdr ("x-disallow-empty-service-list"))
			args.flags |= FLAG_NOEMPTY;

		enum http_rc_e e;
		GError *err;
		if (!(err = _req_path_extract_tokens (&args))
				&& !(err = _req_query_extract_args (&args))
				&& !(err = _req_path_check_tokens (&args, pa->path))
				&& !(err = _req_query_check_tokens (&args, pa->query, pa->query_opt)))
			e = pa->hook (&args);
		else if (err->code == CODE_NAMESPACE_NOTMANAGED || err->code == 404)
			e = _reply_notfound_error (rp, err);
		else
			e = _reply_format_error (rp, err);

		_req_path_clear_tokens (&args);
		return e;
	}

	if (matched)
		return _reply_method_error (rp);
	return _reply_no_handler (rp);
}
