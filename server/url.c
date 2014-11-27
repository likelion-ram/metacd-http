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
	TOK_NS = 0x0001,
	TOK_REF = 0x0002,
	TOK_PATH = 0x0004,
	TOK_TYPE = 0x0008,
	TOK_ACTION = 0x0010,
	TOK_STGPOL = 0x0020,
	TOK_VERPOL = 0x0040,
	TOK_VERSION = 0x0080,
};

enum {
	FLAG_NOEMPTY = 0x0001,
};

struct req_args_s {
	gchar *ns;
	gchar *ref;
	gchar *path;

	gchar *action;

	gchar *size;
	gchar *type;
	gchar *version;
	gchar *stgpol;
	gchar *verpol;

	guint32 flags;

	struct hc_url_s *url;

	// The portion of the URI after the prefix
	const gchar *uri;

	struct http_request_s *rq;
	struct http_reply_ctx_s *rp;
};

struct req_action_s {
	const gchar *method;
	const gchar *prefix;
	enum http_rc_e (*hook) (const struct req_args_s * args);
	guint32 mandatory;
};

struct url_action_s {
	const gchar *token;
	gchar **to;
};

static GError *
_parse_args_pair (gchar ** t, struct url_action_s *pa)
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
_parse_url (gchar ** t, struct url_action_s *actions)
{
	GError *e = NULL;
	for (; !e && *t; t += 2) {
		if (!*(t + 1))
			e = NEWERROR (400, "Invalid URI");
		else
			e = _parse_args_pair (t, actions);
	}
	return e;
}

static GError *
_split_and_parse_url (const gchar * uri, struct url_action_s *actions)
{
	gchar **tokens = g_strsplit (uri, "/", 0);

	if (!tokens)
		return NEWERROR (500, "Internal error");
	GError *e = _parse_url (tokens, actions);
	g_strfreev (tokens);
	return e;
}

static void
req_args_clear (struct req_args_s *args)
{
	metautils_str_clean (&args->ns);
	metautils_str_clean (&args->ref);
	metautils_str_clean (&args->path);
	metautils_str_clean (&args->action);
	metautils_str_clean (&args->size);
	metautils_str_clean (&args->type);
	metautils_str_clean (&args->stgpol);
	metautils_str_clean (&args->verpol);

	if (args->url)
		hc_url_clean (args->url);
	memset (args, 0, sizeof (struct req_args_s));
}

static GError *
req_args_extract_tokens (const gchar * uri, struct req_args_s *args)
{
	struct url_action_s actions[] = {
		{"ns", &args->ns},
		{"ref", &args->ref},
		{"path", &args->path},
		{"type", &args->type},
		{"action", &args->action},
		{"version", &args->version},
		{"size", &args->size},
		{"stgpol", &args->stgpol},
		{"verpol", &args->verpol},
		{NULL, NULL}
	};
	return _split_and_parse_url (uri, actions);
}

static gboolean
_boolhdr (struct http_request_s *rq, const gchar * n)
{
	return metautils_cfg_get_bool (
		(gchar *) g_tree_lookup (rq->tree_headers, n), FALSE);
}

static GError *
req_args_extract (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	const gchar * uri, const gchar * prefix, struct req_args_s *args)
{
	memset (args, 0, sizeof (struct req_args_s));
	args->url = hc_url_empty ();
	args->uri = uri + strlen (prefix);
	args->rq = rq;
	args->rp = rp;
	args->flags |=
		_boolhdr (rq, "x-disallow-empty-service-list") ? FLAG_NOEMPTY : 0;

	return req_args_extract_tokens (args->uri, args);
}

#define PRESENCE(F,T) do { \
	if (flags & TOK_##F && !args->T) \
		return BADREQ("Missing %s", #F); \
} while (0)

static GError *
req_args_check (struct req_args_s *args, guint32 flags)
{
	if (args->ns)
		hc_url_set (args->url, HCURL_NS, args->ns);
	if (args->ref)
		hc_url_set (args->url, HCURL_REFERENCE, args->ref);
	if (args->path)
		hc_url_set (args->url, HCURL_PATH, args->path);

	PRESENCE (NS, ns);
	PRESENCE (REF, ref);
	PRESENCE (TYPE, type);
	PRESENCE (PATH, path);
	PRESENCE (STGPOL, stgpol);
	PRESENCE (VERPOL, verpol);

	// All the fields are present ... now check their value
	if (flags & TOK_NS || args->ns) {
		if (!validate_namespace (hc_url_get(args->url, HCURL_NSPHYS)))
			return NEWERROR (CODE_NAMESPACE_NOTMANAGED, "Invalid NS");
	}
	if (flags & TOK_TYPE) {
		if (!validate_srvtype (args->type))
			return NEWERROR (CODE_NAMESPACE_NOTMANAGED, "Invalid TYPE");
	}
	return NULL;
}

static enum http_rc_e
req_args_call (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	const gchar * uri, struct req_action_s *actions)
{
	gboolean matched = FALSE;
	for (struct req_action_s * pa = actions; pa->prefix; ++pa) {
		if (!g_str_has_prefix (uri, pa->prefix))
			continue;
		matched = TRUE;
		if (0 != strcmp (rq->cmd, pa->method))
			continue;

		GRID_TRACE2("%s|%s matched %s|%s -> %p",
				rq->cmd, rq->req_uri, pa->method, pa->prefix, pa->hook);
		struct req_args_s args;
		enum http_rc_e e;
		GError *err;
		if (NULL != (err = req_args_extract (rq, rp, uri, pa->prefix, &args)))
			e = _reply_format_error (rp, err);

		else {
			GRID_TRACE2("REQ ns:%s ref:%s path:%s",
					args.ns, args.ref, args.path);
			if (NULL != (err = req_args_check (&args, pa->mandatory))) {
				if (err->code == CODE_NAMESPACE_NOTMANAGED || err->code == 404)
					e = _reply_notfound_error (rp, err);
				else
					e = _reply_format_error (rp, err);
			} else {
				e = pa->hook (&args);
			}
		}
		req_args_clear (&args);
		return e;
	}

	if (matched)
		return _reply_method_error (rp);
	return _reply_no_handler (rp);
}
