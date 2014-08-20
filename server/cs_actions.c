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

struct cs_args_s
{
	gchar *ns;
	gchar *url;
	gchar *type;
	gchar *score;

	const gchar *uri;
	struct http_request_s *rq;
	struct http_reply_ctx_s *rp;
};

static void
cs_args_clear(struct cs_args_s *args)
{
	if (args->ns)
		g_free(args->ns);
	if (args->type)
		g_free(args->type);
	if (args->url)
		g_free(args->score);
}

static GError *
cs_args_extract(const gchar *uri, struct cs_args_s *args)
{
	GError* _on_ns(const gchar *v) {
		gchar pns[LIMIT_LENGTH_NSNAME+1];
		metautils_strlcpy_physical_ns(pns, v, sizeof(pns));
		metautils_str_replace(&args->ns, pns);
		return NULL;
	}
	GError* _on_type(const gchar *v) {
		metautils_str_replace(&args->type, *v ? v : NULL);
		return NULL;
	}
	GError* _on_url(const gchar *v) {
		metautils_str_replace(&args->url, v);
		return NULL;
	}
	GError* _on_score(const gchar *v) {
		metautils_str_replace(&args->score, v);
		return NULL;
	}
	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"url", _on_url},
		{"type", _on_type},
		{"score", _on_score},
		{NULL, NULL}
	};

	return _split_and_parse_url(uri, actions);
}

static GString*
_cs_pack_and_free_srvinfo_list(GSList *svc)
{
	GString *gstr = g_string_sized_new(64 + 32 * g_slist_length(svc));

	g_string_append_c(gstr, '{');
	_append_status(gstr, 200, "OK");
	g_string_append(gstr, ",\"srv\":[");

	for (GSList *l = svc; l ;l=l->next) {
		if (l != svc)
			g_string_append_c(gstr, ',');
		service_info_encode_json(gstr, l->data);
	}

	g_string_append(gstr, "]}");
	g_slist_free_full(svc, (GDestroyNotify)service_info_clean);
	return gstr;
}

static enum http_rc_e
action_cs_list (const struct cs_args_s *args)
{
	GError *err = NULL;
	GSList *sl = list_namespace_services2(args->ns, args->type, &err);
	if (NULL != err) {
		g_slist_free_full(sl, (GDestroyNotify)service_info_clean);
		g_prefix_error(&err, "Agent error: ");
		return _reply_soft_error(args->rp, err);
	}
	return _reply_success_json(args->rp,_cs_pack_and_free_srvinfo_list(sl));
}

static enum http_rc_e
action_cs_reg (const struct cs_args_s *args)
{
	(void) args;
	return HTTPRC_ABORT;
}

static enum http_rc_e
action_cs_lock(const struct cs_args_s *args)
{
	(void) args;
	return HTTPRC_ABORT;
}

static enum http_rc_e
action_cs_unlock(const struct cs_args_s *args)
{
	(void) args;
	return HTTPRC_ABORT;
}

static enum http_rc_e
action_cs_clear(const struct cs_args_s *args)
{
	GError *err = NULL;
	gboolean rc = clear_namespace_services(args->ns, args->type, &err);
	if (!rc) {
		g_prefix_error(&err, "Agent error: ");
		return _reply_soft_error(args->rp, err);
	}
	return _reply_success_json(args->rp, _create_status(200, "OK"));
}

static struct cs_action_s
{
	const gchar *method;
	const gchar *prefix;
	enum http_rc_e (*hook) (const struct cs_args_s *args);
	unsigned int expectations;
} cs_actions[] = {
	{ "POST", "clear/",  action_cs_clear,  TOK_NS|TOK_TYPE },
	{ "POST", "unlock/", action_cs_unlock, TOK_NS|TOK_TYPE|TOK_URL },
	{ "POST", "lock/",   action_cs_lock,   TOK_NS|TOK_TYPE|TOK_URL|TOK_SCORE },
	{ "POST", "reg/",    action_cs_reg,    TOK_NS|TOK_TYPE|TOK_URL },
	{ "GET",  "list/",   action_cs_list,   TOK_NS|TOK_TYPE },
	{ NULL, NULL, NULL, 0 }
};

static enum http_rc_e
action_conscience_real(struct cs_action_s *pa, struct cs_args_s *args)
{
	if (!args->ns && pa->expectations & TOK_NS)
		return _reply_format_error(args->rp, BADREQ("Missing NS"));
	if (!args->url && pa->expectations & TOK_URL)
		return _reply_format_error(args->rp, BADREQ("Missing URL"));
	if (!args->type && pa->expectations & TOK_TYPE)
		return _reply_format_error(args->rp, BADREQ("Missing TYPE"));
	if (!args->score && pa->expectations & TOK_SCORE)
		return _reply_format_error(args->rp, BADREQ("Missing SCORE"));
	if (!validate_namespace(args->ns))
		return _reply_soft_error(args->rp, NEWERROR(
			CODE_NAMESPACE_NOTMANAGED, "Invalid NS"));
	return pa->hook(args);
}

static enum http_rc_e
action_conscience(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	gboolean matched = FALSE;
	for (struct cs_action_s *pa = cs_actions; pa->prefix ;++pa) {
		if (!g_str_has_prefix(uri, pa->prefix))
			continue;
		matched = TRUE;
		if (0 != strcmp(rq->cmd, pa->method))
			continue;

		struct cs_args_s args;
		memset(&args, 0, sizeof(args));
		args.uri = uri + strlen(pa->prefix);
		args.rq = rq;
		args.rp = rp;

		GError *err;
		enum http_rc_e e;
		if (NULL != (err = cs_args_extract(args.uri, &args)))
			e = _reply_format_error(rp, err);
		else
			e = action_conscience_real(pa, &args);
		cs_args_clear(&args);
		return e;
	}

	if (matched)
		return _reply_method_error(rp);
	return _reply_no_handler(rp);
}

