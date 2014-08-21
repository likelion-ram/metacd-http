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

struct dir_args_s
{
	struct hc_url_s *url;
	gchar *type;

	const gchar *uri;
	struct http_request_s *rq;
	struct http_reply_ctx_s *rp;
};

static void
dir_args_clear (struct dir_args_s *args)
{
	if (args->url)
		hc_url_clean(args->url);
	if (args->type)
		g_free(args->type);
	memset(args, 0, sizeof(struct dir_args_s));
}

static GError*
dir_args_extract(const gchar *uri, struct dir_args_s *args)
{
	GError* _on_ns(const gchar *v) {
		hc_url_set(args->url, HCURL_NS, *v ? v : NULL);
		return NULL;
	}
	GError* _on_ref(const gchar *v) {
		hc_url_set(args->url, HCURL_REFERENCE, *v ? v : NULL);
		return NULL;
	}
	GError* _on_type(const gchar *v) {
		metautils_str_replace(&args->type, v);
		return NULL;
	}

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"ref", _on_ref},
		{"type", _on_type},
		{NULL, NULL}
	};
	return _split_and_parse_url(uri, actions);
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

static GString *
_pack_and_freev_m1url_list(gchar **urlv)
{
	GString *result = _pack_m1url_list(urlv);
	g_strfreev(urlv);
	return result;
}

static enum http_rc_e
action_dir_list (const struct dir_args_s *args)
{
	gchar **urlv = NULL;
	GError *err = hc_resolve_reference_service (resolver,
			args->url, args->type, &urlv);
	if (NULL != err)
		return _reply_soft_error(args->rp, err);
	return _reply_success_json(args->rp, _pack_and_freev_m1url_list(urlv));
}

static enum http_rc_e
action_dir_link (const struct dir_args_s *args)
{
	gchar **m1v = NULL;
	GError *err = hc_resolve_reference_directory (resolver,
			args->url, &m1v);

	if (NULL != err) {
		g_prefix_error (&err, "No META1: ");
		return _reply_soft_error(args->rp, err);
	}

	for (gchar **pm1=m1v; *pm1 ;++pm1) {
		struct meta1_service_url_s *m1 = meta1_unpack_url(*pm1);
		struct addr_info_s m1a;
		gboolean rc = !m1 ? FALSE : grid_string_to_addrinfo(m1->host, NULL, &m1a);

		if (!rc) {
			GRID_INFO("Invalid META1 [%s] for [%s]",
					m1->host, hc_url_get(args->url, HCURL_WHOLE));
			meta1_service_url_clean(m1);
			continue;
		}
		meta1_service_url_clean(m1);

		gchar **urlv = meta1v2_remote_link_service(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				args->type, 30.0, 60.0, NULL);
		if (!err) {
			g_strfreev(m1v);
			return _reply_success_json(args->rp, _pack_and_freev_m1url_list(urlv));
		}
		else if (err->code == CODE_REDIRECT) {
			g_clear_error(&err);
		}
		else {
			g_strfreev(m1v);
			g_prefix_error(&err, "META1 error: ");
			return _reply_soft_error(args->rp, err);
		}
	}

	g_strfreev(m1v);
	return _reply_soft_error(args->rp, NEWERROR(CODE_UNAVAILABLE,
				"No meta1 answered"));
}

static enum http_rc_e
action_dir_unlink (const struct dir_args_s *args)
{
	gchar **m1v = NULL;
	GError *err = hc_resolve_reference_directory (resolver,
			args->url, &m1v);

	if (NULL != err) {
		g_prefix_error (&err, "No META1: ");
		return _reply_soft_error(args->rp, err);
	}

	for (gchar **pm1=m1v; *pm1 ;++pm1) {
		struct meta1_service_url_s *m1 = meta1_unpack_url(*pm1);
		struct addr_info_s m1a;
		gboolean rc = !m1 ? FALSE : grid_string_to_addrinfo(m1->host, NULL, &m1a);

		if (!rc) {
			GRID_INFO("Invalid META1 [%s] for [%s]",
					m1->host, hc_url_get(args->url, HCURL_WHOLE));
			meta1_service_url_clean(m1);
			continue;
		}
		meta1_service_url_clean(m1);

		meta1v2_remote_unlink_service(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				args->type, 30.0, 60.0, NULL);
		if (!err) {
			g_strfreev(m1v);
			return _reply_success_json(args->rp, NULL);
		}
		else if (err->code == CODE_REDIRECT) {
			g_clear_error(&err);
		}
		else {
			g_strfreev(m1v);
			g_prefix_error(&err, "META1 error: ");
			return _reply_soft_error(args->rp, err);
		}
	}

	g_strfreev(m1v);
	return _reply_soft_error(args->rp, NEWERROR(CODE_UNAVAILABLE,
				"No meta1 answered"));
}

static enum http_rc_e
action_dir_create (const struct dir_args_s *args)
{
	gchar **m1v = NULL;
	GError *err = hc_resolve_reference_directory (resolver,
			args->url, &m1v);

	if (NULL != err) {
		g_prefix_error (&err, "No META1: ");
		return _reply_soft_error(args->rp, err);
	}

	for (gchar **pm1=m1v; *pm1 ;++pm1) {
		struct meta1_service_url_s *m1 = meta1_unpack_url(*pm1);
		struct addr_info_s m1a;
		gboolean rc = !m1 ? FALSE : grid_string_to_addrinfo(m1->host, NULL, &m1a);

		if (!rc) {
			GRID_INFO("Invalid META1 [%s] for [%s]",
					m1->host, hc_url_get(args->url, HCURL_WHOLE));
			meta1_service_url_clean(m1);
			continue;
		}
		meta1_service_url_clean(m1);

		meta1v2_remote_create_reference(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				hc_url_get(args->url, HCURL_REFERENCE),
				30.0, 60.0, NULL);
		if (!err) {
			g_strfreev(m1v);
			return _reply_success_json(args->rp, NULL);
		}
		else if (err->code == CODE_REDIRECT) {
			g_clear_error(&err);
		}
		else {
			g_strfreev(m1v);
			g_prefix_error(&err, "META1 error: ");
			return _reply_soft_error(args->rp, err);
		}
	}

	g_strfreev(m1v);
	return _reply_soft_error(args->rp, NEWERROR(CODE_UNAVAILABLE,
				"No meta1 answered"));
}

static enum http_rc_e
action_dir_destroy (const struct dir_args_s *args)
{
	gchar **m1v = NULL;
	GError *err = hc_resolve_reference_directory (resolver,
			args->url, &m1v);

	if (NULL != err) {
		g_prefix_error (&err, "No META1: ");
		return _reply_soft_error(args->rp, err);
	}

	for (gchar **pm1=m1v; *pm1 ;++pm1) {
		struct meta1_service_url_s *m1 = meta1_unpack_url(*pm1);
		struct addr_info_s m1a;
		gboolean rc = !m1 ? FALSE : grid_string_to_addrinfo(m1->host, NULL, &m1a);

		if (!rc) {
			GRID_INFO("Invalid META1 [%s] for [%s]",
					m1->host, hc_url_get(args->url, HCURL_WHOLE));
			meta1_service_url_clean(m1);
			continue;
		}
		meta1_service_url_clean(m1);

		meta1v2_remote_delete_reference(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				30.0, 60.0, NULL);
		if (!err) {
			hc_decache_reference_service(resolver, args->url, args->type);
			g_strfreev(m1v);
			return _reply_success_json(args->rp, NULL);
		}
		else if (err->code == CODE_REDIRECT) {
			g_clear_error(&err);
		}
		else {
			g_strfreev(m1v);
			g_prefix_error(&err, "META1 error: ");
			return _reply_soft_error(args->rp, err);
		}
	}

	g_strfreev(m1v);
	return _reply_soft_error(args->rp, NEWERROR(CODE_UNAVAILABLE,
				"No meta1 answered"));
}


static enum http_rc_e
action_dir_flush_low (const struct dir_args_s *args)
{
	hc_resolver_flush_services(resolver);
	return _reply_success_json(args->rp, NULL);
}

static enum http_rc_e
action_dir_flush_high (const struct dir_args_s *args)
{
	hc_resolver_flush_csm0(resolver);
	return _reply_success_json(args->rp, NULL);
}

static enum http_rc_e
action_dir_set_max_high (const struct dir_args_s *args)
{
	hc_resolver_set_max_csm0(resolver, atoi(args->uri));
	return _reply_success_json(args->rp, NULL);
}

static enum http_rc_e
action_dir_set_max_low (const struct dir_args_s *args)
{
	hc_resolver_set_max_services (resolver, atoi(args->uri));
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_dir_set_ttl_high (const struct dir_args_s *args)
{
	hc_resolver_set_ttl_csm0 (resolver, atoi(args->uri));
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_dir_set_ttl_low (const struct dir_args_s *args)
{
	hc_resolver_set_ttl_services (resolver, atoi(args->uri));
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_dir_status (const struct dir_args_s *args)
{
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
	return _reply_success_json(args->rp, gstr);
}

static struct directory_action_s
{
	const gchar *method;
	const gchar *prefix;
	enum http_rc_e (*hook) (const struct dir_args_s *args);
	unsigned int expectations;
} dir_actions[] = {
	{ "GET",  "list/",    action_dir_list,         TOK_NS|TOK_REF|TOK_TYPE},
	{ "POST", "link/",    action_dir_link,         TOK_NS|TOK_REF},
	{ "POST", "unlink/",  action_dir_unlink,       TOK_NS|TOK_REF},
	{ "POST", "create/",  action_dir_create,       TOK_NS|TOK_REF},
	{ "POST", "destroy/", action_dir_destroy,      TOK_NS|TOK_REF},
	{ "GET",  "status/",  action_dir_status,       0 },
	{ "POST", "flush/high/",   action_dir_flush_high,   0 },
	{ "POST", "flush/low/",    action_dir_flush_low,    0 },
	{ "POST", "set/ttl/high/", action_dir_set_ttl_high, 0 },
	{ "POST", "set/ttl/low/",  action_dir_set_ttl_low,  0 },
	{ "POST", "set/max/high/", action_dir_set_max_high, 0 },
	{ "POST", "set/max/low/",  action_dir_set_max_low,  0 },

	{ NULL, NULL, NULL, 0 }
};

static enum http_rc_e
action_directory_real (struct directory_action_s *pa, struct dir_args_s *args)
{
	if ((pa->expectations & TOK_NS) && !hc_url_has(args->url, HCURL_NS))
		return _reply_format_error(args->rp, BADREQ("Missing NS"));
	if ((pa->expectations & TOK_REF) && !hc_url_has(args->url, HCURL_REFERENCE))
		return _reply_format_error(args->rp, BADREQ("Missing REF"));
	if ((pa->expectations & TOK_TYPE) && !args->type)
		return _reply_format_error(args->rp, BADREQ("Missing TYPE"));
	if (!validate_namespace(hc_url_get(args->url, HCURL_NSPHYS)))
		return _reply_soft_error(args->rp, NEWERROR(
			CODE_NAMESPACE_NOTMANAGED, "Invalid NS"));
	return pa->hook(args);
}

static enum http_rc_e
action_directory(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	gboolean matched = FALSE;

	for (struct directory_action_s *pa = dir_actions; pa->prefix ;++pa) {
		if (!g_str_has_prefix(uri, pa->prefix))
			continue;
		matched = TRUE;
		if (0 != strcmp(rq->cmd, pa->method))
			continue;

		struct dir_args_s args;
		args.url = hc_url_empty();
		args.type = NULL;
		args.uri = uri + strlen(pa->prefix);
		args.rq = rq;
		args.rp = rp;

		GError *err;
		enum http_rc_e e;
		if (NULL != (err = dir_args_extract(args.uri, &args)))
			e = _reply_format_error(rp, err);
		else
			e = action_directory_real(pa, &args);
		dir_args_clear(&args);
		return e;
	}

	if (matched)
		return _reply_method_error(rp);
	return _reply_no_handler(rp);
}

