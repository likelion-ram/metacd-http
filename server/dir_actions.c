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

