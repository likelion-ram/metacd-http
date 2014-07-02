
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

