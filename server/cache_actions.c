
struct cache_args_s {
	gint64 count;

	const gchar *uri;
	struct http_request_s *rq;
	struct http_reply_ctx_s *rp;
};

static enum http_rc_e
action_cache_flush_low (const struct cache_args_s *args)
{
	hc_resolver_flush_services (resolver);
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_cache_flush_high (const struct cache_args_s *args)
{
	hc_resolver_flush_csm0 (resolver);
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_cache_set_max_high (const struct cache_args_s *args)
{
	hc_resolver_set_max_csm0 (resolver, args->count);
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_cache_set_max_low (const struct cache_args_s *args)
{
	hc_resolver_set_max_services (resolver, args->count);
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_cache_set_ttl_high (const struct cache_args_s *args)
{
	hc_resolver_set_ttl_csm0 (resolver, args->count);
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_cache_set_ttl_low (const struct cache_args_s *args)
{
	hc_resolver_set_ttl_services (resolver, args->count);
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_cache_status (const struct cache_args_s *args)
{
	struct hc_resolver_stats_s s;
	memset (&s, 0, sizeof (s));
	hc_resolver_info (resolver, &s);

	GString *gstr = g_string_new ("{");
	g_string_append_printf (gstr, " \"clock\":%lu,", s.clock);
	g_string_append_printf (gstr, " \"csm0\":{"
		"\"count\":%" G_GINT64_FORMAT ",\"max\":%u,\"ttl\":%lu},",
		s.csm0.count, s.csm0.max, s.csm0.ttl);
	g_string_append_printf (gstr, " \"meta1\":{"
		"\"count\":%" G_GINT64_FORMAT ",\"max\":%u,\"ttl\":%lu}",
		s.services.count, s.services.max, s.services.ttl);
	g_string_append_c (gstr, '}');
	return _reply_success_json (args->rp, gstr);
}

static enum http_rc_e
action_cache (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	const gchar * uri)
{
	struct cache_action_s {
		const gchar *method;
		const gchar *prefix;
		enum http_rc_e (*hook) (const struct cache_args_s * args);
	};
	static struct cache_action_s dir_actions[] = {
		{"GET", "status/", action_cache_status},
		{"POST", "flush/high/", action_cache_flush_high},
		{"POST", "flush/low/", action_cache_flush_low},
		{"POST", "set/ttl/high/", action_cache_set_ttl_high},
		{"POST", "set/ttl/low/", action_cache_set_ttl_low},
		{"POST", "set/max/high/", action_cache_set_max_high},
		{"POST", "set/max/low/", action_cache_set_max_low},
		{NULL, NULL, NULL}
	};
	gboolean matched = FALSE;

	for (struct cache_action_s * pa = dir_actions; pa->prefix; ++pa) {
		if (!g_str_has_prefix (uri, pa->prefix))
			continue;
		matched = TRUE;
		if (0 != strcmp (rq->cmd, pa->method))
			continue;

		struct cache_args_s args;
		memset (&args, 0, sizeof (args));
		args.uri = uri + strlen (pa->prefix);
		args.count = atoi (args.uri);
		args.rq = rq;
		args.rp = rp;

		return pa->hook (&args);
	}

	if (matched)
		return _reply_method_error (rp);
	return _reply_no_handler (rp);
}
