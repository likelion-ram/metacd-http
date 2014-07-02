
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

