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

