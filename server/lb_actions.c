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

static GString *
_lb_pack_and_free_srvinfo_list (const gchar * ns, const gchar * type,
	GSList * svc)
{
	GString *gstr = g_string_sized_new (512);
	gchar straddr[128];

	g_string_append_c (gstr, '{');
	_append_status (gstr, 200, "OK");
	g_string_append_printf (gstr, ",\"ns\":\"%s\",\"type\":\"%s\","
		"\"srv\":[", ns, type);

	for (GSList * l = svc; l; l = l->next) {
		if (l != svc)
			g_string_append_c (gstr, ',');
		struct service_info_s *si = l->data;
		grid_addrinfo_to_string (&(si->addr), straddr, sizeof (straddr));
		g_string_append_printf (gstr, "{\"addr\":\"%s\"}", straddr);
	}

	g_string_append (gstr, "]}");
	g_slist_free_full (svc, (GDestroyNotify) service_info_clean);
	return gstr;
}

static enum http_rc_e
action_lb_sl (const struct req_args_s *args)
{
	struct grid_lb_iterator_s *iter;

	if (!(iter = grid_lbpool_get_iterator (lbpool, args->type))) {
		return _reply_soft_error (args->rp, NEWERROR (460, "Type not managed"));
	}

	struct service_info_s *si = NULL;
	if (!grid_lb_iterator_next (iter, &si)) {
		service_info_clean (si);
		return _reply_soft_error (args->rp,
			NEWERROR (CODE_POLICY_NOT_SATISFIABLE, "Type not available"));
	}

	GString *gstr = _lb_pack_and_free_srvinfo_list (args->ns, args->type,
		g_slist_prepend (NULL, si));
	service_info_clean (si);
	return _reply_success_json (args->rp, gstr);
}

static enum http_rc_e
action_loadbalancing (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	const gchar * uri)
{
	if (!METACD_LB_ENABLED) {
		GError *err = NEWERROR (CODE_UNAVAILABLE,
			"Load-balancer disabled by configuration");
		return _reply_json (rp, CODE_UNAVAILABLE,
			"Service unavailable", _create_status_error (err));
	}

	static struct req_action_s lb_actions[] = {
		{"GET", "/sl", action_lb_sl, TOK_NS | TOK_TYPE},
		{NULL, NULL, NULL, 0},
	};
	return req_args_call (rq, rp, uri, lb_actions);
}
