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
_extract_lb_args(const gchar *args, gchar **rns, gchar **rtype)
{
	gchar *ns = NULL, *type = NULL;

	void _clean(gchar **ps) { if (*ps) g_free(*ps); *ps = NULL; }
	void _repl(gchar **ps, const gchar *v) { _clean(ps); *ps = g_strdup(v); }
	GError* _on_ns(const gchar *v) { _repl(&ns, v); return NULL; }
	GError* _on_type(const gchar *v) { _repl(&type, v); return NULL; }

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"type", _on_type},
		{NULL, NULL}
	};
	GError *err = _split_and_parse_url(args, actions);
	if (!err && (!ns || !*ns))
		err = NEWERROR(400, "Missing namespace");
	if (!err && (!type || !*type))
		err = NEWERROR(400, "Missing service type");
	if (err) {
		_clean(&ns);
		_clean(&type);
		return err;
	}
	*rtype = type;
	*rns = ns;
	return NULL;
}

static GString *
_lb_pack_and_free_srvinfo_list(const gchar *ns, const gchar *type, GSList *svc)
{
	GString *gstr = g_string_sized_new(512);
	gchar straddr[128];

	g_string_append_c(gstr, '{');
	_append_status(gstr, 200, "OK");
	g_string_append_printf(gstr, ",\"ns\":\"%s\",\"type\":\"%s\",\"srv\":[", ns, type);

	for (GSList *l = svc; l ;l=l->next) {
		if (l != svc)
			g_string_append_c(gstr, ',');
		struct service_info_s *si = l->data;
		grid_addrinfo_to_string(&(si->addr), straddr, sizeof(straddr));
		g_string_append_printf(gstr, "{\"addr\":\"%s\"}", straddr);
	}

	g_string_append(gstr, "]}");
	g_slist_free_full(svc, (GDestroyNotify)service_info_clean);
	return gstr;
}

static enum http_rc_e
action_lb_sl(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *args)
{
	gchar *ns = NULL, *type = NULL;
	struct service_info_s *si = NULL;

	if (!METACD_LB_ENABLED) {
		GError *err = NEWERROR(CODE_UNAVAILABLE,
				"Load-balancer disabled by configuration");
		return _reply_json(rp, CODE_UNAVAILABLE,
				"Service unavailable", _create_status_error(err));
	}

	GError *err;
	if (g_ascii_strcasecmp(rq->cmd, "GET"))
		return _reply_method_error(rp);
	if (NULL != (err = _extract_lb_args(args, &ns, &type)))
		return _reply_format_error(rp, err);

	struct grid_lb_iterator_s *iter = grid_lbpool_get_iterator(lbpool, type);
	if (!iter) {
		g_free(ns);
		g_free(type);
		return _reply_soft_error(rp, NEWERROR(460, "Type not managed"));
	}

	if (!grid_lb_iterator_next(iter, &si)) {
		g_free(ns);
		g_free(type);
		service_info_clean(si);
		return _reply_soft_error(rp, NEWERROR(CODE_POLICY_NOT_SATISFIABLE,
					"Type not available"));
	}

	GString *gstr = _lb_pack_and_free_srvinfo_list(ns, type,
			g_slist_prepend(NULL, si));

	g_free(ns);
	g_free(type);
	return _reply_success_json(rp, gstr);
}

