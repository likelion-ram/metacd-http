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

// Legacy ----------------------------------------------------------------------

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
	GRID_TRACE2("%s %s %s", __FUNCTION__, args->ns, args->type);

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
	return _reply_success_json (args->rp, gstr);
}

// New handlers ----------------------------------------------------------------

static GString *
_lb_pack_and_free_srvinfo_tab (struct service_info_s **siv)
{
	GString *gstr = g_string_sized_new (512);
	g_string_append_c(gstr, '[');
	for (struct service_info_s **pp = siv; *pp ;pp++) {
		if (siv != pp)
			g_string_append_c(gstr, ',');
		service_info_encode_json(gstr, *pp);
	}
	g_string_append_c(gstr, ']');
	return gstr;
}

static gboolean
_filter_tag (struct service_info_s *si, gpointer u)
{
	const struct req_args_s *args = u;

	if (!args->tagk)
		return TRUE;
	if (!si || !si->tags)
		return FALSE;

	struct service_tag_s *tag = service_info_get_tag(si->tags, args->tagk);
	if (!tag)
		return FALSE;
	if (!args->tagv) // No value specified, the presence is enough
		return TRUE;

	gchar tmp[128];
	service_tag_to_string(tag, tmp, sizeof(tmp));
	return 0 == strcmp(tmp, args->tagv);
}

static enum http_rc_e
_lb (const struct req_args_s *args, struct grid_lb_iterator_s *iter)
{
	if (!iter)
		return _reply_soft_error (args->rp, NEWERROR (460, "Type not managed"));

	// Terribly configurable and poorly implemented LB
	struct storage_class_s *stgcls;
	NSINFO_DO(stgcls = storage_class_init(&nsinfo, args->stgcls));
	struct lb_next_opt_ext_s opt;
	opt.req.distance = 1;
	opt.req.max = args->size ? atoi(args->size) : 1;
	opt.req.duplicates = FALSE;
	opt.req.stgclass = !args->stgcls ? NULL : stgcls;
	opt.req.strict_stgclass = FALSE;
	opt.req.shuffle = FALSE;
	opt.filter.data = NULL;
	opt.filter.hook = NULL;
	opt.srv_inplace = NULL;
	opt.srv_forbidden = NULL;

	struct service_info_s **siv = NULL;
	gboolean rc = grid_lb_iterator_next_set2(iter, &siv, &opt);
	if (stgcls)
		storage_class_clean(stgcls);

	if (!rc) {
		service_info_cleanv(siv, FALSE);
		return _reply_soft_error (args->rp, NEWERROR(
					CODE_POLICY_NOT_SATISFIABLE, "Too constrained"));
	} else {
		GString *gstr = _lb_pack_and_free_srvinfo_tab (siv);
		service_info_cleanv (siv, FALSE);
		return _reply_success_json (args->rp, gstr);
	}
}

static inline void
_set_lb_hooks(struct grid_lb_iterator_s *it, const struct req_args_s *args)
{
	if (args->tagk)
		grid_lb_iterator_set_pre_filter(it, _filter_tag, (gpointer) args);
}

enum LB_TYPE
{
	DEF = 0,
	RR,
	WRR,
	RAND,
	WRAND
};

static enum http_rc_e
_action_lb(const struct req_args_s *args, enum LB_TYPE type)
{
	struct grid_lb_s *lb = grid_lbpool_ensure_lb(lbpool, args->type);
	struct grid_lb_iterator_s *iter = NULL;

	switch (type) {
	case RR:
		iter = grid_lb_iterator_round_robin(lb);
		break;
	case WRR:
		iter = grid_lb_iterator_weighted_round_robin(lb);
		break;
	case RAND:
		iter = grid_lb_iterator_random(lb);
		break;
	case WRAND:
		iter = grid_lb_iterator_weighted_random(lb);
		break;
	case DEF:
	default:
		iter = grid_lbpool_ensure_iterator(lbpool, args->type);
		break;
	}

	_set_lb_hooks(iter, args);
	enum http_rc_e rc = _lb (args, iter);
	grid_lb_iterator_clean (iter);
	return rc;
}

static enum http_rc_e
action_lb_def (const struct req_args_s *args)
{
	return _action_lb(args, DEF);
}

static enum http_rc_e
action_lb_rr (const struct req_args_s *args)
{
	return _action_lb(args, RR);
}

static enum http_rc_e
action_lb_wrr (const struct req_args_s *args)
{
	return _action_lb(args, WRR);
}

static enum http_rc_e
action_lb_rand (const struct req_args_s *args)
{
	return _action_lb(args, RAND);
}

static enum http_rc_e
action_lb_wrand (const struct req_args_s *args)
{
	return _action_lb(args, WRAND);
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_lb_hash (const struct req_args_s *args)
{
	return _reply_system_error(args->rp, NEWERROR(500, "NYI"));
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_loadbalancing (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	struct req_uri_s *uri, const gchar *path)
{
	if (!METACD_LB_ENABLED) {
		GError *err = NEWERROR (CODE_UNAVAILABLE,
			"Load-balancer disabled by configuration");
		return _reply_json (rp, CODE_UNAVAILABLE,
			"Service unavailable", _create_status_error (err));
	}
	static struct req_action_s lb_actions[] = {
		// Legacy handler
		{"GET", "sl/", action_lb_sl, TOK_NS | TOK_TYPE, 0, 0},

		// New handlers
		{"GET", "h/",     action_lb_hash,  TOK_NS|TOK_TYPE, TOK_KEY, TOK_TAGK|TOK_TAGV|TOK_SIZE},
		{"GET", "def/",   action_lb_def,   TOK_NS|TOK_TYPE, 0, TOK_TAGK|TOK_TAGV|TOK_SIZE},
		{"GET", "rr/",    action_lb_rr,    TOK_NS|TOK_TYPE, 0, TOK_TAGK|TOK_TAGV|TOK_SIZE},
		{"GET", "wrr/",   action_lb_wrr,   TOK_NS|TOK_TYPE, 0, TOK_TAGK|TOK_TAGV|TOK_SIZE},
		{"GET", "rand/",  action_lb_rand,  TOK_NS|TOK_TYPE, 0, TOK_TAGK|TOK_TAGV|TOK_SIZE},
		{"GET", "wrand/", action_lb_wrand, TOK_NS|TOK_TYPE, 0, TOK_TAGK|TOK_TAGV|TOK_SIZE},

		{NULL, NULL, NULL, 0, 0, 0},
	};
	GRID_TRACE2("%s", __FUNCTION__);
	return req_args_call (rq, rp, uri, path, lb_actions);
}
