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

// XXX Pay attention, the format here is different from the format of
// the output of the legacy /lb/sl handler.
static GString *
_cs_pack_and_free_srvinfo_list (GSList * svc)
{
	GString *gstr = g_string_sized_new (64 + 32 * g_slist_length (svc));

	g_string_append_c (gstr, '{');
	_append_status (gstr, 200, "OK");
	g_string_append (gstr, ",\"srv\":[");

	for (GSList * l = svc; l; l = l->next) {
		if (l != svc)
			g_string_append_c (gstr, ',');
		service_info_encode_json (gstr, l->data);
	}

	g_string_append (gstr, "]}");
	g_slist_free_full (svc, (GDestroyNotify) service_info_clean);
	return gstr;
}

static enum http_rc_e
_registration (const struct req_args_s *args, gboolean direct, gboolean unlock)
{
	GError *err;
	struct service_info_s *si = NULL;

	g_byte_array_append (args->rq->body, (guint8 *) "", 1);
	const gchar *encoded = (gchar *) args->rq->body->data;
	err = service_info_load_json (encoded, &si);

	if (err) {
		if (err->code == 400)
			return _reply_format_error (args->rp, err);
		else
			return _reply_system_error (args->rp, err);
	}

	if (!validate_namespace (si->ns_name)) {
		service_info_clean (si);
		return _reply_soft_error (args->rp, NEWERROR (CODE_NAMESPACE_NOTMANAGED,
				"Unexpected NS"));
	}

	if (!direct) {				// Simple registration via the gridagent
		si->score.value = 0;
		si->score.timestamp = 0;
		register_namespace_service (si, &err);
	} else {					// lock or unlock -> direct to the conscience
		gchar *cs = gridcluster_get_config (args->ns, "conscience", ~0);
		if (!cs) {
			err = NEWERROR (CODE_NAMESPACE_NOTMANAGED,
				"No conscience for namespace NS");
		} else {
			struct addr_info_s csaddr;
			if (!grid_string_to_addrinfo (cs, NULL, &csaddr)) {
				err = NEWERROR (CODE_NAMESPACE_NOTMANAGED,
					"Invalid conscience address for NS");
			} else {
				if (unlock)
					si->score.value = -1;
				si->score.timestamp = 0;
				GSList *l = g_slist_prepend (NULL, si);
				gcluster_push_services (&csaddr, 4000, l, TRUE, &err);
				g_slist_free (l);
				metautils_str_clean (&cs);
			}
		}
	}

	if (err) {
		service_info_clean (si);
		return _reply_soft_error (args->rp, err);
	}
	GString *gstr = g_string_sized_new (256);
	service_info_encode_json (gstr, si);
	service_info_clean (si);
	return _reply_success_json (args->rp, gstr);
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_cs_info (const struct req_args_s *args)
{
	struct namespace_info_s ni;
	memset (&ni, 0, sizeof (ni));
	g_static_mutex_lock (&nsinfo_mutex);
	namespace_info_copy (&nsinfo, &ni, NULL);
	g_static_mutex_unlock (&nsinfo_mutex);

	GString *gstr = g_string_sized_new (1024);
	namespace_info_encode_json (gstr, &ni);
	namespace_info_clear (&ni);
	return _reply_success_json (args->rp, gstr);
}

static enum http_rc_e
action_cs_put (const struct req_args_s *args)
{
	return _registration (args, FALSE, FALSE);
}

static enum http_rc_e
action_cs_get (const struct req_args_s *args)
{
	GError *err = NULL;
	GSList *sl = list_namespace_services2 (args->ns, args->type, &err);
	if (NULL != err) {
		g_slist_free_full (sl, (GDestroyNotify) service_info_clean);
		g_prefix_error (&err, "Agent error: ");
		return _reply_soft_error (args->rp, err);
	}
	return _reply_success_json (args->rp, _cs_pack_and_free_srvinfo_list (sl));
}

static enum http_rc_e
action_cs_del (const struct req_args_s *args)
{
	GError *err = NULL;
	gboolean rc = clear_namespace_services (args->ns, args->type, &err);
	if (!rc) {
		g_prefix_error (&err, "Agent error: ");
		return _reply_soft_error (args->rp, err);
	}
	return _reply_success_json (args->rp, _create_status (200, "OK"));
}

static enum http_rc_e
action_cs_post (const struct req_args_s *args)
{
	if (0 == strcmp (args->action, "lock"))
		return _registration (args, TRUE, FALSE);
	if (0 == strcmp (args->action, "unlock"))
		return _registration (args, TRUE, TRUE);
	return _reply_format_error (args->rp, BADREQ ("Invalid action"));
}

static enum http_rc_e
action_conscience (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	const gchar * uri)
{
	static struct req_action_s cs_actions[] = {
		{"GET", "info/", action_cs_info, TOK_NS},

		{"PUT", "srv/", action_cs_put, TOK_NS | TOK_TYPE},
		{"GET", "srv/", action_cs_get, TOK_NS | TOK_TYPE},
		{"DELETE", "srv/", action_cs_del, TOK_NS | TOK_TYPE},
		{"POST", "srv/", action_cs_post, TOK_NS | TOK_TYPE | TOK_ACTION},
		/// lock, unlock
		{NULL, NULL, NULL, 0}
	};
	return req_args_call (rq, rp, uri, cs_actions);
}
