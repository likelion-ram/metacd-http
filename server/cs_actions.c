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

enum reg_op_e {
	REGOP_PUSH,
	REGOP_LOCK,
	REGOP_UNLOCK,
};

static enum http_rc_e
_registration (const struct req_args_s *args, enum reg_op_e op)
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

	si->score.timestamp = network_server_bogonow(args->rq->client->server);
	if (op == REGOP_PUSH)
		si->score.value = 0;
	else if (op == REGOP_UNLOCK)
		si->score.value = -1;

	gchar *key = service_info_key(si);
	PUSH_DO(lru_tree_insert(push_queue, key, si));

	if (err)
		return _reply_soft_error (args->rp, err);
	GString *gstr = g_string_sized_new (256);
	service_info_encode_json (gstr, si);
	return _reply_success_json (args->rp, gstr);
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_cs_nscheck (const struct req_args_s *args)
{
	// The namespace has already been checked
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_cs_info (const struct req_args_s *args)
{
	struct namespace_info_s ni;
	memset (&ni, 0, sizeof (ni));
	NSINFO_DO(namespace_info_copy (&nsinfo, &ni, NULL));

	GString *gstr = g_string_sized_new (1024);
	namespace_info_encode_json (gstr, &ni);
	namespace_info_clear (&ni);
	return _reply_success_json (args->rp, gstr);
}

static enum http_rc_e
action_cs_put (const struct req_args_s *args)
{
	return _registration (args, REGOP_PUSH);
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
action_cs_srvcheck (const struct req_args_s *args)
{
	// The namespace and servcie have already been checked
	return _reply_success_json (args->rp, NULL);
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
		return _registration (args, REGOP_LOCK);
	if (0 == strcmp (args->action, "unlock"))
		return _registration (args, REGOP_UNLOCK);
	return _reply_format_error (args->rp, BADREQ ("Invalid action"));
}

static enum http_rc_e
action_cs_srvtypes (const struct req_args_s *args)
{
	GString *out = g_string_sized_new(128);
	g_string_append_c(out, '[');
	NSINFO_DO(if (srvtypes && *srvtypes) {
		g_string_append_c(out, '"');
		g_string_append(out, *srvtypes);
		g_string_append_c(out, '"');
		for (gchar **ps = srvtypes+1; *ps ;ps++) {
			g_string_append_c(out, ',');
			g_string_append_c(out, '"');
			g_string_append(out, *ps);
			g_string_append_c(out, '"');
		}
	});
	g_string_append_c(out, ']');
	return _reply_success_json (args->rp, out);
}

static enum http_rc_e
action_conscience (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	struct req_uri_s *uri, const gchar *path)
{
	static struct req_action_s cs_actions[] = {
		{"GET", "info/", action_cs_info, TOK_NS, 0, 0},
		{"HEAD", "info/", action_cs_nscheck, TOK_NS, 0, 0},

		{"GET", "types/", action_cs_srvtypes, TOK_NS, 0, 0},

		{"PUT", "srv/", action_cs_put, TOK_NS | TOK_TYPE, 0, 0},
		{"GET", "srv/", action_cs_get, TOK_NS | TOK_TYPE, 0, 0},
		{"HEAD", "srv/", action_cs_srvcheck, TOK_NS | TOK_TYPE, 0, 0},
		{"POST", "srv/", action_cs_post, TOK_NS | TOK_TYPE, TOK_ACTION, 0},
		{"DELETE", "srv/", action_cs_del, TOK_NS | TOK_TYPE, 0, 0},
		/// lock, unlock
		{NULL, NULL, NULL, 0, 0, 0}
	};
	return req_args_call (rq, rp, uri, path, cs_actions);
}
