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

#include <meta2v2/meta2_utils_json.h>
#include <meta2v2/autogen.h>
#include <meta2v2/generic.h>

static void
_json_dump_all_beans (GString * gstr, struct hc_url_s *url, GSList * beans)
{
	g_string_append_c (gstr, '{');
	_append_status (gstr, 200, "OK");
	g_string_append_c (gstr, ',');
	_append_url (gstr, url);
	g_string_append (gstr, ",");
	meta2_json_dump_all_beans (gstr, beans);
	g_string_append (gstr, "}");
}

static enum http_rc_e
_reply_m2_error (const struct req_args_s *args, GError * err)
{
	if (!err)
		return _reply_success_json (args->rp, NULL);
	g_prefix_error (&err, "M2 error: ");
	return _reply_soft_error (args->rp, err);
}

static enum http_rc_e
_reply_beans (const struct req_args_s *args, GError * err, GSList * beans)
{
	if (err) {
		if (err->code == 400)
			return _reply_format_error (args->rp, err);
		else if (err->code == CODE_CONTAINER_NOTFOUND || err->code == CODE_CONTENT_NOTFOUND)
			return _reply_notfound_error (args->rp, err);
		else
			return _reply_system_error (args->rp, err);
	}

	if (!beans && (args->flags & FLAG_NOEMPTY))
		return _reply_notfound_error (args->rp, NEWERROR (404,
				"No bean found"));

	GString *gstr = g_string_sized_new (512);
	_json_dump_all_beans (gstr, args->url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json (args->rp, gstr);
}

static GError *
_jbody_to_beans (GSList ** beans, struct json_object *jbody, const gchar * k)
{
	if (!json_object_is_type (jbody, json_type_object))
		return BADREQ ("Body is not a valid JSON object");
	struct json_object *jbeans = json_object_object_get (jbody, k);
	if (!jbeans)
		return BADREQ ("Section %s not found in JSON body", k);
	if (!json_object_is_type (jbeans, json_type_object))
		return BADREQ ("Section %s from body is not a JSON object", k);
	return meta2_json_object_to_beans (beans, jbeans);
}

static GError *
_resolve_m2_and_do (struct hc_resolver_s *r, struct hc_url_s *u,
	GError * (*hook) (struct meta1_service_url_s * m2))
{
	gchar **m2v = NULL;
	GError *err;

	if (NULL != (err = hc_resolve_reference_service (r, u, "meta2", &m2v))) {
		g_prefix_error (&err, "Resolution error: ");
		return err;
	}

	if (!*m2v)
		err = NEWERROR (CODE_CONTAINER_NOTFOUND, "No meta2 located");
	else {
		for (; m2v && *m2v; ++m2v) {
			struct meta1_service_url_s *m2 = meta1_unpack_url (*m2v);
			err = hook (m2);
			meta1_service_url_clean (m2);

			if (!err)
				goto exit;

			GRID_DEBUG ("M2V2 error : (%d) %s", err->code, err->message);
			g_prefix_error (&err, "M2V2 error: ");

			if (err->code >= 400)
				goto exit;
			g_clear_error (&err);
		}
		if (!err)
			err = NEWERROR (500, "No META2 replied");
	}
exit:
	g_strfreev (m2v);
	return err;
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_m2_container_list (const struct req_args_s *args)
{
	// TODO manage snapshot ?
	GSList *beans = NULL;
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_LIST (m2->host, NULL, args->url, 0, &beans);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	return _reply_beans (args, err, beans);
}

static enum http_rc_e
action_m2_container_check (const struct req_args_s *args)
{
	GError *err;
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_HAS (m2->host, NULL, args->url);
	}
	if (NULL != (err = _resolve_m2_and_do (resolver, args->url, hook))) {
		if (CODE_CONTAINER_NOTFOUND == err->code)
			return _reply_notfound_error (args->rp, err);
		g_prefix_error (&err, "M2 error: ");
		return _reply_system_error (args->rp, err);
	}
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_m2_container_create (const struct req_args_s *args)
{
	GError *hook (struct meta1_service_url_s *m2) {
		struct m2v2_create_params_s param = {
			hc_url_get_option_value (args->url, "stgpol"),
			hc_url_get_option_value (args->url, "verpol"),
			FALSE
		};
		return m2v2_remote_execute_CREATE (m2->host, NULL, args->url, &param);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	if (err && err->code == CODE_CONTAINER_NOTFOUND)	// The reference doesn't exist
		return _reply_forbidden_error (args->rp, err);
	return _reply_m2_error (args, err);
}

static enum http_rc_e
action_m2_container_destroy (const struct req_args_s *args)
{
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_DESTROY (m2->host, NULL, args->url, 0);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	return _reply_m2_error (args, err);
}

static enum http_rc_e
action_m2_container_purge (const struct req_args_s *args)
{
	GSList *beans = NULL;
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_PURGE (m2->host, NULL,
			args->url, FALSE, 30.0, 60.0, &beans);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	return _reply_beans (args, err, beans);
}

static enum http_rc_e
action_m2_container_dedup (const struct req_args_s *args)
{
	GError *err;
	gboolean first = TRUE;
	GString *gstr = g_string_new ("{\"msg\":[");
	GError *hook (struct meta1_service_url_s *m2) {
		gchar *msg = NULL;
		GError *e =
			m2v2_remote_execute_DEDUP (m2->host, NULL, args->url, 0, &msg);
		if (msg) {
			if (!first)
				g_string_append_c (gstr, ',');
			first = FALSE;
			g_string_append_c (gstr, '"');
			g_string_append (gstr, msg);	// TODO escape this!
			g_string_append_c (gstr, '"');
			g_free (msg);
		}
		return e;
	}
	err = _resolve_m2_and_do (resolver, args->url, hook);
	if (NULL != err) {
		g_string_free (gstr, TRUE);
		g_prefix_error (&err, "M2 error: ");
		if (err->code == CODE_CONTAINER_NOTFOUND)
			return _reply_notfound_error (args->rp, err);
		return _reply_system_error (args->rp, err);
	}

	g_string_append (gstr, "]}");
	return _reply_success_json (args->rp, gstr);
}

static enum http_rc_e
action_m2_container_stgpol (const struct req_args_s *args)
{
	if (!args->stgpol)
		return _reply_format_error (args->rp, BADREQ ("Missing STGPOL"));

	GSList *beans = NULL;
	GError *hook (struct meta1_service_url_s * m2) {
		return m2v2_remote_execute_STGPOL (m2->host, NULL, args->url,
			args->stgpol, &beans);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	if (NULL != err) {
		if (err->code == CODE_CONTAINER_NOTFOUND)
			return _reply_notfound_error (args->rp, err);
		return _reply_system_error (args->rp, err);
	}

	GString *gstr = g_string_sized_new (512);
	_json_dump_all_beans (gstr, args->url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json (args->rp, gstr);
}

static enum http_rc_e
action_m2_container_touch (const struct req_args_s *args)
{
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_touch_container_ex (m2->host, NULL, args->url, 0);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	if (NULL != err) {
		if (err->code == CODE_CONTAINER_NOTFOUND)
			return _reply_notfound_error (args->rp, err);
		return _reply_system_error (args->rp, err);
	}

	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_m2_container_action (const struct req_args_s *args)
{
	if (!args->action)
		return _reply_format_error (args->rp, BADREQ ("missing action"));

	if (!strcmp (args->action, "purge"))
		return action_m2_container_purge (args);
	if (!strcmp (args->action, "dedup"))
		return action_m2_container_dedup (args);
	if (!strcmp (args->action, "stgpol"))
		return action_m2_container_stgpol (args);
	if (!strcmp (args->action, "touch"))
		return action_m2_container_touch (args);

	return _reply_format_error (args->rp, BADREQ ("invalid action"));
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_m2_content_beans (const struct req_args_s *args)
{
	if (!args->size)
		return _reply_format_error (args->rp, BADREQ("Missing size estimation"));

	errno = 0;
	gchar *end = NULL;
	gint64 size = g_ascii_strtoll (args->size, &end, 10);
	if ((end && *end) || errno == ERANGE || errno == EINVAL)
		return _reply_format_error (args->rp, BADREQ("Invalid size format"));

	GSList *beans = NULL;
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_BEANS (m2->host, NULL, args->url,
			args->stgpol, size, 0, &beans);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	return _reply_beans (args, err, beans);
}

static enum http_rc_e
action_m2_content_copy (const struct req_args_s *args)
{
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_COPY (m2->host, NULL, args->url, "NYI");
	}
	GError *err;
	if (NULL != (err = _resolve_m2_and_do (resolver, args->url, hook))) {
		g_prefix_error (&err, "M2 error: ");
		return _reply_soft_error (args->rp, err);
	}

	return _reply_success_json (args->rp, NULL);
}

static GError *
_m2_json_spare (struct hc_url_s *url, struct json_object *jbody, GSList ** out)
{
	GSList *notin = NULL, *broken = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans (&notin, jbody, "notin"))
		|| NULL != (err = _jbody_to_beans (&broken, jbody, "broken"))) {
		_bean_cleanl2 (notin);
		_bean_cleanl2 (broken);
		return err;
	}

	GSList *obeans = NULL;
	GError *hook (struct meta1_service_url_s * m2) {
		return m2v2_remote_execute_SPARE (m2->host, NULL, url,
			hc_url_get_option_value (url, "stgpol"), notin, broken, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (broken);
	_bean_cleanl2 (notin);
	g_assert ((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static enum http_rc_e
action_m2_content_spare (const struct req_args_s *args)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (args->rq->body, (guint8 *) "", 1);
	jbody = json_tokener_parse ((gchar *) args->rq->body->data);
	err = _m2_json_spare (args->url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	return _reply_beans (args, err, beans);
}

static GError *
_m2_json_append (struct hc_url_s *url, struct json_object *jbody, GSList ** out)
{
	GSList *ibeans = NULL, *obeans = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans (&ibeans, jbody, "beans"))) {
		_bean_cleanl2 (ibeans);
		ibeans = NULL;
		return err;
	}

	GError *hook (struct meta1_service_url_s * m2) {
		return m2v2_remote_execute_APPEND (m2->host, NULL, url, ibeans,
			&obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (ibeans);
	g_assert ((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static enum http_rc_e
action_m2_content_append (const struct req_args_s *args)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (args->rq->body, (guint8 *) "", 1);
	jbody = json_tokener_parse ((gchar *) args->rq->body->data);
	err = _m2_json_append (args->url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	return _reply_beans (args, err, beans);
}

static GError *
_m2_json_overwrite (struct hc_url_s *url, struct json_object *jbody)
{
	(void) url, (void) jbody;
	return NEWERROR (500, "Not implemented");
}

static enum http_rc_e
action_m2_content_overwrite (const struct req_args_s *args)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (args->rq->body, (guint8 *) "", 1);
	jbody = json_tokener_parse ((gchar *) args->rq->body->data);
	err = _m2_json_overwrite (args->url, jbody);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (!err)
		return _reply_success_json (args->rp, NULL);
	else
		return _reply_format_error (args->rp, err);
}

static enum http_rc_e
action_m2_content_touch (const struct req_args_s *args)
{
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_touch_content (m2->host, NULL, args->url);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	if (NULL != err) {
		if (err->code == CODE_CONTAINER_NOTFOUND || err->code == CODE_CONTENT_NOTFOUND)
			return _reply_notfound_error (args->rp, err);
		return _reply_system_error (args->rp, err);
	}

	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_m2_content_stgpol (const struct req_args_s *args)
{
	if (!args->stgpol)
		return _reply_format_error (args->rp, BADREQ ("missing policy"));

	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_STGPOL (m2->host, NULL, args->url,
				args->stgpol, NULL);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	if (NULL != err) {
		if (err->code == CODE_CONTAINER_NOTFOUND || err->code == CODE_CONTENT_NOTFOUND)
			return _reply_notfound_error (args->rp, err);
		return _reply_system_error (args->rp, err);
	}

	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_m2_content_action (const struct req_args_s *args)
{
	if (!args->action)
		return _reply_format_error (args->rp, BADREQ ("missing action"));

	if (!strcmp (args->action, "beans"))
		return action_m2_content_beans (args);
	if (!strcmp (args->action, "copy"))
		return action_m2_content_copy (args);
	if (!strcmp (args->action, "spare"))
		return action_m2_content_spare (args);
	if (!strcmp (args->action, "touch"))
		return action_m2_content_touch (args);
	if (!strcmp (args->action, "stgpol"))
		return action_m2_content_stgpol (args);
	if (!strcmp (args->action, "append"))
		return action_m2_content_append (args);
	if (!strcmp (args->action, "force"))
		return action_m2_content_overwrite (args);
	return _reply_format_error (args->rp, BADREQ ("invalid action"));
}

static GError *
_m2_json_put (struct hc_url_s *url, struct json_object *jbody, GSList ** out)
{
	GSList *ibeans = NULL, *obeans = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans (&ibeans, jbody, "beans"))) {
		_bean_cleanl2 (ibeans);
		return err;
	}
	// Check the path in the URL matches the name of each alias
	for (GSList * l = ibeans; l; l = l->next) {
		if (DESCR (l->data) == &descr_struct_ALIASES) {
			if (0 != strcmp (hc_url_get (url, HCURL_PATH),
					ALIASES_get_alias (l->data)->str)) {
				err =
					NEWERROR (400, "Path mismatch, (%s) vs (%s)",
					hc_url_get (url, HCURL_PATH),
					ALIASES_get_alias (l->data)->str);
				_bean_cleanl2 (ibeans);
				return err;
			}
		}
	}

	GError *hook (struct meta1_service_url_s * m2) {
		return m2v2_remote_execute_PUT (m2->host, NULL, url, ibeans, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (ibeans);
	g_assert ((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static enum http_rc_e
action_m2_content_put (const struct req_args_s *args)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	jbody = json_tokener_parse_ex (parser, (char *) args->rq->body->data,
		args->rq->body->len);
	err = _m2_json_put (args->url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	return _reply_beans (args, err, beans);
}

static enum http_rc_e
action_m2_content_delete (const struct req_args_s *args)
{
	GSList *beans = NULL;
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_DEL (m2->host, NULL, args->url,
			TRUE /*sync_del?! */ , &beans);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	return _reply_beans (args, err, beans);
}

static enum http_rc_e
action_m2_content_check (const struct req_args_s *args)
{
	GSList *beans = NULL;
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_GET (m2->host, NULL, args->url, 0, &beans);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	_bean_cleanl2 (beans);
	return _reply_beans (args, err, NULL);
}

static enum http_rc_e
action_m2_content_get (const struct req_args_s *args)
{
	GSList *beans = NULL;
	GError *hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_GET (m2->host, NULL, args->url, 0, &beans);
	}
	GError *err = _resolve_m2_and_do (resolver, args->url, hook);
	return _reply_beans (args, err, beans);
}

static enum http_rc_e
action_m2_get (const struct req_args_s *args)
{
	return action_m2_content_get (args);
}

static enum http_rc_e
action_meta2 (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	const gchar * uri)
{
	static struct req_action_s m2_actions[] = {
		// Legacy
		{"GET", "get/", action_m2_get, TOK_NS | TOK_REF | TOK_PATH},

		{"PUT", "container/", action_m2_container_create, TOK_NS | TOK_REF},
		{"GET", "container/", action_m2_container_list, TOK_NS | TOK_REF},
		{"HEAD", "container/", action_m2_container_check, TOK_NS | TOK_REF},
		{"DELETE", "container/", action_m2_container_destroy, TOK_NS | TOK_REF},
		{"POST", "container/", action_m2_container_action,
			TOK_NS | TOK_REF | TOK_ACTION},
		// purge, dedup, touch, stgpol

		{"PUT", "content/", action_m2_content_put, TOK_NS | TOK_REF | TOK_PATH},
		{"GET", "content/", action_m2_content_get, TOK_NS | TOK_REF | TOK_PATH},
		{"HEAD", "content/", action_m2_content_check,
			TOK_NS | TOK_REF | TOK_PATH},
		{"DELETE", "content/", action_m2_content_delete,
			TOK_NS | TOK_REF | TOK_PATH},
		{"POST", "content/", action_m2_content_action,
			TOK_NS | TOK_REF | TOK_PATH | TOK_ACTION},
		// beans, copy, touch, stgpol, append, spare, overwrite

		{NULL, NULL, NULL, 0}
	};
	return req_args_call (rq, rp, uri, m2_actions);
}
