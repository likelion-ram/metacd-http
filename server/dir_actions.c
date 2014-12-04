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
_pack_m1url_list (gchar ** urlv)
{
	GString *gstr = g_string_new ("[");
	for (gchar ** v = urlv; v && *v; v++) {
		struct meta1_service_url_s *m1 = meta1_unpack_url (*v);
		meta1_service_url_encode_json (gstr, m1);
		meta1_service_url_clean (m1);
		if (*(v + 1))
			g_string_append_c (gstr, ',');
	}
	g_string_append (gstr, "]");
	return gstr;
}

static GString *
_pack_and_freev_m1url_list (gchar ** urlv)
{
	GString *result = _pack_m1url_list (urlv);
	g_strfreev (urlv);
	return result;
}

static GString *
_pack_and_freev_pairs (gchar ** pairs)
{
	GString *out = g_string_new ("{");
	for (gchar ** pp = pairs; pp && *pp; ++pp) {
		if (pp != pairs)
			g_string_append_c (out, ',');
		gchar *k = *pp;
		gchar *sep = strchr (k, '=');
		gchar *v = sep + 1;
		g_string_append_printf (out, "\"%.*s\":\"%s\"", (int) (sep - k), k, v);
	}
	g_string_append_c (out, '}');
	g_strfreev (pairs);
	return out;
}

static GError *
_m1_action (const struct req_args_s *args, gchar ** m1v,
	GError * (*hook) (const gchar * m1))
{
	for (gchar ** pm1 = m1v; *pm1; ++pm1) {
		struct meta1_service_url_s *m1 = meta1_unpack_url (*pm1);
		if (!m1)
			continue;

		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1->host, NULL, &m1a)) {
			GRID_INFO ("Invalid META1 [%s] for [%s]",
				m1->host, hc_url_get (args->url, HCURL_WHOLE));
			meta1_service_url_clean (m1);
			continue;
		}

		GError *err = hook (m1->host);
		meta1_service_url_clean (m1);
		if (!err)
			return NULL;
		else if (err->code == CODE_REDIRECT)
			g_clear_error (&err);
		else {
			g_prefix_error (&err, "META1 error: ");
			return err;
		}
	}
	return NEWERROR (CODE_UNAVAILABLE, "No meta1 answered");
}

static GError *
_m1_locate_and_action (const struct req_args_s *args, GError * (*hook) ())
{
	gchar **m1v = NULL;
	GError *err = hc_resolve_reference_directory (resolver, args->url, &m1v);
	if (NULL != err) {
		g_prefix_error (&err, "No META1: ");
		return err;
	}
	g_assert (m1v != NULL);
	err = _m1_action (args, m1v, hook);
	g_strfreev (m1v);
	return err;
}

static GError *
decode_json_m1url (struct meta1_service_url_s **out, void *b, gsize blen)
{
	struct json_tokener *parser = json_tokener_new ();
	struct json_object *jbody =
		json_tokener_parse_ex (parser, (char *) b, blen);
	GError *err = meta1_service_url_load_json_object (jbody, out);
	json_object_put (jbody);
	json_tokener_free (parser);
	return err;
}

static GError *
decode_json_string_array (const gchar * k, gchar *** pkeys, void *b, gsize blen)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	gchar **keys = NULL;
	GError *err = NULL;

	// Parse the keys
	parser = json_tokener_new ();
	jbody = json_tokener_parse_ex (parser, (char *) b, blen);
	if (!json_object_is_type (jbody, json_type_object)) {
		err = BADREQ ("Invalid/Unexpected JSON");
	} else {
		struct json_object *jkeys;
		if (!json_object_object_get_ex (jbody, k, &jkeys)
			|| !json_object_is_type (jkeys, json_type_array)) {
			err = BADREQ ("No/Invalid '%s' section", k);
		} else {
			GPtrArray *v = g_ptr_array_new ();
			guint count = 0;
			for (gint i = json_object_array_length (jkeys); i > 0; --i) {
				++count;
				struct json_object *item =
					json_object_array_get_idx (jkeys, i - 1);
				if (!json_object_is_type (item, json_type_string)) {
					err = BADREQ ("Invalid key at body['%s'][%u]", k, count);
					break;
				}
				g_ptr_array_add (v, g_strdup (json_object_get_string (item)));
			}
			if (!err) {
				g_ptr_array_add (v, NULL);
				keys = (gchar **) g_ptr_array_free (v, FALSE);
			} else {
				g_ptr_array_free (v, TRUE);
			}
		}
	}
	json_object_put (jbody);
	json_tokener_free (parser);

	*pkeys = keys;
	return err;
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_dir_srv_list (const struct req_args_s *args)
{
	gchar **urlv = NULL;
	GError *err = hc_resolve_reference_service (resolver,
		args->url, args->type, &urlv);
	g_assert ((err != NULL) ^ (urlv != NULL));

	if (!err) {

		if ((args->flags & FLAG_NOEMPTY) && !*urlv) {
			g_strfreev (urlv);
			urlv = NULL;
			return _reply_notfound_error (args->rp,
				NEWERROR (CODE_NOT_FOUND, "No service linked"));
		}
		return _reply_success_json (args->rp,
			_pack_and_freev_m1url_list (urlv));
	}

	if (err->code == CODE_CONTAINER_NOTFOUND)
		return _reply_notfound_error (args->rp, err);
	return _reply_system_error (args->rp, err);
}

static enum http_rc_e
action_dir_srv_unlink (const struct req_args_s *args)
{
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_unlink_service (&m1a, &err,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			args->type, 30.0, 60.0, NULL);
		return err;
	}

	GError *err = _m1_locate_and_action (args, hook);

	if (!err || err->code < 100) {
		/* Also decache on timeout, a majority of request succeed,
		 * and it will probably silently succeed  */
		hc_decache_reference_service (resolver, args->url, args->type);
	}

	if (!err)
		return _reply_success_json (args->rp, NULL);
	return _reply_soft_error (args->rp, err);
}

static enum http_rc_e
action_dir_srv_link (const struct req_args_s *args)
{
	gchar **urlv = NULL;
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		urlv = meta1v2_remote_link_service (&m1a, &err,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			args->type, 30.0, 60.0, NULL);
		return err;
	}

	GError *err = _m1_locate_and_action (args, hook);
	if (!err || err->code < 100) {
		/* Also decache on timeout, a majority of request succeed,
		 * and it will probably silently succeed  */
		hc_decache_reference_service (resolver, args->url, args->type);
	}

	if (err)
		return _reply_soft_error (args->rp, err);
	g_assert (urlv != NULL);
	return _reply_success_json (args->rp, _pack_and_freev_m1url_list (urlv));
}

static enum http_rc_e
action_dir_srv_force (const struct req_args_s *args)
{
	GError *err = NULL;
	gchar *url = NULL;

	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_force_reference_service (&m1a, &e,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			"", 30.0, 60.0, NULL);
		return e;
	}

	struct meta1_service_url_s *m1u = NULL;
	err = decode_json_m1url (&m1u, args->rq->body->data, args->rq->body->len);
	if (!err || err->code < 100) {
		/* Also decache on timeout, a majority of request succeed,
		 * and it will probably silently succeed  */
		hc_decache_reference_service (resolver, args->url, args->type);
	}

	if (!err) {
		url = meta1_pack_url (m1u);
		meta1_service_url_clean (m1u);
		err = _m1_locate_and_action (args, hook);
		g_free (url);
		url = NULL;
	}
	if (err)
		return _reply_soft_error (args->rp, err);
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_dir_srv_renew (const struct req_args_s *args)
{
	gchar **urlv = NULL;
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		urlv = meta1v2_remote_poll_reference_service (&m1a, &err,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			args->type, 30.0, 60.0, NULL);
		return err;
	}

	GError *err = _m1_locate_and_action (args, hook);

	if (!err || err->code < 100) {
		/* Also decache on timeout, a majority of request succeed,
		 * and it will probably silently succeed  */
		hc_decache_reference_service (resolver, args->url, args->type);
	}

	if (err)
		return _reply_soft_error (args->rp, err);
	g_assert (urlv != NULL);
	return _reply_success_json (args->rp, _pack_and_freev_m1url_list (urlv));
}

static enum http_rc_e
action_dir_srv_action (const struct req_args_s *args)
{
	if (!strcmp (args->action, "link"))
		return action_dir_srv_link (args);
	if (!strcmp (args->action, "force"))
		return action_dir_srv_force (args);
	if (!strcmp (args->action, "renew"))
		return action_dir_srv_renew (args);
	return _reply_format_error (args->rp, BADREQ ("invalid action"));
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_dir_ref_has (const struct req_args_s *args)
{
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_has_reference (&m1a, &err,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			30.0, 60.0);
		return err;
	}
	GError *err = _m1_locate_and_action (args, hook);
	if (!err)
		return _reply_success_json (args->rp, NULL);
	if (err->code == CODE_CONTAINER_NOTFOUND)
		return _reply_notfound_error (args->rp, err);
	return _reply_system_error (args->rp, err);
}

static enum http_rc_e
action_dir_ref_create (const struct req_args_s *args)
{
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_create_reference (&m1a, &err,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			hc_url_get (args->url, HCURL_REFERENCE), 30.0, 60.0, NULL);
		return err;
	}
	GError *err = _m1_locate_and_action (args, hook);
	if (!err)
		return _reply_success_json (args->rp, NULL);
	if (err->code == CODE_CONTAINER_EXISTS)
		return _reply_forbidden_error (args->rp, err);
	return _reply_system_error (args->rp, err);
}

static enum http_rc_e
action_dir_ref_destroy (const struct req_args_s *args)
{
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_delete_reference (&m1a, &err,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			30.0, 60.0, NULL);
		return err;
	}
	GError *err = _m1_locate_and_action (args, hook);
	if (!err || err->code < 100) {
		/* Also decache on timeout, a majority of request succeed,
		 * and it will probably silently succeed  */
		NSINFO_DO(if (srvtypes) {
			for (gchar ** p = srvtypes; *p; ++p)
				hc_decache_reference_service (resolver, args->url, *p);
		});
		hc_decache_reference (resolver, args->url);
	}
	if (!err)
		return _reply_success_json (args->rp, NULL);
	if (err->code == CODE_CONTAINER_NOTFOUND)
		return _reply_notfound_error (args->rp, err);
	return _reply_system_error (args->rp, err);
}

//------------------------------------------------------------------------------

static enum http_rc_e
action_dir_prop_get (const struct req_args_s *args)
{
	gchar **keys = NULL;
	GError *err = decode_json_string_array ("keys", &keys,
		args->rq->body->data, args->rq->body->len);

	// Execute the request
	gchar **pairs = NULL;
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_reference_get_property (&m1a, &e,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			keys, &pairs, 30.0, 60.0);
		return e;
	}

	if (!err) {
		err = _m1_locate_and_action (args, hook);
		g_strfreev (keys);
		keys = NULL;
	}
	if (!err)
		return _reply_success_json (args->rp, _pack_and_freev_pairs (pairs));
	return _reply_soft_error (args->rp, err);
}

static enum http_rc_e
action_dir_prop_set (const struct req_args_s *args)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GError *err = NULL;
	gchar **pairs = NULL;

	parser = json_tokener_new ();
	jbody = json_tokener_parse_ex (parser, (char *) args->rq->body->data,
		args->rq->body->len);
	if (!json_object_is_type (jbody, json_type_object)) {
		err = BADREQ ("Unexpected JSON");
	} else {
		struct json_object *jpairs;
		if (!json_object_object_get_ex (jbody, "pairs", &jpairs)
			|| !json_object_is_type (jpairs, json_type_object)) {
			err = BADREQ ("No/Invalid 'pairs' section");
		} else {
			GPtrArray *v = g_ptr_array_new ();
			guint count = 0;
			json_object_object_foreach (jpairs, key, val) {
				++count;
				if (!json_object_is_type (val, json_type_string)) {
					err = BADREQ ("Invalid property doc['pairs']['%s']", key);
					break;
				}
				g_ptr_array_add (v, g_strdup_printf ("%s=%s", key,
						json_object_get_string (val)));
			}
			if (!err) {
				g_ptr_array_add (v, NULL);
				pairs = (gchar **) g_ptr_array_free (v, FALSE);
			} else {
				g_ptr_array_free (v, TRUE);
			}
		}
	}
	json_object_put (jbody);
	json_tokener_free (parser);

	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_reference_set_property (&m1a, &e,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			pairs, 30.0, 60.0, NULL);
		return e;
	}

	if (!err) {
		err = _m1_locate_and_action (args, hook);
		g_free (pairs);
	}
	if (!err)
		return _reply_success_json (args->rp, NULL);
	return _reply_soft_error (args->rp, err);
}

static enum http_rc_e
action_dir_prop_del (const struct req_args_s *args)
{
	gchar **keys = NULL;
	GError *err = decode_json_string_array ("keys", &keys,
		args->rq->body->data, args->rq->body->len);

	// Execute the request
	GError *hook (const gchar * m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo (m1, NULL, &m1a))
			return NEWERROR (CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_reference_del_property (&m1a, &e,
			hc_url_get (args->url, HCURL_NS), hc_url_get_id (args->url),
			keys, 30.0, 60.0, NULL);
		return e;
	}

	if (!err) {
		err = _m1_locate_and_action (args, hook);
		g_strfreev (keys);
		keys = NULL;
	}
	if (!err)
		return _reply_success_json (args->rp, NULL);
	return _reply_soft_error (args->rp, err);
}

static enum http_rc_e
action_directory (struct http_request_s *rq, struct http_reply_ctx_s *rp,
	struct req_uri_s *uri, const gchar *path)
{
	static struct req_action_s dir_actions[] = {
		{"HEAD", "ref/", action_dir_ref_has, TOK_NS | TOK_REF, 0, 0},
		{"GET", "ref/", action_dir_ref_has, TOK_NS | TOK_REF, 0, 0},
		{"PUT", "ref/", action_dir_ref_create, TOK_NS | TOK_REF, 0, 0},
		{"DELETE", "ref/", action_dir_ref_destroy, TOK_NS | TOK_REF, 0, 0},

		{"GET", "srv/", action_dir_srv_list, TOK_NS | TOK_REF | TOK_TYPE, 0, 0},
		{"HEAD", "srv/", action_dir_srv_list, TOK_NS | TOK_REF | TOK_TYPE, 0, 0},
		{"DELETE", "srv/", action_dir_srv_unlink, TOK_NS | TOK_REF, 0, 0},
		{"POST", "srv/", action_dir_srv_action, TOK_NS | TOK_REF, TOK_ACTION, 0},

		{"GET", "prop/", action_dir_prop_get, TOK_NS | TOK_REF, 0, 0},
		{"DELETE", "prop/", action_dir_prop_del, TOK_NS | TOK_REF, 0, 0},
		{"POST", "prop/", action_dir_prop_set, TOK_NS | TOK_REF, TOK_ACTION, TOK_STGPOL},

		{NULL, NULL, NULL, 0, 0, 0}
	};
	return req_args_call (rq, rp, uri, path, dir_actions);
}
