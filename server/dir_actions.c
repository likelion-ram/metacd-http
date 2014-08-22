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

struct dir_args_s
{
	struct hc_url_s *url;
	gchar *type;
	gchar *renew;

	const gchar *uri;
	struct http_request_s *rq;
	struct http_reply_ctx_s *rp;
};


static void
dir_args_clear (struct dir_args_s *args)
{
	if (args->url)
		hc_url_clean(args->url);
	if (args->type)
		g_free(args->type);
	if (args->renew)
		g_free(args->renew);
	memset(args, 0, sizeof(struct dir_args_s));
}

static GError*
dir_args_extract(const gchar *uri, struct dir_args_s *args)
{
	GError* _on_ns(const gchar *v) {
		hc_url_set(args->url, HCURL_NS, *v ? v : NULL);
		return NULL;
	}
	GError* _on_ref(const gchar *v) {
		hc_url_set(args->url, HCURL_REFERENCE, *v ? v : NULL);
		return NULL;
	}
	GError* _on_type(const gchar *v) {
		metautils_str_replace(&args->type, v);
		return NULL;
	}
	GError* _on_renew(const gchar *v) {
		metautils_str_replace(&args->renew, v);
		return NULL;
	}

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"ref", _on_ref},
		{"type", _on_type},
		{"renew", _on_renew},
		{NULL, NULL}
	};
	return _split_and_parse_url(uri, actions);
}


static GString *
_pack_m1url_list(gchar **urlv)
{
	GString *gstr = g_string_new("{");
	_append_status(gstr, 200, "OK");
	g_string_append(gstr, ",\"srv\":[");
	for (gchar **v=urlv; v && *v ;v++) {
		struct meta1_service_url_s *m1 = meta1_unpack_url(*v);
		meta1_service_url_encode_json(gstr, m1);
		meta1_service_url_clean(m1);
		if (*(v+1))
			g_string_append_c(gstr, ',');
	}
	g_string_append(gstr, "]}");
	return gstr;
}

static GString *
_pack_and_freev_m1url_list(gchar **urlv)
{
	GString *result = _pack_m1url_list(urlv);
	g_strfreev(urlv);
	return result;
}

static GString *
_pack_and_freev_pairs (gchar **pairs)
{
	GString *out = g_string_new("{");
	for (gchar **pp=pairs; pp && *pp ;++pp) {
		if (pp != pairs)
			g_string_append_c(out, ',');
		gchar *k = *pp;
		gchar *sep = strchr(k, '=');
		gchar *v = sep+1;
		g_string_append_printf(out, "\"%.*s\":\"%s\"", (int)(sep-k), k, v);
	}
	g_string_append_c(out, '}');
	g_strfreev(pairs);
	return out;
}


static GError*
_m1_action (const struct dir_args_s *args, gchar **m1v, GError* (*hook) (const gchar *m1))
{
	for (gchar **pm1=m1v; *pm1 ;++pm1) {
		struct meta1_service_url_s *m1 = meta1_unpack_url(*pm1);
		if (!m1)
			continue;

		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1->host, NULL, &m1a)) {
			GRID_INFO("Invalid META1 [%s] for [%s]",
					m1->host, hc_url_get(args->url, HCURL_WHOLE));
			meta1_service_url_clean(m1);
			continue;
		}

		GError *err = hook(m1->host);
		meta1_service_url_clean(m1);
		if (!err)
			return NULL;
		else if (err->code == CODE_REDIRECT)
			g_clear_error(&err);
		else {
			g_prefix_error(&err, "META1 error: ");
			return err;
		}
	}
	return NEWERROR(CODE_UNAVAILABLE, "No meta1 answered");
}

static GError*
_m1_locate_and_action (const struct dir_args_s *args, GError* (*hook) ())
{
	gchar **m1v = NULL;
	GError *err = hc_resolve_reference_directory (resolver, args->url, &m1v);
	if (NULL != err) {
		g_prefix_error (&err, "No META1: ");
		return err;
	}
	g_assert(m1v != NULL);
	err = _m1_action(args, m1v, hook);
	g_strfreev(m1v);
	return err;
}

static GError *
decode_json_m1url (struct meta1_service_url_s **out, void *b, gsize blen)
{
	struct json_tokener *parser = json_tokener_new ();
	struct json_object *jbody = json_tokener_parse_ex(parser, (char*) b, blen);
	GError *err = meta1_service_url_load_json_object(jbody, out);
	json_object_put (jbody);
	json_tokener_free (parser);
	return err;
}

static GError *
decode_json_string_array (const gchar *k, gchar ***pkeys, void *b, gsize blen)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	gchar **keys = NULL;
	GError *err = NULL;

	// Parse the keys
	parser = json_tokener_new ();
	jbody = json_tokener_parse_ex(parser, (char*) b, blen);
	if (!json_object_is_type(jbody, json_type_object)) {
		err = BADREQ("Invalid/Unexpected JSON");
	} else {
		struct json_object *jkeys;
		if (!json_object_object_get_ex(jbody, k, &jkeys)
				|| !json_object_is_type(jkeys, json_type_array)) {
			err = BADREQ("No/Invalid '%s' section", k);
		} else {
			GPtrArray *v = g_ptr_array_new();
			guint count = 0;
			for (gint i=json_object_array_length(jkeys); i>0 ;--i) {
				++ count;
				struct json_object *item = json_object_array_get_idx(jkeys, i-1);
				if (!json_object_is_type(item, json_type_string)) {
					err = BADREQ("Invalid key at body['%s'][%u]", k, count);
					break;
				}
				g_ptr_array_add(v, g_strdup(json_object_get_string(item)));
			}
			if (!err) {
				g_ptr_array_add(v, NULL);
				keys = (gchar**) g_ptr_array_free(v, FALSE);
			} else {
				g_ptr_array_free(v, TRUE);
			}
		}
	}
	json_object_put (jbody);
	json_tokener_free (parser);

	*pkeys = keys;
	return err;
}


static enum http_rc_e
action_dir_list (const struct dir_args_s *args)
{
	gchar **urlv = NULL;
	GError *err = hc_resolve_reference_service (resolver,
			args->url, args->type, &urlv);
	g_assert((err!=NULL) ^ (urlv!=NULL));
	if (NULL != err)
		return _reply_soft_error(args->rp, err);
	return _reply_success_json(args->rp, _pack_and_freev_m1url_list(urlv));
}

static enum http_rc_e
action_dir_link (const struct dir_args_s *args)
{
	gchar **urlv = NULL;
	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		if (metautils_cfg_get_bool(args->renew, FALSE)) {
			urlv = meta1v2_remote_poll_reference_service(&m1a, &err,
					hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
					args->type, 30.0, 60.0, NULL);
		} else {
			urlv = meta1v2_remote_link_service(&m1a, &err,
					hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
					args->type, 30.0, 60.0, NULL);
		}
		return err;
	}

	GError *err = _m1_locate_and_action(args, hook);
	if (err)
		return _reply_soft_error(args->rp, err);
	g_assert(urlv != NULL);
	return _reply_success_json(args->rp, _pack_and_freev_m1url_list(urlv));
}

static enum http_rc_e
action_dir_force (const struct dir_args_s *args)
{
	GError *err = NULL;
	gchar *url = NULL;

	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_force_reference_service(&m1a, &e,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				"", 30.0, 60.0, NULL);
		return e;
	}

	struct meta1_service_url_s *m1u = NULL;
	err = decode_json_m1url(&m1u, args->rq->body->data, args->rq->body->len);
	if (!err) {
		url = meta1_pack_url(m1u);
		meta1_service_url_clean(m1u);
		err = _m1_locate_and_action(args, hook);
		g_free(url);
		url = NULL;
	}
	if (err)
		return _reply_soft_error(args->rp, err);
	return _reply_success_json(args->rp, NULL);
}

static enum http_rc_e
action_dir_unlink (const struct dir_args_s *args)
{
	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_unlink_service(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				args->type, 30.0, 60.0, NULL);
		return err;
	}

	GError *err = _m1_locate_and_action(args, hook);
	if (!err)
		return _reply_success_json(args->rp, NULL);
	return _reply_soft_error(args->rp, err);
}

static enum http_rc_e
action_dir_has (const struct dir_args_s *args)
{
	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_has_reference(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				30.0, 60.0);
		return err;
	}
	GError *err = _m1_locate_and_action(args, hook);
	if (!err)
		return _reply_success_json(args->rp, NULL);
	return _reply_soft_error(args->rp, err);
}

static enum http_rc_e
action_dir_create (const struct dir_args_s *args)
{
	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_create_reference(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				hc_url_get(args->url, HCURL_REFERENCE),
				30.0, 60.0, NULL);
		return err;
	}
	GError *err = _m1_locate_and_action(args, hook);
	if (!err)
		return _reply_success_json(args->rp, NULL);
	return _reply_soft_error(args->rp, err);
}

static enum http_rc_e
action_dir_destroy (const struct dir_args_s *args)
{
	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *err = NULL;
		meta1v2_remote_delete_reference(&m1a, &err,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				30.0, 60.0, NULL);
		return err;
	}
	GError *err = _m1_locate_and_action(args, hook);
	if (!err)
		return _reply_success_json(args->rp, NULL);
	return _reply_soft_error(args->rp, err);
}

static enum http_rc_e
action_dir_propget (const struct dir_args_s *args)
{
	gchar ** keys = NULL;
	GError *err = decode_json_string_array ("keys", &keys,
			args->rq->body->data, args->rq->body->len);

	// Execute the request
	gchar **pairs = NULL;
	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_reference_get_property(&m1a, &e,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				keys, &pairs, 30.0, 60.0);
		return e;
	}

	if (!err) {
		err = _m1_locate_and_action(args, hook);
		g_strfreev(keys);
		keys = NULL;
	}
	if (!err)
		return _reply_success_json(args->rp, _pack_and_freev_pairs(pairs));
	return _reply_soft_error(args->rp, err);
}

static enum http_rc_e
action_dir_propset (const struct dir_args_s *args)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GError *err = NULL;
	gchar **pairs = NULL;

	parser = json_tokener_new ();
	jbody = json_tokener_parse_ex(parser, (char*) args->rq->body->data,
			args->rq->body->len);
	if (!json_object_is_type(jbody, json_type_object)) {
		err = BADREQ("Unexpected JSON");
	} else {
		struct json_object *jpairs;
		if (!json_object_object_get_ex(jbody, "pairs", &jpairs)
				|| !json_object_is_type(jpairs, json_type_object)) {
			err = BADREQ("No/Invalid 'pairs' section");
		} else {
			GPtrArray *v = g_ptr_array_new();
			guint count = 0;
			json_object_object_foreach(jpairs,key,val) {
				++ count;
				if (!json_object_is_type(val, json_type_string)) {
					err = BADREQ("Invalid property doc['pairs']['%s']", key);
					break;
				}
				g_ptr_array_add(v, g_strdup_printf("%s=%s", key,
							json_object_get_string(val)));
			}
			if (!err) {
				g_ptr_array_add(v, NULL);
				pairs = (gchar**) g_ptr_array_free(v, FALSE);
			} else {
				g_ptr_array_free(v, TRUE);
			}
		}
	}
	json_object_put (jbody);
	json_tokener_free (parser);

	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_reference_set_property(&m1a, &e,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				pairs, 30.0, 60.0, NULL);
		return e;
	}

	if (!err) {
		err = _m1_locate_and_action(args, hook);
		g_free(pairs);
	}
	if (!err)
		return _reply_success_json(args->rp, NULL);
	return _reply_soft_error(args->rp, err);
}

static enum http_rc_e
action_dir_propdel (const struct dir_args_s *args)
{
	gchar ** keys = NULL;
	GError *err = decode_json_string_array ("keys", &keys,
			args->rq->body->data, args->rq->body->len);

	// Execute the request
	GError* hook (const gchar *m1) {
		struct addr_info_s m1a;
		if (!grid_string_to_addrinfo(m1, NULL, &m1a))
			return NEWERROR(CODE_NETWORK_ERROR, "Invalid M1 address");
		GError *e = NULL;
		meta1v2_remote_reference_del_property(&m1a, &e,
				hc_url_get(args->url, HCURL_NS), hc_url_get_id(args->url),
				keys, 30.0, 60.0, NULL);
		return e;
	}

	if (!err) {
		err = _m1_locate_and_action(args, hook);
		g_strfreev(keys);
		keys = NULL;
	}
	if (!err)
		return _reply_success_json(args->rp, NULL);
	return _reply_soft_error(args->rp, err);
}


static enum http_rc_e
action_dir_flush_low (const struct dir_args_s *args)
{
	hc_resolver_flush_services(resolver);
	return _reply_success_json(args->rp, NULL);
}

static enum http_rc_e
action_dir_flush_high (const struct dir_args_s *args)
{
	hc_resolver_flush_csm0(resolver);
	return _reply_success_json(args->rp, NULL);
}

static enum http_rc_e
action_dir_set_max_high (const struct dir_args_s *args)
{
	hc_resolver_set_max_csm0(resolver, atoi(args->uri));
	return _reply_success_json(args->rp, NULL);
}

static enum http_rc_e
action_dir_set_max_low (const struct dir_args_s *args)
{
	hc_resolver_set_max_services (resolver, atoi(args->uri));
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_dir_set_ttl_high (const struct dir_args_s *args)
{
	hc_resolver_set_ttl_csm0 (resolver, atoi(args->uri));
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_dir_set_ttl_low (const struct dir_args_s *args)
{
	hc_resolver_set_ttl_services (resolver, atoi(args->uri));
	return _reply_success_json (args->rp, NULL);
}

static enum http_rc_e
action_dir_status (const struct dir_args_s *args)
{
	struct hc_resolver_stats_s s;
	memset(&s, 0, sizeof(s));
	hc_resolver_info(resolver, &s);

	GString *gstr = g_string_new("{");
	g_string_append_printf(gstr, " \"clock\":%lu,", s.clock);
	g_string_append_printf(gstr, " \"csm0\":{"
			"\"count\":%"G_GINT64_FORMAT",\"max\":%u,\"ttl\":%lu},",
			s.csm0.count, s.csm0.max, s.csm0.ttl);
	g_string_append_printf(gstr, " \"meta1\":{"
			"\"count\":%"G_GINT64_FORMAT",\"max\":%u,\"ttl\":%lu}",
			s.services.count, s.services.max, s.services.ttl);
	g_string_append_c(gstr, '}');
	return _reply_success_json(args->rp, gstr);
}

static struct directory_action_s
{
	const gchar *method;
	const gchar *prefix;
	enum http_rc_e (*hook) (const struct dir_args_s *args);
	unsigned int expectations;
} dir_actions[] = {

	// old patterns maintained because publicly exposed and used.
	{ "GET", "list/", action_dir_list,     TOK_NS|TOK_REF|TOK_TYPE},

	// new patterns
	{ "GET",    "ref/", action_dir_has,     TOK_NS|TOK_REF},
	{ "POST",   "ref/", action_dir_create,  TOK_NS|TOK_REF},
	{ "DELETE", "ref/", action_dir_destroy, TOK_NS|TOK_REF},

	{ "GET",    "prop/", action_dir_propget, TOK_NS|TOK_REF },
	{ "POST",   "prop/", action_dir_propset, TOK_NS|TOK_REF },
	{ "DELETE", "prop/", action_dir_propdel, TOK_NS|TOK_REF },

	{ "GET",      "srv/", action_dir_list,   TOK_NS|TOK_REF|TOK_TYPE},
	{ "POST",     "srv/", action_dir_link,   TOK_NS|TOK_REF},
	{ "DELETE",   "srv/", action_dir_unlink, TOK_NS|TOK_REF},
	{ "PUT",      "srv/", action_dir_force,  TOK_NS|TOK_REF},

	{ "GET",    "status/",       action_dir_status,       0 },
	{ "POST",   "flush/high/",   action_dir_flush_high,   0 },
	{ "POST",   "flush/low/",    action_dir_flush_low,    0 },
	{ "POST",   "set/ttl/high/", action_dir_set_ttl_high, 0 },
	{ "POST",   "set/ttl/low/",  action_dir_set_ttl_low,  0 },
	{ "POST",   "set/max/high/", action_dir_set_max_high, 0 },
	{ "POST",   "set/max/low/",  action_dir_set_max_low,  0 },

	{ NULL, NULL, NULL, 0 }
};

static enum http_rc_e
action_directory_real (struct directory_action_s *pa, struct dir_args_s *args)
{
	if ((pa->expectations & TOK_NS) && !hc_url_has(args->url, HCURL_NS))
		return _reply_format_error(args->rp, BADREQ("Missing NS"));
	if ((pa->expectations & TOK_REF) && !hc_url_has(args->url, HCURL_REFERENCE))
		return _reply_format_error(args->rp, BADREQ("Missing REF"));
	if ((pa->expectations & TOK_TYPE) && !args->type)
		return _reply_format_error(args->rp, BADREQ("Missing TYPE"));
	if (!validate_namespace(hc_url_get(args->url, HCURL_NSPHYS)))
		return _reply_soft_error(args->rp, NEWERROR(
			CODE_NAMESPACE_NOTMANAGED, "Invalid NS"));
	return pa->hook(args);
}

static enum http_rc_e
action_directory(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	gboolean matched = FALSE;

	for (struct directory_action_s *pa = dir_actions; pa->prefix ;++pa) {
		if (!g_str_has_prefix(uri, pa->prefix))
			continue;
		matched = TRUE;
		if (0 != strcmp(rq->cmd, pa->method))
			continue;

		struct dir_args_s args;
		memset(&args, 0, sizeof(args));
		args.url = hc_url_empty();
		args.uri = uri + strlen(pa->prefix);
		args.rq = rq;
		args.rp = rp;

		GError *err;
		enum http_rc_e e;
		if (NULL != (err = dir_args_extract(args.uri, &args)))
			e = _reply_format_error(rp, err);
		else
			e = action_directory_real(pa, &args);
		dir_args_clear(&args);
		return e;
	}

	if (matched)
		return _reply_method_error(rp);
	return _reply_no_handler(rp);
}

