
static GError*
_extract_m2_url(const gchar *uri, struct hc_url_s **rurl)
{
	struct hc_url_s *url = hc_url_empty();

	GError* _on_ns(const gchar *v) {
		hc_url_set(url, HCURL_NS, *v ? v : NULL);
		return NULL;
	}
	GError* _on_ref(const gchar *v) {
		hc_url_set(url, HCURL_REFERENCE, *v ? v : NULL);
		return NULL;
	}
	GError* _on_path(const gchar *v) {
		hc_url_set(url, HCURL_PATH, *v ? v : NULL);
		return NULL;
	}
	GError* _on_version(const gchar *v) {
		hc_url_set(url, HCURL_VERSION, *v ? v : NULL);
		return NULL;
	}
	GError* _on_size(const gchar *v) {
		hc_url_set_option(url, "size", *v ? v : NULL);
		return NULL;
	}
	GError* _on_stgpol(const gchar *v) {
		hc_url_set_option(url, "stgpol", *v ? v : NULL);
		return NULL;
	}
	GError* _on_verpol(const gchar *v) {
		hc_url_set_option(url, "verpol", *v ? v : NULL);
		return NULL;
	}

	struct url_action_s actions[] = {
		{"ns", _on_ns},
		{"ref", _on_ref},
		{"path", _on_path},
		{"version", _on_version},
		{"size", _on_size},
		{"stgpol", _on_stgpol},
		{"verpol", _on_verpol},
		{NULL, NULL}
	};
	GError *e = _split_and_parse_url(uri, actions);
	if (e) {
		hc_url_clean(url);
		return e;
	}
	else {
		*rurl = url;
		return NULL;
	}
}

static GError *
_resolve_m2_and_do(struct hc_resolver_s *r, struct hc_url_s *u,
		GError* (*hook) (struct meta1_service_url_s *m2))
{
	gchar **m2v = NULL;
	GError *err;

	if (NULL != (err = hc_resolve_reference_service(r, u, "meta2", &m2v))) {
		g_prefix_error(&err, "Resolution error: ");
		return err;
	}

	if (!*m2v)
		err = NEWERROR(CODE_CONTAINER_NOTFOUND, "No meta2 located");
	else {
		for (; m2v && *m2v ;++m2v) {
			struct meta1_service_url_s *m2 = meta1_unpack_url(*m2v);
			err = hook(m2);
			meta1_service_url_clean(m2);

			if (!err)
				goto exit;

			GRID_INFO("M2V2 error : (%d) %s", err->code, err->message);
			g_prefix_error(&err, "M2V2 error: ");

			if (err->code >= 400)
				goto exit;
			g_clear_error(&err);
		}
		if (!err)
			err = NEWERROR(500, "No META2 replied");
	}
exit:
	g_strfreev(m2v);
	return err;
}

static GError*
_single_get_v1(struct meta1_service_url_s *m2, struct hc_url_s *url, GSList **beans)
{
	int rc;
	GError *err = NULL;
	struct metacnx_ctx_s cnx;

	metacnx_clear(&cnx);
	rc = metacnx_init_with_url(&cnx, m2->host, &err);

	if (!rc) {
		g_prefix_error(&err, "URL error: ");
		return err;
	}

	struct meta2_raw_content_v2_s *raw = NULL;
	rc = meta2_remote_stat_content_v2(&cnx, hc_url_get_id(url),
			hc_url_get(url, HCURL_PATH), &raw, &err);
	metacnx_close(&cnx);
	metacnx_clear(&cnx);

	if (!rc) {
		g_prefix_error(&err, "M2V1 error: ");
		return err;
	}

	*beans = m2v2_beans_from_raw_content_v2("fake", raw);
	meta2_raw_content_v2_clean(raw);
	return NULL;
}

static enum http_rc_e
action_m2_list(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	// TODO manage snapshot ?

	GError *err;
	GSList *beans = NULL;
	GError* hook(struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_LIST(m2->host, NULL, url, 0, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(1024);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_get(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	if (NULL == hc_url_get_option_value(url, "size"))
		return _reply_format_error(rp, NEWERROR(400, "Missing size"));

	GError *err;
	GSList *beans = NULL;
	GError* hook(struct meta1_service_url_s *m2) {
		GError *e = m2v2_remote_execute_GET(m2->host, NULL, url, 0, &beans);
		if (!e || e->code == 404)
			return e;
		GRID_INFO("M2V2 error : probably a M2V1");
		g_clear_error(&e);
		return _single_get_v1(m2, url, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_beans(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	gchar *end = NULL;
	const gchar *strpol = hc_url_get_option_value(url, "policy");
	const gchar *strsize = hc_url_get_option_value(url, "size");
	gint64 size = g_ascii_strtoll(strsize, &end, 10);

	GError *err;
	GSList *beans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_BEANS(m2->host, NULL, url, strpol, size, 0, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_create(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		struct m2v2_create_params_s param = {
			hc_url_get_option_value(url, "stgpol"),
			hc_url_get_option_value(url, "verpol"),
		};
		return m2v2_remote_execute_CREATE(m2->host, NULL, url, &param);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_destroy(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_DESTROY(m2->host, NULL, url, 0);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_open(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_OPEN(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_close(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_CLOSE(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_has(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_HAS(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}
	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_dedup(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	gboolean first = TRUE;
	GString *gstr = g_string_new("{\"\":[");
	GError* hook (struct meta1_service_url_s *m2) {
		gchar *msg = NULL;
		GError *e = m2v2_remote_execute_DEDUP(m2->host, NULL, url, 0, &msg);
		if (msg) {
			if (first) {
				g_string_append_c(gstr, ',');
				first = FALSE;
			}
			g_string_append_c(gstr, '"');
			g_string_append(gstr, msg); // TODO escpae this!
			g_string_append_c(gstr, '"');
		}
		return e;
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_string_free(gstr, TRUE);
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	g_string_append(gstr, "]}");
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_purge(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GSList *beans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_PURGE(m2->host, NULL, url, FALSE, 10.0, 10.0, &beans);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_exitelection(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError *err;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_EXITELECTION(m2->host, NULL, url);
	}
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_stgpol(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	const gchar *stgpol = hc_url_get_option_value(url, "stgpol");
	if (!stgpol)
		return _reply_format_error(rp, NEWERROR(400, "Missing storage policy"));

	GSList *beans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_STGPOL(m2->host, NULL, url, stgpol, &beans);
	}
	GError *err;
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2(beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_touch(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError* hook (struct meta1_service_url_s *m2) {
		if (hc_url_has(url, HCURL_PATH))
			return m2v2_remote_touch_content(m2->host, NULL, url);
		else
			return m2v2_remote_touch_container_ex(m2->host, NULL, url, 0);
	}
	GError *err;
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	return _reply_success_json(rp, NULL);
}

static enum http_rc_e
action_m2_copy(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	(void) rq;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_COPY(m2->host, NULL, url, "NYI");
	}
	GError *err;
	if (NULL != (err = _resolve_m2_and_do(resolver, url, hook))) {
		g_prefix_error(&err, "M2 error: ");
		return _reply_soft_error(rp, err);
	}

	return _reply_success_json(rp, NULL);
}

static GError *
_m2_json_put(struct hc_url_s *url, struct json_object *jbody, GSList **out)
{
	GSList *ibeans = NULL, *obeans = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans(&ibeans, jbody, "beans"))) {
		_bean_cleanl2 (ibeans);
		ibeans = NULL;
		return err;
	}

	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_PUT(m2->host, NULL, url, ibeans, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (ibeans);
	g_assert((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static GError *
_m2_json_spare(struct hc_url_s *url, struct json_object *jbody, GSList **out)
{
	GSList *notin = NULL, *broken = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans(&notin, jbody, "notin"))) {
		_bean_cleanl2 (notin);
		_bean_cleanl2 (broken);
		return err;
	}
	if (NULL != (err = _jbody_to_beans(&broken, jbody, "broken"))) {
		_bean_cleanl2 (notin);
		_bean_cleanl2 (broken);
		return err;
	}

	GSList *obeans = NULL;
	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_SPARE(m2->host, NULL, url,
				hc_url_get_option_value(url, "stgpol"),
				notin, broken, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (broken);
	_bean_cleanl2 (notin);
	g_assert((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static GError *
_m2_json_append(struct hc_url_s *url, struct json_object *jbody, GSList **out)
{
	GSList *ibeans = NULL, *obeans = NULL;
	GError *err;

	if (NULL != (err = _jbody_to_beans(&ibeans, jbody, "beans"))) {
		_bean_cleanl2 (ibeans);
		ibeans = NULL;
		return err;
	}

	GError* hook (struct meta1_service_url_s *m2) {
		return m2v2_remote_execute_APPEND(m2->host, NULL, url, ibeans, &obeans);
	}
	err = _resolve_m2_and_do (resolver, url, hook);
	_bean_cleanl2 (ibeans);
	g_assert((err != NULL) ^ (obeans != NULL));
	if (!err)
		*out = obeans;
	else
		_bean_cleanl2 (obeans);
	return err;
}

static GError *
_m2_json_overwrite(struct hc_url_s *url, struct json_object *jbody)
{
	(void) url, (void) jbody;
	return NEWERROR(500, "Not implemented");
}

static enum http_rc_e
action_m2_put(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_put(url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (err)
		return _reply_soft_error(rp, err);

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_spare(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_spare(url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (err)
		return _reply_soft_error(rp, err);

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_append(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GSList *beans = NULL;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_append(url, jbody, &beans);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (err)
		return _reply_soft_error(rp, err);

	GString *gstr = g_string_sized_new(512);
	_json_dump_all_beans(gstr, url, beans);
	_bean_cleanl2 (beans);
	return _reply_success_json(rp, gstr);
}

static enum http_rc_e
action_m2_overwrite(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		struct hc_url_s *url)
{
	struct json_tokener *parser;
	struct json_object *jbody;
	GError *err;

	parser = json_tokener_new ();
	g_byte_array_append (rq->body, (guint8*)"", 1);
	jbody = json_tokener_parse((gchar*) rq->body->data);
	err = _m2_json_overwrite(url, jbody);
	json_object_put (jbody);
	json_tokener_free (parser);

	if (!err)
		return _reply_success_json(rp, NULL);
	else
		return _reply_soft_error(rp, err);
}

struct m2_action_s
{
	const gchar *method;
	const gchar *prefix;
	enum http_rc_e (*hook) (struct http_request_s *rq,
			struct http_reply_ctx_s *rp, struct hc_url_s *url);
	unsigned int expect_path;
} m2_actions[] = {
	{ "GET",  "get/",          action_m2_get,          1},
	{ "GET",  "beans/",        action_m2_beans,        1},
	{ "GET",  "list/",         action_m2_list,         0},
	{ "GET",  "has/",          action_m2_has,          0},
	{ "POST", "create/",       action_m2_create,       0},
	{ "POST", "destroy/",      action_m2_destroy,      0},
	{ "POST", "open/",         action_m2_open,         0},
	{ "POST", "close/",        action_m2_close,        0},
	{ "POST", "purge/",        action_m2_purge,        0},
	{ "POST", "dedup/",        action_m2_dedup,        0},
	{ "POST", "stgpol/",       action_m2_stgpol,       0},
	{ "POST", "exitelection/", action_m2_exitelection, 0},
	{ "POST", "touch/",        action_m2_touch,        0},
	{ "POST", "copy/",         action_m2_copy,         0},
	{ "POST", "put/",          action_m2_put,          1},
	{ "POST", "spare/",        action_m2_spare,        1},
	{ "POST", "append/",       action_m2_append,       1},
	{ "POST", "overwrite/",    action_m2_overwrite,    1},
	{ NULL, NULL, NULL, 0}
};

static enum http_rc_e
action_meta2(struct http_request_s *rq, struct http_reply_ctx_s *rp,
		const gchar *uri)
{
	for (struct m2_action_s *pa = m2_actions; pa->prefix ;++pa) {
		if (!g_str_has_prefix(uri, pa->prefix))
			continue;

		uri += strlen(pa->prefix);
		if (0 != strcmp(rq->cmd, pa->method))
			return _reply_method_error(rp);

		GError *err;
		struct hc_url_s *url = NULL;
		if (NULL != (err = _extract_m2_url(uri, &url)))
			return _reply_format_error(rp, err);

		if (!hc_url_has(url, HCURL_NS) || !hc_url_has(url, HCURL_REFERENCE)) {
			hc_url_clean(url);
			return _reply_format_error(rp, NEWERROR(400, "Missing NS/REF in url"));
		}
		if (pa->expect_path && !hc_url_has(url, HCURL_PATH)) {
			hc_url_clean(url);
			return _reply_format_error(rp, NEWERROR(400, "Missing PATH in url"));
		}

		enum http_rc_e e = pa->hook(rq, rp, url);
		hc_url_clean(url);
		return e;
	}
	return _reply_no_handler(rp);
}

