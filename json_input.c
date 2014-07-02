
typedef GError* (*jbean_mapper) (struct json_object*, gpointer*);

static GError*
_alias2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError*
_header2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError*
_content2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError*
_chunk2bean (struct json_object *j, gpointer *pbean)
{
	(void)j, (void)pbean;
	return NEWERROR(500, "NYI");
}

static GError *
_jarray_to_beans (GSList **out, struct json_object *jv, jbean_mapper map)
{
	if (!json_object_is_type(jv, json_type_array))
		return NEWERROR(400, "Invalid JSON, exepecting array of beans");
	int vlen = json_object_array_length (jv);
	for (int i=0; i<vlen ;++i) {
		struct json_object *j = json_object_array_get_idx (jv, i);
		if (!json_object_is_type (j, json_type_object))
			return NEWERROR(400, "Invalid JSON for a bean");
		gpointer bean = NULL;
		GError *err = map(j, &bean);
		g_assert((bean != NULL) ^ (err != NULL));
		if (err)
			return err;
		*out = g_slist_prepend(*out, bean);
	}
	return NULL;
}

static GError *
_jbody_to_beans(GSList **beans, struct json_object *jbody, const gchar *k)
{
	if (!json_object_is_type(jbody, json_type_object))
		return NEWERROR(400, "Bad format");

	struct json_object *jbeans = json_object_object_get(jbody, k);
	if (!jbeans)
		return NEWERROR(400, "Bad format, no bean");
	if (!json_object_is_type(jbody, json_type_object)) {
		json_object_put (jbeans);
		return NEWERROR(400, "Bad format");
	}

	static gchar* title[] = { "alias", "header", "content", "chunk", NULL };
	static jbean_mapper mapper[] = { _alias2bean, _header2bean, _content2bean,
		_chunk2bean };

	GError *err = NULL;
	gchar **ptitle;
	jbean_mapper *pmapper;
	for (ptitle=title,pmapper=mapper; *ptitle ;++ptitle,++pmapper) {
		struct json_object *jv = json_object_object_get (jbeans, *ptitle);
		if (!jv)
			continue;
		err = _jarray_to_beans(beans, jv, *pmapper);
		json_object_put (jv);
		if (err != NULL)
			break;
	}

	json_object_put (jbeans);
	return err;
}

