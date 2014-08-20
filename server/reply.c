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

static void
_append_status(GString *gstr, gint code, const gchar *msg)
{
	g_string_append_printf(gstr,
			"\"status\":%d,\"message\":\"%s\"", code, msg);
}

static GString*
_create_status(gint code, const gchar *msg)
{
	GString *gstr = g_string_sized_new(256);
	g_string_append_c(gstr, '{');
	_append_status(gstr, code, msg);
	g_string_append_c(gstr, '}');
	return gstr;
}

static GString*
_create_status_error(GError *e)
{
	GString *gstr;
	if (e) {
		gstr = _create_status(e->code, e->message);
		g_error_free(e);
	} else {
		gstr = _create_status(500, "unknown error");
	}
	return gstr;
}

static void
_append_url(GString *gstr, struct hc_url_s *url)
{
	if (!url)
		g_string_append_printf(gstr, "\"URL\":null");
	else
		g_string_append_printf(gstr,
				"\"URL\":{\"ns\":\"%s\",\"ref\":\"%s\",\"path\":\"%s\"}",
				none(hc_url_get(url, HCURL_NS)),
				none(hc_url_get(url, HCURL_REFERENCE)),
				none(hc_url_get(url, HCURL_PATH)));
}

static enum http_rc_e
_reply_no_handler(struct http_reply_ctx_s *rp)
{
	rp->set_body(NULL, 0);
	rp->set_status(404, "No handler found");
	rp->finalize();
	return HTTPRC_DONE;
}

static enum http_rc_e
_reply_json(struct http_reply_ctx_s *rp, int code, const gchar *msg, GString *gstr)
{
	rp->set_status(code, msg);
	if (gstr) {
		rp->set_body_gstr(gstr);
		rp->set_content_type("application/json");
	} else {
		rp->set_body(NULL, 0);
	}
	rp->finalize();
	return HTTPRC_DONE;
}

static enum http_rc_e
_reply_soft_error(struct http_reply_ctx_s *rp, GError *err)
{
	if (err->code < 100)
		err->code = CODE_UNAVAILABLE;
	return _reply_json(rp, 200, "OK", _create_status_error(err));
}

static enum http_rc_e
_reply_format_error(struct http_reply_ctx_s *rp, GError *err)
{
	return _reply_json(rp, 400, "Bad request", _create_status_error(err));
}

static enum http_rc_e
_reply_system_error(struct http_reply_ctx_s *rp, GError *err)
{
	return _reply_json(rp, 500, "Internal error", _create_status_error(err));
}

static enum http_rc_e
_reply_method_error(struct http_reply_ctx_s *rp)
{
	return _reply_json(rp, 405, "Method not allowed", NULL);
}

static enum http_rc_e
_reply_success_json(struct http_reply_ctx_s *rp, GString *gstr)
{
	return _reply_json(rp, 200, "OK", gstr);
}

