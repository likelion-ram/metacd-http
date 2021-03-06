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

#ifndef G_LOG_DOMAIN
#define G_LOG_DOMAIN "metacd.http"
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdarg.h>

#include <glib.h>
#include <json.h>

#include <metautils/lib/metautils.h>
#include <metautils/lib/metacomm.h>
#include <cluster/lib/gridcluster.h>
#include <cluster/remote/gridcluster_remote.h>
#include <server/network_server.h>
#include <server/transport_http.h>
#include <server/stats_holder.h>
#include <resolver/hc_resolver.h>
#include <meta1v2/meta1_remote.h>
#include <meta2v2/meta2v2_remote.h>
#include <meta2v2/meta2_utils.h>
#include <meta2v2/autogen.h>
#include <meta2v2/generic.h>
#include <meta2/remote/meta2_services_remote.h>

#define BADREQ(M,...) NEWERROR(CODE_BAD_REQUEST,M,##__VA_ARGS__)

#ifndef RESOLVD_DEFAULT_TTL_SERVICES
#define RESOLVD_DEFAULT_TTL_SERVICES 3600
#endif

#ifndef RESOLVD_DEFAULT_MAX_SERVICES
#define RESOLVD_DEFAULT_MAX_SERVICES 200000
#endif

#ifndef RESOLVD_DEFAULT_TTL_CSM0
#define RESOLVD_DEFAULT_TTL_CSM0 0
#endif

#ifndef RESOLVD_DEFAULT_MAX_CSM0
#define RESOLVD_DEFAULT_MAX_CSM0 0
#endif

#define XTRACE() GRID_TRACE2("%s (%s)", __FUNCTION__, hc_url_get(args->url, HCURL_WHOLE))

static struct http_request_dispatcher_s *dispatcher = NULL;
static struct network_server_s *server = NULL;

static gchar *nsname = NULL;
static struct hc_resolver_s *resolver = NULL;
static struct grid_lbpool_s *lbpool = NULL;

static struct lru_tree_s *push_queue = NULL;
static GStaticMutex push_mutex;
#define PUSH_DO(Action) do { \
	g_static_mutex_lock(&push_mutex); \
	Action ; \
	g_static_mutex_unlock(&push_mutex); \
} while (0)

static struct grid_task_queue_s *admin_gtq = NULL;
static struct grid_task_queue_s *upstream_gtq = NULL;
static struct grid_task_queue_s *downstream_gtq = NULL;

static GThread *admin_thread = NULL;
static GThread *upstream_thread = NULL;
static GThread *downstream_thread = NULL;

static struct namespace_info_s nsinfo;
static gchar **srvtypes = NULL;
static GStaticMutex nsinfo_mutex;
#define NSINFO_DO(Action) do { \
	g_static_mutex_lock(&nsinfo_mutex); \
	Action ; \
	g_static_mutex_unlock(&nsinfo_mutex); \
} while (0)

// Configuration
static gint timeout_cs_push = 4000;
static gint lb_downstream_delay = 10;
static guint lb_upstream_delay = 1;
static guint nsinfo_refresh_delay = 5;
#define METACD_LB_ENABLED (lb_downstream_delay > 0)

static guint dir_low_ttl = RESOLVD_DEFAULT_TTL_SERVICES;
static guint dir_low_max = RESOLVD_DEFAULT_MAX_SERVICES;
static guint dir_high_ttl = RESOLVD_DEFAULT_TTL_CSM0;
static guint dir_high_max = RESOLVD_DEFAULT_MAX_CSM0;

static gboolean validate_namespace (const gchar * ns);
static gboolean validate_srvtype (const gchar * n);

#include "reply.c"
#include "url.c"

#include "dir_actions.c"
#include "lb_actions.c"
#include "cs_actions.c"
#include "m2_actions.c"
#include "cache_actions.c"

// Misc. handlers --------------------------------------------------------------

static enum http_rc_e
action_status(struct http_request_s *rq, struct http_reply_ctx_s *rp,
	struct req_uri_s *uri, const gchar *path)
{
	(void) uri, (void) path;

	if (0 == strcasecmp("HEAD", rq->cmd))
		return _reply_success_json(rp, NULL);
	if (0 != strcasecmp("GET", rq->cmd))
		return _reply_method_error(rp);

	GString *gstr = g_string_sized_new (128);
	gboolean runner (const gchar *n, guint64 v) {
		g_string_append_printf(gstr, "%s = %"G_GINT64_FORMAT"\n", n, v);
		return TRUE;
	}
	grid_stats_holder_foreach(rq->client->main_stats, NULL, runner);

	struct hc_resolver_stats_s s;
	memset(&s, 0, sizeof(s));
	hc_resolver_info(resolver, &s);

	g_string_append_printf(gstr, "cache.dir.count = %"G_GINT64_FORMAT"\n", s.csm0.count);
	g_string_append_printf(gstr, "cache.dir.max = %u\n", s.csm0.max);
	g_string_append_printf(gstr, "cache.dir.ttl = %lu\n", s.csm0.ttl);
	g_string_append_printf(gstr, "cache.dir.clock = %lu\n", s.clock);

	g_string_append_printf(gstr, "cache.srv.count = %"G_GINT64_FORMAT"\n", s.services.count);
	g_string_append_printf(gstr, "cache.srv.max = %u\n", s.services.max);
	g_string_append_printf(gstr, "cache.srv.ttl = %lu\n", s.services.ttl);
	g_string_append_printf(gstr, "cache.srv.clock = %lu\n", s.clock);

	rp->set_body_gstr(gstr);
	rp->set_status(200, "OK");
	rp->set_content_type("text/x-java-properties");
	rp->finalize();
	return HTTPRC_DONE;
}

static enum http_rc_e
handler_action (gpointer u, struct http_request_s *rq,
	struct http_reply_ctx_s *rp)
{
	static struct action_s {
		const gchar *prefix;
		enum http_rc_e (*hook) (
				struct http_request_s * rq,
				struct http_reply_ctx_s * rp,
				struct req_uri_s *uri,
				const gchar *path);
	} actions[] = {
		// Legacy request handlers
		{"lb/", action_loadbalancing},

		// New request handlers
		{"m2/", action_meta2},
		{"cs/", action_conscience},
		{"dir/", action_directory},
		{"cache/", action_cache},
		{"status", action_status},
		{NULL, NULL}
	};

	(void) u;
	struct req_uri_s ruri = {NULL, NULL, NULL, NULL};
	_req_uri_extract_components (rq->req_uri, &ruri);
	GRID_TRACE2("URI path[%s] query[%s] fragment[%s]",
			ruri.path, ruri.query, ruri.fragment);

	for (struct action_s * pa = actions; pa->prefix; ++pa) {
		if (g_str_has_prefix (ruri.path + 1, pa->prefix)) {
			enum http_rc_e rc = pa->hook (rq, rp, &ruri,
					ruri.path + 1 + strlen (pa->prefix));
			_req_uri_free_components(&ruri);
			return rc;
		}
	}

	_req_uri_free_components(&ruri);
	return _reply_no_handler (rp);
}

static gboolean
validate_namespace (const gchar * ns)
{
	return 0 == strcmp (ns, nsname);
}

static gboolean
validate_srvtype (const gchar * n)
{
	gboolean rc = FALSE;
	NSINFO_DO(if (srvtypes) {
		for (gchar ** p = srvtypes; !rc && *p; ++p)
			rc = !strcmp (*p, n);
	});
	return rc;
}

static struct lru_tree_s *
_push_queue_create (void)
{
	return lru_tree_create((GCompareFunc)g_strcmp0, g_free,
			(GDestroyNotify) service_info_clean, LTO_NOATIME);
}

// Administrative tasks --------------------------------------------------------

static void
_task_expire_resolver (struct hc_resolver_s *r)
{
	hc_resolver_set_now (r, time (0));
	guint count = hc_resolver_expire (r);
	if (count)
		GRID_DEBUG ("Expired %u resolver entries", count);
	count = hc_resolver_purge (r);
	if (count)
		GRID_DEBUG ("Purged %u resolver ", count);
}

static void
_task_reload_lbpool (struct grid_lbpool_s *p)
{
	GError *err;

	if (NULL != (err = gridcluster_reconfigure_lbpool (p))) {
		GRID_NOTICE ("LBPOOL : reconfigure error : (%d) %s", err->code,
			err->message);
		g_clear_error (&err);
	}

	if (NULL != (err = gridcluster_reload_lbpool (p))) {
		GRID_NOTICE ("LBPOOL : reload error : (%d) %s", err->code,
			err->message);
		g_clear_error (&err);
	}
}

static void
_task_reload_nsinfo (gpointer p)
{
	(void) p;
	GError *err = NULL;
	struct namespace_info_s *ni;

	if (!(ni = get_namespace_info (nsname, &err))) {
		GRID_WARN ("NSINFO reload error [%s] : (%d) %s",
			nsname, err->code, err->message);
		g_clear_error (&err);
	} else {
		NSINFO_DO(namespace_info_copy (ni, &nsinfo, NULL));
		namespace_info_free (ni);
	}
}

static void
_task_reload_srvtypes (gpointer p)
{
	(void) p;
	GError *err = NULL;

	GSList *_l = list_namespace_service_types (nsname, &err);
	if (err != NULL) {
		GRID_WARN ("SRVTYPES reload error [%s] : (%d) %s",
			nsname, err->code, err->message);
		return;
	}

	gchar **newset = (gchar **) metautils_list_to_array (_l);
	g_slist_free (_l);
	_l = NULL;

	NSINFO_DO(register gchar **tmp = srvtypes;
	srvtypes = newset;
	newset = tmp;);

	if (newset)
		g_strfreev (newset);
}

// Poll some elements and forward them
static void
_task_push (gpointer p)
{
	(void) p;
	struct lru_tree_s *lru = NULL;
	GSList *tmp = NULL;
	gboolean _list (gpointer k, gpointer v, gpointer u) {
		(void) k, (void) u;
		tmp = g_slist_prepend(tmp, v);
		return FALSE;
	}

	PUSH_DO(lru = push_queue; push_queue = _push_queue_create());
	lru_tree_foreach_DEQ(lru, _list, NULL);

	struct addr_info_s csaddr;
	gchar *cs = gridcluster_get_config (nsname, "conscience", ~0);
	if (!cs) {
		GRID_ERROR("Push error: %s", "No conscience for namespace NS");
	} else {
		if (!grid_string_to_addrinfo (cs, NULL, &csaddr)) {
			GRID_ERROR("Push error: %s", "Invalid conscience address for NS");
		} else {
			GError *err = NULL;
			gcluster_push_services (&csaddr, timeout_cs_push, tmp, TRUE, &err);
			if (err != NULL) {
				GRID_WARN("Push error: (%d) %s", err->code, err->message);
				g_clear_error(&err);
			}
		}
		metautils_str_clean (&cs);
	}

	g_slist_free(tmp);
	lru_tree_destroy(lru);
}

// MAIN callbacks --------------------------------------------------------------

static void
_main_error (GError * err)
{
	GRID_ERROR ("Action failure : (%d) %s", err->code, err->message);
	g_clear_error (&err);
	grid_main_set_status (1);
}

static void
grid_main_action (void)
{
	GError *err = NULL;

	if (NULL != (err = network_server_open_servers (server))) {
		_main_error (err);
		return;
	}

	grid_task_queue_fire (admin_gtq);
	grid_task_queue_fire (upstream_gtq);
	grid_task_queue_fire (downstream_gtq);

	if (!(admin_thread = grid_task_queue_run (admin_gtq, &err))) {
		g_prefix_error (&err, "Admin thread startup failure: ");
		_main_error (err);
		return;
	}

	if (!(upstream_thread = grid_task_queue_run (upstream_gtq, &err))) {
		g_prefix_error (&err, "Upstream thread startup failure: ");
		_main_error (err);
		return;
	}

	if (!(downstream_thread = grid_task_queue_run (downstream_gtq, &err))) {
		g_prefix_error (&err, "Downstream thread startup failure: ");
		_main_error (err);
		return;
	}

	if (NULL != (err = network_server_run (server))) {
		_main_error (err);
		return;
	}
}

static struct grid_main_option_s *
grid_main_get_options (void)
{
	static struct grid_main_option_s options[] = {

		{"LbRefresh", OT_INT, {.i = &lb_downstream_delay},
			"Interval between load-balancer service refreshes (seconds)\n"
			"\t\t-1 to disable, 0 to never refresh"},
		{"NsinfoRefresh", OT_UINT, {.u = &nsinfo_refresh_delay},
			"Interval between NS configuration's refreshes (seconds)"},
		{"SrvPush", OT_INT, {.u = &lb_upstream_delay},
			"Interval between load-balancer service refreshes (seconds)\n"
			"\t\t-1 to disable, 0 to never refresh"},

		{"DirLowTtl", OT_UINT, {.u = &dir_low_ttl},
			"Directory 'low' (meta1) TTL for cache elements"},
		{"DirLowMax", OT_UINT, {.u = &dir_low_max},
			"Directory 'low' (meta1) MAX cached elements"},
		{"DirHighTtl", OT_UINT, {.u = &dir_high_ttl},
			"Directory 'high' (cs+meta0) TTL for cache elements"},
		{"DirHighMax", OT_UINT, {.u = &dir_high_max},
			"Directory 'high' (cs+meta0) MAX cached elements"},
		{NULL, 0, {.i = 0}, NULL}
	};

	return options;
}

static void
grid_main_set_defaults (void)
{
}

static void
_stop_queue (struct grid_task_queue_s **gtq, GThread **gth)
{
	if (*gth) {
		grid_task_queue_stop (*gtq);
		g_thread_join (*gth);
		*gth = NULL;
	}
	if (*gtq) {
		grid_task_queue_destroy (*gtq);
		*gtq = NULL;
	}
}

static void
grid_main_specific_fini (void)
{
	_stop_queue (&admin_gtq, &admin_thread);
	_stop_queue (&upstream_gtq, &upstream_thread);
	_stop_queue (&downstream_gtq, &downstream_thread);

	if (server) {
		network_server_close_servers (server);
		network_server_stop (server);
		network_server_clean (server);
		server = NULL;
	}
	if (dispatcher) {
		http_request_dispatcher_clean (dispatcher);
		dispatcher = NULL;
	}
	if (lbpool) {
		grid_lbpool_destroy (lbpool);
		lbpool = NULL;
	}
	if (resolver) {
		hc_resolver_destroy (resolver);
		resolver = NULL;
	}
	namespace_info_clear (&nsinfo);
	metautils_str_clean (&nsname);
	g_static_mutex_free(&nsinfo_mutex);
	g_static_mutex_free(&push_mutex);
}

static gboolean
grid_main_configure (int argc, char **argv)
{
	static struct http_request_descr_s all_requests[] = {
		{"action", handler_action},
		{NULL, NULL}
	};

	if (argc != 2) {
		GRID_ERROR ("Invalid parameter, expected : IP:PORT NS");
		return FALSE;
	}

	g_static_mutex_init (&push_mutex);
	g_static_mutex_init (&nsinfo_mutex);

	nsname = g_strdup (argv[1]);
	metautils_strlcpy_physical_ns (nsname, argv[1], strlen (nsname) + 1);

	memset (&nsinfo, 0, sizeof (nsinfo));
	metautils_strlcpy_physical_ns (nsinfo.name, argv[1], sizeof (nsinfo.name));
	nsinfo.chunk_size = 1;

	dispatcher = transport_http_build_dispatcher (NULL, all_requests);
	server = network_server_init ();
	resolver = hc_resolver_create ();
	lbpool = grid_lbpool_create (nsname);

	if (resolver) {
		hc_resolver_set_ttl_csm0 (resolver, dir_high_ttl);
		hc_resolver_set_max_csm0 (resolver, dir_high_max);
		hc_resolver_set_ttl_services (resolver, dir_low_ttl);
		hc_resolver_set_max_services (resolver, dir_low_max);
		GRID_INFO ("RESOLVER limits HIGH[%u/%u] LOW[%u/%u]",
			dir_high_max, dir_high_ttl, dir_low_max, dir_low_ttl);
	}

	// Prepare a queue responsible for upstream to the conscience
	push_queue = _push_queue_create();

	upstream_gtq = grid_task_queue_create ("upstream");

	grid_task_queue_register(upstream_gtq, (guint) lb_upstream_delay,
			(GDestroyNotify) _task_push, NULL, NULL);

	// Prepare a queue responsible for the downstream from the conscience
	downstream_gtq = grid_task_queue_create ("downstream");

	if (METACD_LB_ENABLED)
		grid_task_queue_register (downstream_gtq, (guint) lb_downstream_delay,
			(GDestroyNotify) _task_reload_lbpool, NULL, lbpool);

	// Now prepare a queue for administrative tasks, such as cache expiration,
	// configuration reloadings, etc.
	admin_gtq = grid_task_queue_create ("admin");

	grid_task_queue_register (admin_gtq, 1,
		(GDestroyNotify) _task_expire_resolver, NULL, resolver);

	grid_task_queue_register (admin_gtq, nsinfo_refresh_delay,
		(GDestroyNotify) _task_reload_nsinfo, NULL, lbpool);

	grid_task_queue_register (admin_gtq, nsinfo_refresh_delay,
		(GDestroyNotify) _task_reload_srvtypes, NULL, NULL);

	network_server_bind_host (server, argv[0],
		dispatcher, transport_http_factory);

	return TRUE;
}

static const char *
grid_main_get_usage (void)
{
	return "IP:PORT NS";
}

static void
grid_main_specific_stop (void)
{
	if (admin_gtq)
		grid_task_queue_stop (admin_gtq);
	if (server)
		network_server_stop (server);
}

static struct grid_main_callbacks main_callbacks = {
	.options = grid_main_get_options,
	.action = grid_main_action,
	.set_defaults = grid_main_set_defaults,
	.specific_fini = grid_main_specific_fini,
	.configure = grid_main_configure,
	.usage = grid_main_get_usage,
	.specific_stop = grid_main_specific_stop,
};

int
main (int argc, char **argv)
{
	return grid_main (argc, argv, &main_callbacks);
}
