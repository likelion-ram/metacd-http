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
# define G_LOG_DOMAIN "metacd.http"
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
#include <server/network_server.h>
#include <server/transport_http.h>
#include <resolver/hc_resolver.h>
#include <meta2v2/meta2v2_remote.h>
#include <meta2v2/meta2_utils.h>
#include <meta2v2/autogen.h>
#include <meta2v2/generic.h>
#include <meta2/remote/meta2_services_remote.h>

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

static struct http_request_dispatcher_s *dispatcher = NULL;
static struct network_server_s *server = NULL;

static gchar *nsname = NULL;
static struct hc_resolver_s *resolver = NULL;
static struct grid_lbpool_s *lbpool = NULL;

static struct grid_task_queue_s *admin_gtq = NULL;
static GThread *admin_thread = NULL;

// Configuration
static gint lb_refresh_delay = 10;
#define METACD_LB_ENABLED (lb_refresh_delay >= 0)

static guint dir_low_ttl =  RESOLVD_DEFAULT_TTL_SERVICES;
static guint dir_low_max =  RESOLVD_DEFAULT_MAX_SERVICES;
static guint dir_high_ttl = RESOLVD_DEFAULT_TTL_CSM0;
static guint dir_high_max = RESOLVD_DEFAULT_MAX_CSM0;

#include "reply.c"
#include "url.c"
#include "json_input.c"
#include "json_output.c"

#include "dir_actions.c"
#include "lb_actions.c"
#include "cs_actions.c"
#include "m2_actions.c"

// Misc. handlers --------------------------------------------------------------

struct action_s
{
	const gchar *prefix;
	enum http_rc_e (*hook) (struct http_request_s *rq,
			struct http_reply_ctx_s *rp, const gchar *uri);
} actions[] = {
	{"lb/sl/",               action_lb_sl},

	{"m2/",                  action_meta2},

	{"dir/list/",            action_dir_list},
	{"dir/link/",            action_dir_link},
	{"dir/unlink/",          action_dir_unlink},
	{"dir/status/",          action_dir_status},
	{"dir/flush/high/",      action_dir_flush_high},
	{"dir/flush/low/",       action_dir_flush_low},
	{"dir/set/ttl/high/",    action_dir_set_ttl_high},
	{"dir/set/ttl/low/",     action_dir_set_ttl_low},
	{"dir/set/max/high/",    action_dir_set_max_high},
	{"dir/set/max/low/",     action_dir_set_max_low},

	{"cs/list/",             action_cs_list},
	{"cs/reg/",              action_cs_reg},
	{"cs/lock/",             action_cs_lock},
	{"cs/unlock/",           action_cs_unlock},
	{"cs/clear/",            action_cs_clear},

	{NULL,NULL}
};

static enum http_rc_e
handler_action(gpointer u, struct http_request_s *rq, struct http_reply_ctx_s *rp)
{
	(void) u;
	for (struct action_s *pa = actions; pa->prefix ;++pa) {
		if (g_str_has_prefix(rq->req_uri + 1, pa->prefix))
			return pa->hook(rq, rp, rq->req_uri + 1 + strlen(pa->prefix));
	}
	return _reply_no_handler(rp);
}


// Administrative tasks --------------------------------------------------------

static void
_task_expire_resolver(struct hc_resolver_s *r)
{
	hc_resolver_set_now(r, time(0));
	guint count = hc_resolver_expire(r);
	if (count)
		GRID_DEBUG("Expired %u resolver entries", count);
	count = hc_resolver_purge(r);
	if (count)
		GRID_DEBUG("Purged %u resolver ", count);
}

static void
_task_reload_lbpool(struct grid_lbpool_s *p)
{
	GError *err;

	if (NULL != (err = gridcluster_reconfigure_lbpool(p))) {
		GRID_NOTICE("LBPOOL : reconfigure error : (%d) %s", err->code, err->message);
		g_clear_error(&err);
	}

	if (NULL != (err = gridcluster_reload_lbpool(p))) {
		GRID_NOTICE("LBPOOL : reload error : (%d) %s", err->code, err->message);
		g_clear_error(&err);
	}
}

// MAIN callbacks --------------------------------------------------------------

static void
_main_error(GError *err)
{
	GRID_ERROR("Action failure : (%d) %s", err->code, err->message);
	g_clear_error(&err);
	grid_main_set_status(1);
}

static void
grid_main_action(void)
{
	GError *err = NULL;

	if (NULL != (err = network_server_open_servers(server))) {
		_main_error(err);
		return;
	}

	if (!(admin_thread = grid_task_queue_run(admin_gtq, &err))) {
		g_prefix_error(&err, "Admin thread startup failure: ");
		_main_error(err);
		return;
	}

	if (NULL != (err = network_server_run(server))) {
		_main_error(err);
		return;
	}
}

static struct grid_main_option_s *
grid_main_get_options(void)
{
	static struct grid_main_option_s options[] = {
		{"LbRefresh", OT_INT, {.i=&lb_refresh_delay},
			"Interval between load-balancer service refreshes (seconds)\n"
				"\t\t-1 to disable, 0 to never refresh"},
		{"DirLowTtl", OT_UINT, {.u=&dir_low_ttl},
			"Directory 'low' (meta1) TTL for cache elements"},
		{"DirLowMax", OT_UINT, {.u=&dir_low_max},
			"Directory 'low' (meta1) MAX cached elements"},
		{"DirHighTtl", OT_UINT, {.u=&dir_high_ttl},
			"Directory 'high' (cs+meta0) TTL for cache elements"},
		{"DirHighMax", OT_UINT, {.u=&dir_high_max},
			"Directory 'high' (cs+meta0) MAX cached elements"},
		{NULL, 0, {.i=0}, NULL}
	};

	return options;
}

static void
grid_main_set_defaults(void)
{
}

static void
grid_main_specific_fini(void)
{
	if (admin_thread) {
		grid_task_queue_stop(admin_gtq);
		g_thread_join(admin_thread);
		admin_thread = NULL;
	}
	if (admin_gtq) {
		grid_task_queue_destroy(admin_gtq);
		admin_gtq = NULL;
	}
	if (server) {
		network_server_close_servers(server);
		network_server_stop(server);
		network_server_clean(server);
		server = NULL;
	}
	if (dispatcher) {
		http_request_dispatcher_clean(dispatcher);
		dispatcher = NULL;
	}
	if (lbpool) {
		grid_lbpool_destroy(lbpool);
		lbpool = NULL;
	}
	if (resolver) {
		hc_resolver_destroy(resolver);
		resolver = NULL;
	}
}

static gboolean
grid_main_configure(int argc, char **argv)
{
	static struct http_request_descr_s all_requests[] = {
		{ "action", handler_action },
		{ NULL, NULL }
	};

	if (argc != 2) {
		GRID_ERROR("Invalid parameter, expected : IP:PORT NS");
		return FALSE;
	}

	nsname = g_strdup(argv[1]);
	dispatcher = transport_http_build_dispatcher(NULL, all_requests);
	server = network_server_init();
	resolver = hc_resolver_create();
	lbpool = grid_lbpool_create(nsname);

	if (resolver) {
		hc_resolver_set_ttl_csm0(resolver, dir_high_ttl);
		hc_resolver_set_max_csm0(resolver, dir_high_max);
		hc_resolver_set_ttl_services(resolver, dir_low_ttl);
		hc_resolver_set_max_services(resolver, dir_low_max);
		GRID_INFO("RESOLVER limits HIGH[%u/%u] LOW[%u/%u]",
				dir_high_max, dir_high_ttl, dir_low_max, dir_low_ttl);
	}

	admin_gtq = grid_task_queue_create("admin");
	grid_task_queue_register(admin_gtq, 1,
			(GDestroyNotify)_task_expire_resolver, NULL, resolver);
	if (METACD_LB_ENABLED)
		grid_task_queue_register(admin_gtq, (guint)lb_refresh_delay,
				(GDestroyNotify)_task_reload_lbpool, NULL, lbpool);
	grid_task_queue_fire(admin_gtq);

	network_server_bind_host_lowlatency(server, argv[0],
			dispatcher, transport_http_factory);

	return TRUE;
}

static const char *
grid_main_get_usage(void)
{
	return "IP:PORT NS";
}

static void
grid_main_specific_stop(void)
{
	if (admin_gtq)
		grid_task_queue_stop(admin_gtq);
	if (server)
		network_server_stop(server);
}

static struct grid_main_callbacks main_callbacks =
{
	.options = grid_main_get_options,
	.action = grid_main_action,
	.set_defaults = grid_main_set_defaults,
	.specific_fini = grid_main_specific_fini,
	.configure = grid_main_configure,
	.usage = grid_main_get_usage,
	.specific_stop = grid_main_specific_stop,
};

int
main(int argc, char ** argv)
{
	return grid_main(argc, argv, &main_callbacks);
}

