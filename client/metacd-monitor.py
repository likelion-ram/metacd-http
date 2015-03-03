#!/usr/bin/env python

import sys, logging, httplib

logging.basicConfig(
	format='%(asctime)s %(message)s',
	datefmt='%m/%d/%Y %I:%M:%S',
	level=logging.INFO)

if len(sys.argv) < 2 :
	logging.critical("Missing service identifier : NS|TYPE|IP:PORT")
	sys.exit()
else:
	tokens = sys.argv[1].split('|')
	ns, type, svc = tokens[:3]
	host, port = svc.split(':')
	logging.debug("Contacting [%s] at [%s:%d] NS[%s]", type, host, int(port), ns)

body = None
try:
	cnx = httplib.HTTPConnection(svc)
	cnx.request("GET", "/status")
	resp = cnx.getresponse()
	status, reason, body = resp.status, resp.reason, resp.read()
	cnx.close()
except Exception as e:
	logging.error("transport error : %s", str(e))
	sys.exit()

if status / 100 != 2:
	logging.error("metacd error : %s", reason)
	sys.exit()

for line in str(body).splitlines():
	line = line.strip()
	if len(line) <= 0 or line[0] == '#':
		continue
	tokens = [x.strip() for x in line.split('=')]
	if not tokens or tokens is None or len(tokens) < 2:
		logging.debug("Not enough tokens for [%s]", line)
		continue
	k, v = tokens[:2]
	print k+'='+v

