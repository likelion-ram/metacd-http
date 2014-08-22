#!/usr/bin/env python

import json, httplib, urlparse

CODE_NOT_FOUND = 400
CODE_NOT_ALLOWED = 403
CODE_NAMESPACE_NOTMANAGED = 418

suite_cs  = [
]

suite_dir = [
]

suite_meta2 = [
	### Invalid URL, wrong method, no json in body, etc
	( { 'method':'GET', 'url':'/m3', 'body':None },
	  { 'status':404, 'body':None }),
	( { 'method':'GET', 'url':'/m2', 'body':None },
	  { 'status':404, 'body':None }),
	( { 'method':'GET', 'url':'/m2/', 'body':None },
	  { 'status':404, 'body':None }),
	( { 'method':'GET', 'url':'/m2/put/', 'body':None },
	  { 'status':405, 'body':None }),
	( { 'method':'POST', 'url':'/m2/put/', 'body':None },
	  { 'status':400, 'body':None }),
	( { 'method':'POST', 'url':'/m2/put/ns/NS/ref/JFS', 'body':None },
	  { 'status':400, 'body':None }),

	( { 'method':'POST', 'url':'/m2/put/ns/NS/ref/JFS', 'body':{ 'beans':{} } },
	  { 'status':200, 'body':{"status":400} }),

	( { 'method':'POST', 'url':'/m2/put/ns/NS1/ref/JFS', 'body':{ 'beans':{} } },
	  { 'status':200, 'body':{"status":CODE_NAMESPACE_NOTMANAGED} }),

	### Regular PUT that should work
	( { 'method':'POST', 'url':'/m2/put/ns/NS/ref/JFS', 'body':{
				"beans" : {
					"aliases" : [
						{ "name":"content", "ver":0, "ctime":1, "header":"00", "system_metadata":"plop=plop"}
					],
					"headers" : [
						{ "id":"00", "hash":"00000000000000000000000000000000", "size":0 }
					],
					"contents" : [
						{ "hdr":"00", "pos":"0", "chunk":"http://127.0.0.1:6014/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000" }
					],
					"chunks" : [
						{ "id":"http://127.0.0.1:6014/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000", "hash":"00000000000000000000000000000000", "size":0 }
					]
				}
			}
		}, {'status':200}),

	### PUT on a rawx not registered.
	( { 'method':'POST', 'url':'/m2/put/ns/NS/ref/JFS', 'body':{
				"beans" : {
					"aliases" : [
						{ "name":"content", "ver":0, "ctime":1, "header":"00", "system_metadata":"plop=plop"}
					],
					"headers" : [
						{ "id":"00", "hash":"00000000000000000000000000000000", "size":0 }
					],
					"contents" : [
						{ "hdr":"00", "pos":"0", "chunk":"http://127.0.0.1:1025/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000" }
					],
					"chunks" : [
						{ "id":"http://127.0.0.1:1025/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000", "hash":"00000000000000000000000000000000", "size":0 }
					]
				}
			}
		}, {'status':200,'body':{'status':200} }),

	### PUT with no chunk linked to the alias
	( { 'method':'POST', 'url':'/m2/put/ns/NS/ref/JFS', 'body':{
				"beans" : {
					"aliases" : [
						{ "name":"content", "ver":0, "ctime":1, "header":"00", "system_metadata":"plop=plop"}
					],
					"headers" : [
						{ "id":"00", "hash":"00000000000000000000000000000000", "size":0 }
					],
					"contents" : [
						{ "hdr":"00", "pos":"0", "chunk":"http://127.0.0.1:6014/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000" }
					],
					"chunks" : [
						{ "id":"http://127.0.0.1:1025/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000", "hash":"00000000000000000000000000000000", "size":0 }
					]
				}
			}
		}, {'status':200,'body':{'status':400} }),

	( { 'method':'POST', 'url':'/m2/append/ns/NS/ref/JFS', 'body':{
				"beans" : {
					"aliases" : [
						{ "name":"content", "ver":0, "ctime":1, "header":"00", "system_metadata":"plop=plop"}
					],
					"headers" : [
						{ "id":"00", "hash":"00000000000000000000000000000000", "size":0 }
					],
					"contents" : [
						{ "hdr":"00", "pos":"0", "chunk":"http://127.0.0.1:6014/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000" }
					],
					"chunks" : [
						{ "id":"http://127.0.0.1:1025/DATA/NS/localhost/rawx-2/0000000000000000000000000000000000000000000000000000000000000000", "hash":"00000000000000000000000000000000", "size":0 }
					]
				}
			}
		}, {'status':200,'body':{'status':400}})
]

def run_test_suite (suite):
	count = 0
	for i, o in suite:
		url = urlparse.urlparse('http://127.0.0.1:1234' + i['url'])
		print ""
		print repr(i) 
		print repr(o) 
		encoded = json.dumps(i['body'])
		cnx = httplib.HTTPConnection(url.netloc)
		cnx.request(i['method'], url.path, encoded)
		resp = cnx.getresponse()
		status, reason, body = resp.status, resp.reason, resp.read()
		cnx.close()
		print '***', status, reason, repr(body)
		decoded = None
		if body is not None and body:
			decoded = json.loads(body)
		if status != o['status']:
			raise Exception('Bad status at {0}, {1} instead of {2}'.format(count, status, o['status']))
		if 'body' in o and o['body'] is not None:
			for k in o['body']:
				if o['body'][k] != decoded[k]:
					raise Exception('Bad body at {0}'.format(count))
		count += 1

if __name__ == '__main__':
	run_test_suite(suite_cs)
	run_test_suite(suite_dir)
	run_test_suite(suite_meta2)

