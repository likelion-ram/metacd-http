
import json, httplib, urlparse, logging

def validate_rec(path,obj,schema):
	"""Check the mandatory fields are present and with the same type than
	in the schema, but does not check for exceeding fields."""
	print path, "obj", repr(obj), "schema", repr(schema)
	def succ(k):
		return '/'.join((path,k))
	if isinstance(schema,dict):
		for k,v in schema.items():
			if k:
				if k not in obj:
					raise Exception("Missing key "+path+'/'+k)
				ov = obj[k]
				validate_rec(succ(k), obj[k], v)
			else:
				for k,ov in obj.items():
					validate_rec(succ(k), ov, v)
	elif isinstance(schema,list) or isinstance(schema,tuple):
		if not isinstance(obj,list) and not isinstance(obj,tuple):
			raise Exception("Unexpected type at "+path)
		if len(schema) <= 0:
			if len(obj) != 0:
				raise Exception("Array not empty at "+path)
		count = 0
		for item in obj:
			validate_rec(succ(str(count)), item, schema[0])
			count = count + 1
	elif type(obj) != type(schema):
		raise Exception("Unexpected type at "+path)

def validate(obj,schema):
	validate_rec('', obj, schema)

schema_tag = {"":""}
schema_srv = {"ns":"", "type":"", "addr":"", "score":0, "tags":[0]}
schema_srvset = [ schema_srv ]
schema_beanset = {"beans":{
	"aliases":[{"name":"","ver":0,"ctime":0,"header":"","system_metadata":""}]
	"headers":[{"id":"", "hash":"", "size":0}]
	"contents":[{"hdr":"", "pos":"", "chunk":""}]
	"chunks":[{ "id":"", "hash":"", "size":0 }]
}}

class AbstractResource(object):
	def GET(self):
		raise Exception("Not implemented")
	def HEAD(self):
		raise Exception("Not implemented")
	def PUT(self):
		raise Exception("Not implemented")
	def POST(self):
		raise Exception("Not implemented")
	def DELETE(self):
		raise Exception("Not implemented")

def Url(ns, ref=None, path=None):
	u = dict()
	assert(ns is not None)
	nstoken = ns.split('.',2)
	u['ns'] = str(ns)
	u['pns'] = nstoken[0]
	u['vns'] = '.'.join(nstoken[1:])
	u['ref'] = ref
	u['path'] = path
	return u

class NsInfo(AbstractResource):
	def __init__ (self, metacd):
		self.metacd = metacd
	def _url (self):
		return '/'.join((
			"/cs/info",
			"ns", str(self.metacd.ns()),
		))
	def GET (self):
		return self.metacd._query("GET", self._url())
	def HEAD (self):
		return self.metacd._query("HEAD", self._url())

class NsService(AbstractResource):
	def __init__ (self, metacd, srvtype):
		self.metacd = metacd
		self.srvtype = srvtype
	def _url (self):
		return '/'.join((
			"/cs/srv",
			"ns", str(self.metacd.ns()),
			"type", str(self.srvtype),
		))
	def DELETE (self):
		return self.metacd._query("DELETE", self._url())
	def PUT (self, srv):
		validate(srv,schema_srv)
		body = json.dumps(srv)
		return self.metacd._query("PUT", self._url(), body=body)
	def GET (self):
		return self.metacd._query("GET", self._url())
	def HEAD (self):
		return self.metacd._query("HEAD", self._url())
	def POST (self, action=None, srv=None):
		if action is None:
			raise Exception("Invalid action")
		if srv is None or not srv:
			raise Exception("Missing services")
		# TODO sanitize the services set.
		url = self._url() + '/action/'+str(action)
		return self.metacd._query("HEAD", url, json.dumps(srv))

class Conscience(object):
	def __init__(self, metacd):
		self.metacd = metacd
	def info(self):
		"""Conscience configuration"""
		return NsInfo(self.metacd)
	def services(self, srvtype):
		"""Conscience content"""
		return NsService(self.metacd, srvtype)


class DirectoryReferencePool(AbstractResource):
	def __init__(self, metacd, url):
		self.metacd = metacd
		self.url = url
	def _url (self):
		return '/'.join(("/dir/ref",
			"ns", self.url['ns'],
			"ref", self.url['ref']))
	def GET (self):
		return self.metacd._query("GET", self._url())
	def HEAD (self):
		return self.metacd._query("HEAD", self._url())
	def PUT (self):
		return self.metacd._query("PUT", self._url())
	def DELETE (self):
		return self.metacd._query("DELETE", self._url())

class DirectoryServicePool(AbstractResource):
	def __init__(self, metacd, url, srvtype):
		self.metacd = metacd
		self.url = url
		self.srvtype = srvtype
	def _url (self):
		return '/'.join(("/dir/srv",
			"ns", self.url['ns'],
			"ref", self.url['ref'],
			'type', self.srvtype,))
	def GET (self):
		return self.metacd._query("GET", self._url())
	def HEAD (self):
		return self.metacd._query("HEAD", self._url())
	def POST (self, action=None):
		if action is None:
			raise Exception("Invalid action")
		url = self._url() + '/action/'+str(action)
		return self.metacd._query("POST", url)
	def DELETE (self):
		return self.metacd._query("DELETE", self._url())

class Directory(object):
	def __init__(self,metacd):
		self.metacd = metacd
	def references(self, url):
		"""Manage references registrations"""
		return DirectoryReferencePool(self.metacd, url)
	def services(self, url, srvtype):
		"""Manage directory services"""
		return DirectoryServicePool(self.metacd, url, srvtype)


class Meta2Containers(AbstractResource):
	def __init__(self, metacd, url):
		self.metacd = metacd
		self.url = url
	def _url (self):
		return '/'.join(('/m2/container',
			'ns', self.url['ns'],
			'ref', self.url['ref'],
		))
	def HEAD (self):
		return self.metacd._query('HEAD', self._url())
	def GET (self):
		return self.metacd._query('GET', self._url())
	def PUT (self):
		return self.metacd._query('PUT', self._url())
	def DELETE (self):
		return self.metacd._query('DELETE', self._url())

class Meta2Contents(AbstractResource):
	def __init__(self, metacd, url):
		self.metacd = metacd
		self.url = url
	def _url (self):
		return '/'.join(('/m2/contents',
			'ns', self.url['ns'],
			'ref', self.url['ref'],
			'path', self.url['path'],
		))
	def HEAD (self):
		return self.metacd._query('HEAD', self._url())
	def GET (self):
		return self.metacd._query('GET', self._url())
	def PUT (self, beans):
		validate(beans, schema_beans)
		return self.metacd._query('PUT', self._url(), json.dumps(beans))
	def DELETE (self):
		return self.metacd._query('DELETE', self._url())

class Meta2Properties(AbstractResource):
	def __init__(self, metacd, url):
		self.metacd = metacd
		self.url = url
	def _url (self):
		if 'path' in self.url:
			return '/'.join(('/m2/container/props',
				'ns', self.url['ns'],
				'ref', self.url['ref'],
				'path', self.url['path'],
			))
		return '/'.join(('/m2/container/props',
			'ns', self.url['ns'],
			'ref', self.url['ref'],
		))
	def GET (self, keys=None):
		body = None
		if keys is not None:
			validate(keys,[""])
			body = json.dumps(keys)
		return self.metacd._query('GET', self._url(), body=body)
	def DELETE (self, keys=None):
		body = None
		if keys is not None:
			validate(keys,[""])
			body = json.dumps(keys)
		return self.metacd._query('DELETE', self._url(), body=body)
	def PUT (self, pairs):
		validate(pairs, {"":""})
		body = json.dumps(pairs)
		return self.metacd._query('PUT', self._url(), body=body)

class Meta2(object):
	def __init__(self, metacd):
		self.metacd = metacd
	def containers(self, url):
		validate(url,{"ns":"","ref":""})
		return Meta2Containers (self.metacd, url)
	def contents(self, url):
		validate(url,{"ns":"","ref":"","path":""})
		return Meta2Contents (self.metacd, url)
	def properties(self, url):
		if 'path' in url:
			validate(url,{"ns":"","ref":"",'path':''})
		else:
			validate(url,{"ns":"","ref":""})
		return Meta2Properties (self.metacd, url)


class Metacd:
	def __init__(self, ns, url=None):
		self._ns = ns
		self._url = url
	def _query(self, method, path, body=None, headers={}):
		"""Factorize a request to the metacd"""
		print "->", method, path
		cnx = httplib.HTTPConnection(self._url)
		cnx.request(method, path, body, headers)
		resp = cnx.getresponse()
		status, reason, body = resp.status, resp.reason, resp.read()
		cnx.close()
		if status / 100 > 2:
			logging.warn("%s", body)
			raise Exception("Service error: "+str(reason))
		if not body:
			return None
		return json.loads(body)
	def ns (self):
		return self._ns

	def conscience (self):
		return Conscience(self)
	def directory (self):
		return Directory(self)
	def meta2 (self):
		return Meta2(self)

def main ():
	mcd = Metacd("NS", url="127.0.0.1:5000")
	conscience = mcd.conscience()
	directory = mcd.directory()
	meta2 = mcd.meta2()
	url = Url(ns="NS", ref="JFS")

	print conscience.info().HEAD()
	print conscience.info().GET()

	print conscience.services("meta2").HEAD()
	print conscience.services("meta2").GET()
	print conscience.services("meta2").PUT({
		"ns" : "NS", "type" : "meta2", "addr" : "127.0.0.1:7000",
		"score" : 1, "tags" : [] })
	print conscience.services("meta2").DELETE()

	print directory.references(url).PUT()
	print directory.references(url).GET()
	print directory.references(url).HEAD()
	print directory.services(url, "meta2").HEAD()
	print directory.services(url, "meta2").GET()
	print directory.references(url).DELETE()

	print directory.references(url).PUT()
	print directory.services(url, "meta2").POST(action="link")
	print meta2.containers(url).PUT()
	print meta2.containers(url).HEAD()
	print meta2.containers(url).GET()
	print meta2.containers(url).DELETE()
	print directory.services(url, "meta2").DELETE()
	print directory.references(url).DELETE()

if __name__ == '__main__':
	main()

