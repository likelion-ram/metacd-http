
import json, httplib, urlparse

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
	
class NsResource(AbstractResource):
	def __init__ (self, metacd, ns):
		self.metacd = metacd
		self.ns = ns
	def _url (self):
		return "/cs/info/ns/"+str(self.ns)
	def GET (self):
		return self.metacd._query("GET", self._url())
	def HEAD (self):
		return self.metacd._query("HEAD", self._url())

class NsService(AbstractResource):
	def __init__ (self, metacd, ns, srvtype):
		self.metacd = metacd
		self.ns = ns
		self.srvtype = srvtype
	def _url (self):
		return "/cs/srv/ns/"+str(self.ns)+"/type/"+str(self.srvtype)
	def PUT (self, srv):
		return self.metacd._query("PUT", self._url(), json.dumps(srv))
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
		url = self._url() + 'action/'+str(action)
		return self.metacd._query("HEAD", url, json.dumps(srv))

class Conscience(object):
	def __init__(self,metacd,ns):
		self.metacd = metacd
		self.ns = ns
	def info(self):
		"""Conscience configuration"""
		return NsResource(self.metacd, self.ns)
	def services(self, srvtype):
		"""Conscience content"""
		return NsService(self.metacd, self.ns, srvtype)

class Directory(object):
	def __init__(self,metacd,ns):
		self.metacd = metacd
		self.ns = ns
	def dir_service(self, ns, srvtype):
		"""Manage directory services"""
		return DirectoryServicePool(self, ns, srvtype)

class Metacd:
	def __init__(self, url):
		self.url = url
	def _query(self, method, path, body=None, headers={}):
		"""Factorize a request to the metacd"""
		print "->", method, path
		cnx = httplib.HTTPConnection(self.url)
		cnx.request(method, path, json.dumps(body), headers)
		resp = cnx.getresponse()
		status, reason, body = resp.status, resp.reason, resp.read()
		cnx.close()
		if status / 100 > 2:
			raise Exception("Service error: "+str(reason))
		if not body:
			return None
		return json.loads(body)
	def conscience (self,ns):
		return Conscience(self,ns)
	def directory (self,ns):
		return Directory(self,ns)

def main ():
	mcd = Metacd("127.0.0.1:5000")
	print mcd.conscience("NS").info().GET()
	print mcd.conscience("NS").info().HEAD()
	print mcd.conscience("NS").services("meta2").GET()
	print mcd.conscience("NS").services("meta2").HEAD()

if __name__ == '__main__':
	main()

