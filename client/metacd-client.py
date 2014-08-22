
class ServiceResource(object):
	def __init__(self, metacd, ns):
		self.metacd = metacd
		self.ns = ns
	def POST(self):
		pass
	def PUT(self):
		pass
	def GET(self):
		pass
	def DELETE(self):
		pass

class Metacd:
	def __init__(self,url):
		self.url = url
	def srv(self, ns):
		return ServiceResource(self, ns)
