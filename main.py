import falcon
import json
import happybase
import ConfigParser
from wsgiref import simple_server
from kafka import SimpleProducer
from db import connect_kafka
import time

config = ConfigParser.ConfigParser()
config.read('app.cfg')

connection = happybase.Connection(config.get('default', 'HBASE_HOST'))
kafka = connect_kafka()
producer = SimpleProducer(kafka)

class RootAPI:
	def on_get(self, req, res):
		res.body = json.dumps({ 'message':  'JVN Microservices!' })

class HBaseQuery:
	def on_get(self, req, res):
		connection.open()
		current_table = connection.table('userscore')
		
		candidate_id = req.get_param('candidate_id') or req.get_param('id') or ''
		print '==============', candidate_id
		result = current_table.row(candidate_id)

		if not result:
			request_json = { "candidate_id": candidate_id }
			producer.send_message('wordcounttopic', json.dumps(request_json))
			time.sleep(2)
			result = current_table.row(candidate_id)

		res.body = json.dumps(result)
		
class Tables:
	def on_get(self, req, res):
		tables = connection.tables()
		res.body = json.dumps(tables)


class StaticResource:
	def on_get(self, req, res):
		res.status = falcon.HTTP_200
		res.content_type = 'text/html'
		with open('./views/help.html', 'r') as f:
			res.body = f.read()

api = falcon.API()
api.add_route('/', RootAPI())
api.add_route('/query', HBaseQuery())
api.add_route('/tables', Tables())
api.add_route('/help', StaticResource())

if __name__ == '__main__':
	httpd = simple_server.make_server('0.0.0.0', 8000, api)
	httpd.serve_forever()
	print 'Listening in 0.0.0.0:8000'