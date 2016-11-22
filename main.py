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
#connection.open()

kafka = connect_kafka()
# producer = SimpleProducer(kafka)

current_table = connection.table('userscore')

class RootAPI:
	def on_get(self, req, res):
		res.body = json.dumps({ 'message':  'JVN Microservices!' })

class HBaseQuery:
	def on_get(self, req, res):
		global connection, current_table

		# connection = happybase.Connection(config.get('default', 'HBASE_HOST'))
		connection.open()
		current_table = connection.table('userscore')
		
		req_data = {
			'pclass': req.get_param('pclass'),
			'age': req.get_param('age'),
			'sex': req.get_param('sex'),
			'fare': req.get_param('fare')
		}

		#score = req.get_param('score') or 0
		#print '==============', candidate_id
		result = current_table.row(req_data['fare'])

		if not result:
			#request_json = { "name": candidate_id, "score": score }
			producer = SimpleProducer(kafka)
			producer.send_messages('userscore', json.dumps(req_data))
			

			n = 3
			while n >= 0:
				result = current_table.row(req_data['fare'])
				if not result:
					time.sleep(1)
					n -= 1
				else:
					break

		connection.close()
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
