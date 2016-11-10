from flask import Flask 
from flask_restful import Resource, Api

app = Flask(__name__)
api = Api(app)

class RootAPI(Resource):
	def get(self):
		return { 'message':  'JVN Microservices!'}

api.add_resource(RootAPI, '/')

if __name__ == '__main__':
	app.run(host='0.0.0.0')