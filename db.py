from kafka import KafkaClient, SimpleProducer, SimpleConsumer
import ConfigParser

config = ConfigParser.ConfigParser()
config.read('app.cfg')

def connect_kafka():
	kafka_host = config.get('default', 'KAFKA_HOST')
	kafka = KafkaClient(kafka_host)
	return kafka