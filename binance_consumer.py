from confluent_kafka import Consumer, KafkaException
from json import loads
from pymongo import MongoClient

conf = {
	'bootstrap.servers': 'localhost:9092',
	'group.id': 'binance-24h-id',
	'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['binance_24h'])

client = MongoClient('mongodb://localhost:27017/')
db = client['binance_data']
collection = db['binance_24h']

try:
	while True:
		msg = consumer.poll(1.0)
		if msg is None:
			continue
		if msg.error():
			raise KafkaException(msg.error())
		try:
			data = loads(msg.value().decode('utf-8'))
			collection.insert_many(data)
			print(f'[+] {len(data)} Data inserted to MongoDB')
		except Exception as e:
			print(f"Error processing message: {e}")
except KeyboardInterrupt:
	pass
finally:
	consumer.close()
	client.close()