import requests, json, time
from typing import List, Dict
from confluent_kafka import Producer

binance_url = "https://api.binance.com"
binance_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

local_binance_config = {
	'bootstrap.servers': 'localhost:9092',	'auto.offset.reset': 'earliest'
}

producer = Producer(local_binance_config)

def binance_extract(symbols : List) -> List[Dict]:
	endpoint = f"{binance_url}/api/v3/ticker/24hr"
	params = {"symbols": json.dumps(symbols, separators=(',', ':'))} if symbols else {}
	response = requests.get(endpoint, params=params)
	response.raise_for_status()
	return response.json()

def send_to_kafka(binance_data : List[Dict]):
	kafka_data = json.dumps(binance_data)
	producer.produce('binance_24h', kafka_data.encode('utf-8'))
	producer.flush()
	print('[+] Data sent to kafka')
	
if __name__ == "__main__":
	data = binance_extract(binance_symbols)
	while data:
		send_to_kafka(data)
		time.sleep(1)
