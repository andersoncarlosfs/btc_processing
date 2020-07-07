import json
import time
import urllib.request

from kafka import KafkaProducer

url = "http://api.coindesk.com/v1/bpi/currentprice.json"

producer = KafkaProducer(bootstrap_servers=["localhost:9092", "localhost:9093"])
while True:
    response = urllib.request.urlopen(url)
    rates = json.loads(response.read().decode())
    producer.send("rates", json.dumps(rates).encode())
    print(rates)
    time.sleep(1)
