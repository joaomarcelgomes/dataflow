import requests
from confluent_kafka import Producer
import json

def fetch_weather():
    params = {"city_name": "São Paulo"}
    response = requests.get('https://api.hgbrasil.com/weather', params=params)
    response.raise_for_status()
    return response.json()

def publish_to_kafka(producer, data):
    producer.produce('ingestion_api', value=json.dumps(data).encode('utf-8'))
    producer.flush()

def main():
    producer = Producer({'bootstrap.servers': 'kafka:9092'})

    data = fetch_weather()
    publish_to_kafka(producer, data)

main()