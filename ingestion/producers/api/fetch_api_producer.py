import requests
from confluent_kafka import Producer
import json
import logging

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO
)

def fetch_weather():
    params = {"city_name": "São Paulo"}
    response = requests.get('https://api.hgbrasil.com/weather', params=params)
    response.raise_for_status()
    return response.json()

def publish_to_kafka(producer, data):
    producer.produce('ingestion_api', value=json.dumps(data).encode('utf-8'))
    producer.flush()

def main():
    logging.info("Starting weather API producer")
    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    logging.info("Connected to Kafka")

    logging.info("Fetching weather data")
    data = fetch_weather()
    logging.info("Fetched weather data")

    logging.info("Publishing to Kafka topic 'ingestion_api'")
    publish_to_kafka(producer, data)
    logging.info("Published to Kafka")

if __name__ == '__main__':
    main()
