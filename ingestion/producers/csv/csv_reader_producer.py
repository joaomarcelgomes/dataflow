#ler o arquivo file.csv
import pandas as pd
from confluent_kafka import Producer
import json

def publish_to_kafka(producer, data):
    producer.produce('ingestion_csv', value=json.dumps(data).encode('utf-8'))
    producer.flush()

def main():
    producer = Producer({'bootstrap.servers': 'kafka:9092'})

    data = pd.read_csv('/data/csv/file.csv').to_json(orient='records')

    publish_to_kafka(producer, data)

main()