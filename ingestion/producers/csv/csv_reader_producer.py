import os
import json
import logging
import pandas as pd
from confluent_kafka import Producer

KAFKA_BROKER = 'kafka:9092'
TARGET_TOPIC = 'ingestion_csv'
CSV_DIR = '/data/csv'

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO
)

def publish_to_kafka(producer: Producer, data: dict) -> None:
    producer.produce(TARGET_TOPIC, value=json.dumps(data).encode('utf-8'))
    producer.flush(timeout=5)
    logging.info("Published row to Kafka")

def main() -> None:
    logging.info("Starting CSV Producer")
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})

    for filename in os.listdir(CSV_DIR):
        if filename.endswith(".csv"):
            path = os.path.join(CSV_DIR, filename)
            logging.info(f"Processing file: {filename}")
            df = pd.read_csv(path)
            for _, row in df.iterrows():
                publish_to_kafka(producer, row.to_dict())

if __name__ == '__main__':
    main()
