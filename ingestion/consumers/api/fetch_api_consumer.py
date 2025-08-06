import json
import logging
import time
from datetime import datetime
from typing import Optional

import boto3
from botocore.client import Config
from confluent_kafka import Consumer, Producer, KafkaError, Message

# Config
KEY_ID = 'minio'
SECRET_KEY = 'minio123'
BUCKET_NAME = 'lake-api'
BUCKET_URL = 'http://minio:9000'
KAFKA_BROKER = 'kafka:9092'
SOURCE_TOPIC = 'ingestion_api'
TARGET_TOPIC = 'transform_api'
GROUP_ID = 'api_group'

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO
)

def get_consumer() -> Consumer:
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe([SOURCE_TOPIC])
    return consumer

def get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=BUCKET_URL,
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def create_bucket_if_not_exists():
    s3_res = boto3.resource(
        's3',
        endpoint_url=BUCKET_URL,
        aws_access_key_id=KEY_ID,
        aws_secret_access_key=SECRET_KEY
    )
    bucket = s3_res.Bucket(BUCKET_NAME)
    if bucket.creation_date is None:
        bucket.create()
        logging.info(f"Bucket '{BUCKET_NAME}' created")

def insert_file(s3, file_data: str) -> str:
    create_bucket_if_not_exists()
    timestamp = datetime.now().isoformat(timespec='seconds')
    filename = f"raw/json/{timestamp}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=file_data,
        ContentType='application/json'
    )
    logging.info(f"Inserted file into bucket: {filename}")
    return filename

def publish_to_kafka(producer: Producer, filename: str) -> None:
    producer.produce(TARGET_TOPIC, value=filename.encode('utf-8'))
    producer.flush(timeout=5)
    logging.info(f"Published filename to topic '{TARGET_TOPIC}'")

def consume_and_process(consumer: Consumer, producer: Producer, s3) -> None:
    timeout = time.time() + 10
    while time.time() < timeout:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Kafka error: {msg.error()}")
            continue

        try:
            value = msg.value().decode('utf-8')
            logging.info("Consumed message from Kafka")

            filename = insert_file(s3, value)
            consumer.commit()
            logging.info("Offset committed")

            publish_to_kafka(producer, filename)

        except Exception as e:
            logging.exception(f"Processing failed: {e}")


def main() -> None:
    logging.info("Starting API consumer")
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    consumer = get_consumer()
    s3 = get_s3_client()

    consume_and_process(consumer, producer, s3)

if __name__ == '__main__':
    main()
