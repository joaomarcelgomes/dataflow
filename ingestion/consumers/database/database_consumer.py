import boto3
import json
from datetime import datetime
from botocore.client import Config
from confluent_kafka import Consumer

key_id = 'minio'
secret_key = 'minio123'
bucket_name = 'lake-database'
bucket_url = 'http://minio:9000'

def get_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'database_group',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['ingestion_database'])

    return consumer

def get_s3_client():
    s3 = boto3.client(
        's3',
        endpoint_url=bucket_url,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    return s3

def create_bucket_if_not_exists():
    s3_res = boto3.resource(
        's3',
        endpoint_url=bucket_url,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key
    )

    bucket = s3_res.Bucket(bucket_name)

    if bucket.creation_date is None:
        bucket.create()

def insert_file(s3, file):
    create_bucket_if_not_exists()

    timestamp = datetime.now().isoformat(timespec='seconds')
    filename = f"raw/database/{timestamp}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=file,
        ContentType='application/json'
    )

def main():
    consumer = get_consumer()
    s3 = get_s3_client()

    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue

        data = message.value().decode('utf-8')
        insert_file(s3, data)
        consumer.commit()

main()