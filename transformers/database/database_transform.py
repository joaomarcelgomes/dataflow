import logging
import time
import pandas as pd
import json
import boto3
import psycopg2
from io import BytesIO, StringIO
from datetime import datetime
from botocore.client import Config
from confluent_kafka import Consumer
from sqlalchemy import create_engine

KAFKA_BROKER = 'kafka:9092'
TOPIC = 'transform_database'
GROUP_ID = 'transform_database_group'
SOURCE_BUCKET = 'lake-database'
CSV_BUCKET = 'csv-gold'
PARQUET_BUCKET = 'parquet-gold'
CSV_FILE = 'gold.csv'
PARQUET_FILE = 'gold.parquet'
POSTGRES_URL = 'postgresql+psycopg2://postgres:postgres@db:5432/dataflow'

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO
)

def connect_minio():
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def ensure_bucket_exists(s3, name):
    try:
        s3.head_bucket(Bucket=name)
    except s3.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            s3.create_bucket(Bucket=name)
            logging.info(f"Created bucket: {name}")
        else:
            raise

def load_json_from_s3(s3, bucket, filename) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=filename)
    df = pd.read_json(obj['Body'])
    return df

def append_to_csv(s3, df: pd.DataFrame):
    try:
        obj = s3.get_object(Bucket=CSV_BUCKET, Key=CSV_FILE)
        existing = pd.read_csv(obj['Body'])
    except s3.exceptions.NoSuchKey:
        existing = pd.DataFrame()

    final_df = pd.concat([existing, df], ignore_index=True)
    buffer = StringIO()
    final_df.to_csv(buffer, index=False)
    s3.put_object(Bucket=CSV_BUCKET, Key=CSV_FILE, Body=buffer.getvalue(), ContentType='text/csv')
    logging.info("Appended data to gold.csv")

def insert_db_forecast_into_postgres(conn, records):
    cur = conn.cursor()
    insert = """
        INSERT INTO weather_forecast_raw (
            full_date, weekday, temp_range, humidity, cloudiness,
            rain, rain_probability, wind_speedy, description, condition
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for r in records:
        full_date = datetime.utcfromtimestamp(r["date"] / 1000).date()
        temp_range = f"{r['min_temp']}-{r['max_temp']}"
        cur.execute(insert, (
            full_date,
            r["weekday"],
            temp_range,
            r["humidity"],
            r["cloudiness"],
            r["rain"],
            r["rain_probability"],
            r["wind_speedy"],
            r["description"],
            r["condition"]
        ))
    conn.commit()
    cur.close()

def process_message(s3, filename: str, pg_conn):
    logging.info(f"Processing file: {filename}")

    obj = s3.get_object(Bucket=SOURCE_BUCKET, Key=filename)
    records = json.load(obj['Body'])

    if not records:
        logging.warning("Empty dataset, skipping.")
        return

    df = pd.DataFrame(records)

    append_to_csv(s3, df)
    insert_db_forecast_into_postgres(pg_conn, records)


def main():
    logging.info("Starting API transform process")
    s3 = connect_minio()
    for bucket in [CSV_BUCKET, PARQUET_BUCKET]:
        ensure_bucket_exists(s3, bucket)

    pg_engine = create_engine(POSTGRES_URL)
    pg_conn = pg_engine.raw_connection()

    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    consumer.subscribe([TOPIC])
    logging.info(f"Subscribed to Kafka topic '{TOPIC}'")

    timeout = time.time() + 10
    while time.time() < timeout:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Kafka error: {msg.error()}")
            continue

        try:
            filename = msg.value().decode('utf-8')
            process_message(s3, filename, pg_conn)
            consumer.commit()
        except Exception as e:
            logging.exception(f"Failed to process message: {e}")

    consumer.close()
    pg_conn.close()
    logging.info("Kafka consumer closed")

if __name__ == '__main__':
    main()
