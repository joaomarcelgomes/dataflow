import json
import pandas as pd
from io import BytesIO
from confluent_kafka import Consumer
import boto3
from botocore.client import Config
from sqlalchemy import create_engine

#criando client minio
def create_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

#criando consumidor kafka
def create_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'transform_bd_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['transform_database'])

# leitura do arquivo do MinIO
def read_file(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_json(BytesIO(obj['Body'].read()))

# Inserção no PostgreSQL
def write_to_postgres(df):
    engine = create_engine('postgresql+psycopg2://postgres:postgres@db:5432/dataflow')
    for _, row in df.iterrows():
        row.to_frame().T.to_sql('weather_processed', con=engine, if_exists='append', index=False)
    engine.dispose()
    print("dados inseridos no banco de dados")

def main():
    consumer = create_consumer()
    s3 = create_s3_client()

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        file_key = json.loads(msg.value().decode('utf-8'))['file']
        print(f"processando arquivo: {file_key}")

        df = read_file(s3, 'lake-csv', file_key)
        write_to_postgres(df)

        consumer.commit()
        print("deu certo!\n")

if __name__ == "__main__":
    main()