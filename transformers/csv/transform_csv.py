import json
import pandas as pd
from io import BytesIO
from confluent_kafka import Consumer
import boto3
from botocore.client import Config
from sqlalchemy import create_engine

# criando cliente S3 (MinIO)
def create_s3_client():
    return boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minio',
        aws_secret_access_key='minio123',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

# construindo consumidor Kafka
def create_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'transform_csv_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['transform_csv'])
    return consumer

# leitura do arquivo do MinIO
def read_file(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_json(BytesIO(obj['Body'].read()))

# escrida do CSV mensal
def write_csv_monthly(s3, df):
    df['date'] = pd.to_datetime(df['date'])
    month_str = df['date'].dt.to_period('M')[0].strftime('%Y-%m')
    key = f"trusted/csv/{month_str}.csv"
    csv_data = df.to_csv(index=False).encode('utf-8')
    s3.put_object(Bucket='lake-csv', Key=key, Body=csv_data, ContentType='text/csv')
    print(f"csv salvo em: {key}")

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
        write_csv_monthly(s3, df)

        consumer.commit()
        print("tranformação do csv concluida.\n")

if __name__ == "__main__":
    main()