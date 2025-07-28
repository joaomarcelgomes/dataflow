import json
import pandas as pd
from io import BytesIO
from confluent_kafka import Consumer
import boto3
from botocore.client import Config

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
        'group.id': 'transform_parquet_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(['transform_parquet'])
    return consumer

# leitura do arquivo do MinIO
def read_file(s3, bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_json(BytesIO(obj['Body'].read()))

# escrita do parquet anual
def write_parquet_yearly(s3, df):
    df['date'] = pd.to_datetime(df['date'])
    year = df['date'].dt.year[0]
    key = f"trusted/parquet/{year}.parquet"
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket='lake-csv', Key=key, Body=buffer.read(), ContentType='application/octet-stream')
    print(f"parquet salvo em: {key}")

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
        write_parquet_yearly(s3, df)

        consumer.commit()
        print("deu certo!\n")

if __name__ == "__main__":
    main()