import boto3
import pandas as pd
from datetime import datetime
from botocore.client import Config
from io import BytesIO
import pyarrow

key_id = 'minio'
secret_key = 'minio123'
bucket_name = 'lake-csv'
bucket_url = 'http://minio:9000'

def connection_bucket():
    s3 = boto3.client(
        's3',
        endpoint_url=bucket_url,
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

    return s3

def create_bucket_if_not_exists(retries=3):
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
    filename = f"raw/csv/{timestamp}.parquet"

    s3.put_object(
        Bucket=bucket_name,
        Key=filename,
        Body=file.getvalue(),
        ContentType='application/octet-stream'
    )

def main():
    file_csv = pd.read_csv('/data/csv/file.csv')

    buffer = BytesIO()
    file_csv.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = connection_bucket()
    insert_file(s3, buffer)

main()