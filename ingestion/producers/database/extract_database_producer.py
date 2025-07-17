import pandas as pd
from io import StringIO
import psycopg2
from sqlalchemy import create_engine
from confluent_kafka import Producer
import json

def connection_database():
    engine = create_engine('postgresql+psycopg2://postgres:postgres@db:5432/dataflow')
    return engine

def publish_to_kafka(producer, data):
    producer.produce('ingestion_database', value=json.dumps(data).encode('utf-8'))
    producer.flush()

def main():
    producer = Producer({'bootstrap.servers': 'kafka:9092'})

    engine = connection_database()

    df = pd.read_sql_query('SELECT * FROM weather_forecast', engine)
    engine.dispose()

    buffer = StringIO()
    df.to_json(buffer, orient='records')
    buffer.seek(0)

    publish_to_kafka(producer, buffer)

main()