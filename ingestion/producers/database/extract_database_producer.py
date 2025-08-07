import logging
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from confluent_kafka import Producer

KAFKA_BROKER = 'kafka:9092'
TARGET_TOPIC = 'ingestion_database'
DB_URL = 'postgresql+psycopg2://postgres:postgres@db:5432/dataflow'
TABLE = 'weather_forecast'

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO
)

def connection_database():
    return create_engine(DB_URL)

def publish_to_kafka(producer: Producer, payload: str) -> None:
    producer.produce(TARGET_TOPIC, value=payload.encode('utf-8'))
    producer.flush(timeout=5)
    logging.info("Published data to Kafka")

def main() -> None:
    logging.info("Starting Database Producer")

    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    engine = connection_database()

    logging.info(f"Querying table: {TABLE}")
    df = pd.read_sql_query(f'SELECT * FROM {TABLE}', engine)
    engine.dispose()

    buffer = StringIO()
    df.to_json(buffer, orient='records')
    buffer.seek(0)

    publish_to_kafka(producer, buffer.getvalue())

if __name__ == '__main__':
    main()