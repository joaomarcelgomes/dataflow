from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from pathlib import Path

default_args = {
    'start_date': datetime.utcnow() - timedelta(minutes=1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=10)
}

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_DIR = str(BASE_DIR / 'data/csv')

def create_operator(task_id: str, image: str, command: str) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=f'dataflow_{image}',
        command=f'python {command}.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='dataflow_airflow_net',
        mount_tmp_dir=False,
        auto_remove=True,
        dag=dag
    )

with DAG(
    dag_id='pipeline',
    default_args=default_args,
    # schedule_interval='* * * * *',
    schedule_interval=None,
    catchup=False
) as dag:
    api_producer = create_operator('run_api_producer', 'api_producer', 'fetch_api_producer')
    api_consumer = create_operator('run_api_consumer', 'api_consumer', 'fetch_api_consumer')

    db_producer = create_operator('run_db_producer', 'database_producer', 'extract_database_producer')
    db_consumer = create_operator('run_db_consumer', 'database_consumer', 'database_consumer')

    csv_producer = create_operator('run_csv_producer', 'csv_producer', 'csv_reader_producer')
    csv_consumer = create_operator('run_csv_consumer', 'csv_consumer', 'csv_reader_consumer')

    db_producer >> db_consumer
    csv_producer >> csv_consumer
    api_producer >> api_consumer