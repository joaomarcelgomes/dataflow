from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from pathlib import Path
from docker.types import Mount as DockerMount

default_args = {
    'start_date': datetime(2025, 7, 1),
}

CSV_DIR = str(Path(__file__).resolve().parent.parent / 'data/csv')

def create_operator(taskid, image, command):
    operator = DockerOperator(
        task_id=taskid,
        image=f'dataflow_{image}', 
        command=f'python {command}.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='dataflow_airflow_net',
        mount_tmp_dir=False,
        auto_remove=True
    )

    return operator

with DAG(
    dag_id='pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    run_api_producer = create_operator(
        'run_api_producer',
        'api_producer',
        'fetch_api_producer'
    )

    run_api_consumer = create_operator(
        'run_api_consumer',
        'api_consumer',
        'fetch_api_consumer'
    )

    run_db_producer = create_operator(
        'run_db_producer', 
        'database_producer',
        'extract_database_producer'
    )

    run_db_consumer = create_operator(
        'run_db_consumer', 
        'database_consumer',
        'database_consumer'
    )

    run_csv_producer = create_operator(
        'run_csv_producer',
        'csv_producer',
        'csv_reader_producer'
    )

    run_csv_consumer = create_operator(
        'run_csv_consumer',
        'csv_consumer',
        'csv_reader_consumer'
    )

    run_api_producer >> run_api_consumer
    run_db_producer >> run_db_consumer
    # run_csv_producer >> run_csv_consumer