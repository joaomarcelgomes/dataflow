from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 1),
}

def create_operator(taskid, image, command):
    operator = DockerOperator(
        task_id=taskid,
        image=f'dataflow-{image}', 
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

    # run_db_producer = DockerOperator(
    #     task_id='run_db_producer',
    #     image='dataflow-database_producer', 
    #     command='python extract_database_producer.py',
    #     docker_url='unix://var/run/docker.sock',
    #     network_mode='dataflow_airflow_net',
    #     mount_tmp_dir=False,
    #     auto_remove=True
    # )

    run_db_producer = create_operator(
        'run_db_producer', 
        'database_producer',
        'extract_database_producer'
    )

    run_db_producer