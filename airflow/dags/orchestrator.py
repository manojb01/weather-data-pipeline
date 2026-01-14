import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

# Add src/pipelines to path
# Add project root to path
sys.path.append('/opt/airflow')
from src.pipelines.insert_records import main

# Get paths from environment or use defaults
PROJECT_ROOT = os.getenv('PROJECT_ROOT', '/opt/airflow')
DOCKER_NETWORK = os.getenv('DOCKER_NETWORK', 'weather_data_data_pipeline')

default_args = {
    'owner': 'data_engineer',
    'description': 'Weather data ETL pipeline',
    'start_date': datetime(2025, 4, 30),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='weather-api-orchestrator',
    default_args=default_args,
    schedule=timedelta(minutes=5),
    tags=['weather', 'etl']
)

with dag:

    task1 = PythonOperator(
        task_id='ingest_data_task',
        python_callable=main
    )

    task2 = DockerOperator(
        task_id='transform_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest@sha256:a705312b55af0ebdd149977914c28502a382d74dca8fe51fff368371a61cc8a7',
        command='run',
        working_dir='/usr/app/my_project',
        mounts=[
            Mount(
                source=f'{PROJECT_ROOT}/dbt/my_project/',
                target='/usr/app/my_project',
                type='bind'
            ),
            Mount(
                source=f'{PROJECT_ROOT}/dbt/profiles.yml',
                target='/root/.dbt/profiles.yml',
                type='bind'
            ),
        ],
        network_mode=DOCKER_NETWORK,
        docker_url='unix://var/run/docker.sock',
        auto_remove='success'
    )

    task1 >> task2

