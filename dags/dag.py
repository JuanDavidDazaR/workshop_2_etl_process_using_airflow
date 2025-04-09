"""Initialization of the ETL pipeline for Grammy and Spotify data"""

import os
import sys
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from src.extraction.read_csv import read_csv_spotify
from src.extraction.read_db import extract_grammy_database
from src.transformation.spotify import transform_spotify_data
from src.transformation.grammy import transform_grammy_data
from src.merge.merge import merge_spotify_grammy
from src.loading.load import load_to_db
from src.store.store import store_to_drive # Nueva importación

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'etl_extract_transform',
    default_args=default_args,
    description='ETL pipeline para extracción, transformación y carga de datos de Grammy y Spotify',
    schedule_interval=None,
    start_date=datetime(2025, 4, 8),
    catchup=False,
) as dag:

    extract_spotify_task = PythonOperator(
        task_id='read_csv',
        python_callable=read_csv_spotify,
    )

    transform_spotify_task = PythonOperator(
        task_id='transform_spotify',
        python_callable=transform_spotify_data,
        provide_context=True,
    )

    extract_grammy_task = PythonOperator(
        task_id='read_grammy',
        python_callable=extract_grammy_database,
    )

    transform_grammy_task = PythonOperator(
        task_id='transform_grammy',
        python_callable=transform_grammy_data,
        provide_context=True,
    )

    merge_task = PythonOperator(
        task_id='merge_spotify_grammy',
        python_callable=merge_spotify_grammy,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True,
    )
    
    store_to_drive_task = PythonOperator(
        task_id='store_to_drive',
        python_callable=store_to_drive,
        provide_context=True,
    )

    # Definir dependencias
    extract_spotify_task >> transform_spotify_task
    extract_grammy_task >> transform_grammy_task
    [transform_spotify_task, transform_grammy_task] >> merge_task
    merge_task >> load_task
    load_task >> store_to_drive_task  # Nueva dependencia

