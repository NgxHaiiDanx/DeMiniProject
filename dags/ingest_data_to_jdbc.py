from datetime import datetime
from common.constants import tz_vn
from airflow import DAG
from airflow.decorators import task

from common.postgres import process_table
from common.postgres import PostgresExecutor


download_urls = [
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip',
    'https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip'
]

with DAG(
    dag_id='ingest_uri_publish_jdbc',
    schedule=None,
    start_date=datetime(year=2023, month=7, day=7, hour=10, tzinfo=tz_vn),
    catchup=False,
) as dag:
    @task(task_id='create_fact_table')
    def ingest_process_trip_table(urls):
        process_table(urls)
        return True
    fact_table = ingest_process_trip_table(download_urls)

    @task(task_id='create_dim_table')
    def publish_dim_table():
        PostgresExecutor.create_dim_table_process()
        return True
    dim_table = publish_dim_table()

    fact_table >> dim_table

















