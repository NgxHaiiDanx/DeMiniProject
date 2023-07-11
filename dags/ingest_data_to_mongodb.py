from datetime import datetime
from common.constants import tz_vn
from airflow import DAG
from airflow.decorators import task
from common.mongo import get_data_from_json


url = "https://api.covidtracking.com/v2/us/daily.json"

with DAG(
    dag_id='ingest_uri_publish_mongodb',
    schedule=None,
    start_date=datetime(year=2023, month=7, day=7, hour=10, tzinfo=tz_vn),
    catchup=False,
) as dag:
    @task(task_id='ingest_mongodb')
    def ingest_mongodb(url):
        get_data_from_json(url)
        return True
    ingest_mongodb(url)
