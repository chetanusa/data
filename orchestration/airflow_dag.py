from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")

from ingestion.tmdb_producer import fetch_and_produce
from ingestion.tmdb_consumer import consume_to_json
from transformation.spark_transform import run_transform
from warehouse.snowflake_loader import load_to_snowflake

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="tmdb_etl_pipeline",
    default_args=default_args,
    description="TMDB Movie ETL Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tmdb", "etl", "movies"]
) as dag:

    produce_task = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=fetch_and_produce,
        op_kwargs={"pages": 10}
    )

    consume_task = PythonOperator(
        task_id="consume_from_kafka",
        python_callable=consume_to_json,
        op_kwargs={"output_path": "/tmp/raw_movies.json"}
    )

    transform_task = PythonOperator(
        task_id="spark_transform",
        python_callable=run_transform,
        op_kwargs={
            "input_path": "/tmp/raw_movies.json",
            "output_path": "/tmp/transformed_movies"
        }
    )

    load_task = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
        op_kwargs={"parquet_path": "/tmp/transformed_movies"}
    )

    produce_task >> consume_task >> transform_task >> load_task