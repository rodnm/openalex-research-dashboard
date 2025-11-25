from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add /opt/airflow to sys.path so we can import from etl/
sys.path.append('/opt/airflow')

from etl.bronze import fetch_works
from etl.silver import process_bronze_to_silver
from etl.gold import process_silver_to_gold

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'openalex_medallion_etl',
    default_args=default_args,
    description='ETL pipeline for OpenAlex data',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    topics = [
        "Artificial intelligence",
        "Economics",
        "Sociology",
        "History",
        "Physics",
        "Biology",
        "Geopolitics",
        "Statistics",
        "Mathematics"
    ]

    extract_tasks = []
    for topic in topics:
        task_id = f'extract_bronze_{topic.replace(" ", "_").lower()}'
        t1 = PythonOperator(
            task_id=task_id,
            python_callable=fetch_works,
            op_kwargs={'concept_name': topic, 'limit': 200}
        )
        extract_tasks.append(t1)

    t2 = PythonOperator(
        task_id='transform_silver',
        python_callable=process_bronze_to_silver,
    )

    t3 = PythonOperator(
        task_id='aggregate_gold',
        python_callable=process_silver_to_gold,
    )

    # All extract tasks must finish before transform
    extract_tasks >> t2 >> t3
