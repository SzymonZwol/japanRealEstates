from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_world",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # manual trigger
    catchup=False,
    tags=["demo"],
) as dag:
    hello = BashOperator(
        task_id="hello",
        bash_command='echo "Hello World from Astro!"',
    )
