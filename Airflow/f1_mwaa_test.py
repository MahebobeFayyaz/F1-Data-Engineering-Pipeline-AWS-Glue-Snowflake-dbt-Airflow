from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def test_mwaa():
    print("====================================")
    print("F1 MWAA TEST DAG RUNNING SUCCESSFULLY")
    print("====================================")


with DAG(
    dag_id="f1_mwaa_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["f1", "test"],
) as dag:

    test_task = PythonOperator(
        task_id="test_mwaa_task",
        python_callable=test_mwaa,
    )