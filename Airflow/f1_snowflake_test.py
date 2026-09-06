from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator


with DAG(
    dag_id="f1_mwaa_snowflake_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["f1", "test", "snowflake"],
) as dag:

    snowflake_connection_test = SQLExecuteQueryOperator(
        task_id="snowflake_connection_test",
        conn_id="snowflake_f1",
        sql="""
            SELECT
                CURRENT_USER(),
                CURRENT_ROLE(),
                CURRENT_DATABASE(),
                CURRENT_WAREHOUSE();
        """,
    )