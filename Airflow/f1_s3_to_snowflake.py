from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SNOWFLAKE_CONN_ID = "snowflake_f1"


# ---------------------------------------------------------
# Get batch ID passed from DAG 1
# ---------------------------------------------------------

def get_batch_id(**context):

    dag_run = context["dag_run"]

    batch_id = dag_run.conf.get("batch_id")

    if not batch_id:
        raise ValueError(
            "BATCH_ID was not passed from DAG 1."
        )

    print(
        f"Received BATCH_ID from DAG 1: {batch_id}"
    )

    context["ti"].xcom_push(
        key="batch_id",
        value=batch_id
    )


# ---------------------------------------------------------
# DAG 2
# ---------------------------------------------------------

with DAG(
    dag_id="f1_snowflake_pipeline",

    start_date=datetime(2026, 1, 1),

    schedule=None,

    catchup=False,

    tags=[
        "f1",
        "snowflake",
        "bronze"
    ],

) as dag:


    # -----------------------------------------------------
    # 1. Get BATCH_ID from DAG 1
    # -----------------------------------------------------

    get_batch = PythonOperator(

        task_id="get_batch_id",

        python_callable=get_batch_id,
    )


    # -----------------------------------------------------
    # 2. Load CIRCUITS
    # -----------------------------------------------------

    load_circuits = SQLExecuteQueryOperator(

        task_id="load_bronze_circuits",

        conn_id=SNOWFLAKE_CONN_ID,

        sql="""
        COPY INTO F1_DATABASE.BRONZE.CIRCUITS
        FROM @F1_DATABASE.BRONZE.F1_PROCESSED_STAGE
        FILE_FORMAT = (
            FORMAT_NAME = 'F1_DATABASE.BRONZE.F1_PARQUET_FORMAT'
        )
        PATTERN = '.*{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}/circuits/.*\\.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )


    # -----------------------------------------------------
    # 3. Load RACES
    # -----------------------------------------------------

    load_races = SQLExecuteQueryOperator(

        task_id="load_bronze_races",

        conn_id=SNOWFLAKE_CONN_ID,

        sql="""
        COPY INTO F1_DATABASE.BRONZE.RACES
        FROM @F1_DATABASE.BRONZE.F1_PROCESSED_STAGE
        FILE_FORMAT = (
            FORMAT_NAME = 'F1_DATABASE.BRONZE.F1_PARQUET_FORMAT'
        )
        PATTERN = '.*{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}/races/.*\\.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )


    # -----------------------------------------------------
    # 4. Load CONSTRUCTORS
    # -----------------------------------------------------

    load_constructors = SQLExecuteQueryOperator(

        task_id="load_bronze_constructors",

        conn_id=SNOWFLAKE_CONN_ID,

        sql="""
        COPY INTO F1_DATABASE.BRONZE.CONSTRUCTORS
        FROM @F1_DATABASE.BRONZE.F1_PROCESSED_STAGE
        FILE_FORMAT = (
            FORMAT_NAME = 'F1_DATABASE.BRONZE.F1_PARQUET_FORMAT'
        )
        PATTERN = '.*{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}/constructors/.*\\.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )


    # -----------------------------------------------------
    # 5. Load DRIVERS
    # -----------------------------------------------------

    load_drivers = SQLExecuteQueryOperator(

        task_id="load_bronze_drivers",

        conn_id=SNOWFLAKE_CONN_ID,

        sql="""
        COPY INTO F1_DATABASE.BRONZE.DRIVERS
        FROM @F1_DATABASE.BRONZE.F1_PROCESSED_STAGE
        FILE_FORMAT = (
            FORMAT_NAME = 'F1_DATABASE.BRONZE.F1_PARQUET_FORMAT'
        )
        PATTERN = '.*{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}/drivers/.*\\.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )


    # -----------------------------------------------------
    # 6. Load RESULTS
    # -----------------------------------------------------

    load_results = SQLExecuteQueryOperator(

        task_id="load_bronze_results",

        conn_id=SNOWFLAKE_CONN_ID,

        sql="""
        COPY INTO F1_DATABASE.BRONZE.RESULTS
        FROM @F1_DATABASE.BRONZE.F1_PROCESSED_STAGE
        FILE_FORMAT = (
            FORMAT_NAME = 'F1_DATABASE.BRONZE.F1_PARQUET_FORMAT'
        )
        PATTERN = '.*{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}/results/.*\\.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )


    # -----------------------------------------------------
    # 7. Load SPRINTS
    # -----------------------------------------------------

    load_sprints = SQLExecuteQueryOperator(

        task_id="load_bronze_sprints",

        conn_id=SNOWFLAKE_CONN_ID,

        sql="""
        COPY INTO F1_DATABASE.BRONZE.SPRINTS
        FROM @F1_DATABASE.BRONZE.F1_PROCESSED_STAGE
        FILE_FORMAT = (
            FORMAT_NAME = 'F1_DATABASE.BRONZE.F1_PARQUET_FORMAT'
        )
        PATTERN = '.*{{ ti.xcom_pull(task_ids="get_batch_id", key="batch_id") }}/sprints/.*\\.parquet'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
    )


    # -----------------------------------------------------
    # Dependencies
    # -----------------------------------------------------

    get_batch >> [
        load_circuits,
        load_races,
        load_constructors,
        load_drivers,
        load_results,
        load_sprints,
    ]