
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import boto3


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

BUCKET_NAME = "f1-data-engineering-bucket"
LANDING_PREFIX = "landing/"

GLUE_REGION = "ap-southeast-2"


# ---------------------------------------------------------
# 1. Detect latest batch
# ---------------------------------------------------------

def detect_batch(**context):
    s3 = boto3.client("s3", region_name=GLUE_REGION)

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=LANDING_PREFIX,
        Delimiter="/"
    )

    batch_folders = []

    for prefix in response.get("CommonPrefixes", []):
        folder = prefix["Prefix"]

        # Example:
        # landing/2025-01/
        batch_id = folder.replace(LANDING_PREFIX, "").strip("/")

        if batch_id:
            batch_folders.append(batch_id)

    if not batch_folders:
        raise ValueError("No batch folders found in S3.")

    # YYYY-MM format makes this chronological
    batch_id = sorted(batch_folders)[-1]

    print(f"Detected BATCH_ID: {batch_id}")

    # Make batch_id available to downstream Airflow tasks
    context["ti"].xcom_push(
        key="batch_id",
        value=batch_id
    )


# ---------------------------------------------------------
# 2. Validate batch
# ---------------------------------------------------------

def validate_batch(**context):
    s3 = boto3.client("s3", region_name=GLUE_REGION)

    batch_id = context["ti"].xcom_pull(
        task_ids="detect_batch",
        key="batch_id"
    )

    if not batch_id:
        raise ValueError("BATCH_ID was not found.")

    required_objects = [
        f"landing/{batch_id}/circuits.csv",
        f"landing/{batch_id}/constructors.json",
        f"landing/{batch_id}/drivers.json",
        f"landing/{batch_id}/races.csv",
    ]

    required_prefixes = [
        f"landing/{batch_id}/results/",
        f"landing/{batch_id}/sprints/",
    ]

    # Check individual files
    for key in required_objects:
        try:
            s3.head_object(
                Bucket=BUCKET_NAME,
                Key=key
            )

            print(
                f"Found: s3://{BUCKET_NAME}/{key}"
            )

        except Exception as e:
            raise ValueError(
                f"Required object not found: "
                f"s3://{BUCKET_NAME}/{key}"
            ) from e

    # Check results/sprints folders
    for prefix in required_prefixes:
        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix,
            MaxKeys=1
        )

        if response.get("KeyCount", 0) == 0:
            raise ValueError(
                f"No files found under: "
                f"s3://{BUCKET_NAME}/{prefix}"
            )

        print(
            f"Found data under: "
            f"s3://{BUCKET_NAME}/{prefix}"
        )

    print(f"Batch validation successful: {batch_id}")


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id="f1_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["f1", "bronze", "glue"],
) as dag:

    detect_batch = PythonOperator(
        task_id="detect_batch",
        python_callable=detect_batch,
    )

    validate_batch = PythonOperator(
        task_id="validate_batch",
        python_callable=validate_batch,
    )

    circuits_ingestion = GlueJobOperator(
        task_id="circuits_ingestion",
        job_name="f1_circuits_ingestion",
        region_name=GLUE_REGION,
        script_args={
            "--BATCH_ID": "{{ ti.xcom_pull(task_ids='detect_batch', key='batch_id') }}"
        },
        wait_for_completion=True,
    )

    races_ingestion = GlueJobOperator(
        task_id="races_ingestion",
        job_name="f1_races_ingestion",
        region_name=GLUE_REGION,
        script_args={
            "--BATCH_ID": "{{ ti.xcom_pull(task_ids='detect_batch', key='batch_id') }}"
        },
        wait_for_completion=True,
    )

    constructors_ingestion = GlueJobOperator(
        task_id="constructors_ingestion",
        job_name="3.f1_constructors_ingestion",
        region_name=GLUE_REGION,
        script_args={
            "--BATCH_ID": "{{ ti.xcom_pull(task_ids='detect_batch', key='batch_id') }}"
        },
        wait_for_completion=True,
    )

    drivers_ingestion = GlueJobOperator(
        task_id="drivers_ingestion",
        job_name="4.f1_drivers_ingestion",
        region_name=GLUE_REGION,
        script_args={
            "--BATCH_ID": "{{ ti.xcom_pull(task_ids='detect_batch', key='batch_id') }}"
        },
        wait_for_completion=True,
    )

    results_ingestion = GlueJobOperator(
        task_id="results_ingestion",
        job_name="f1_results_ingestion",
        region_name=GLUE_REGION,
        script_args={
            "--BATCH_ID": "{{ ti.xcom_pull(task_ids='detect_batch', key='batch_id') }}"
        },
        wait_for_completion=True,
    )

    sprints_ingestion = GlueJobOperator(
        task_id="sprints_ingestion",
        job_name="f1_sprints_ingestion",
        region_name=GLUE_REGION,
        script_args={
            "--BATCH_ID": "{{ ti.xcom_pull(task_ids='detect_batch', key='batch_id') }}"
        },
        wait_for_completion=True,
    )

    bronze_complete = PythonOperator(
        task_id="bronze_complete",
        python_callable=lambda: print(
            "Bronze ingestion completed successfully."
        ),
    )

    # -----------------------------------------------------
    # Dependencies
    # -----------------------------------------------------

    detect_batch >> validate_batch

    validate_batch >> [
        circuits_ingestion,
        races_ingestion,
        constructors_ingestion,
        drivers_ingestion,
        results_ingestion,
        sprints_ingestion,
    ]

    [
        circuits_ingestion,
        races_ingestion,
        constructors_ingestion,
        drivers_ingestion,
        results_ingestion,
        sprints_ingestion,
    ] >> bronze_complete

