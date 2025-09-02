from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='api_bronze_dag',
    start_date=datetime(2025, 8, 26),
    schedule_interval=None,  # Run manually for testing
    catchup=False,
) as dag:
    # Task 1: Prepare /staging directories
    prepare_staging = BashOperator(
        task_id='prepare_staging',
        bash_command='mkdir -p /staging && chmod -R 777 /staging',
    )

    # Task 2: Run ingest_api_data.py to create users.json
    ingest_api_data = BashOperator(
        task_id='ingest_api_data',
        bash_command='python /opt/airflow/scripts/ingest_api_data.py',
    )

    # Task 3: Run Spark job api_bronze_ingestion.py
    spark_submit = SparkSubmitOperator(
        task_id='spark_bronze_ingestion',
        application='/opt/spark/jobs/api_bronze_ingestion.py',
        conn_id='spark_default',
        jars='/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
        application_args=['/staging/users.json'],
    )

    # Task 4: Sync Parquet to S3 and clean up
    sync_to_s3 = BashOperator(
        task_id='sync_to_s3',
        bash_command='aws s3 sync /staging/users_bronze_output s3://$BRONZE_S3_BUCKET/users/ --delete && rm -rf /staging/users_bronze_output/* /staging/users.json',
    )

    # Set dependency chain: Task 1 -> Task 2 -> Task 3 -> Task 4
    prepare_staging >> ingest_api_data >> spark_submit >> sync_to_s3