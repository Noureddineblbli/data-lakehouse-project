from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id='api_silver_dag',
    start_date=datetime(2025, 8, 27),
    schedule_interval=None,  # Run manually for testing
    catchup=False,
) as dag:
    # Task 1: Prepare /staging/users_silver_output directory
    prepare_silver_staging = BashOperator(
        task_id='prepare_silver_staging',
        bash_command='mkdir -p /staging/users_silver_output && chmod -R 777 /staging/users_silver_output',
    )

    # Task 2: Run Spark job api_silver_transformation.py
    spark_submit_silver = SparkSubmitOperator(
        task_id='spark_silver_transformation',
        application='/opt/spark/jobs/api_silver_transformation.py',
        conn_id='spark_default',
        jars='/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
    )

    # Task 3: Sync Parquet to S3 Silver bucket and clean up
    sync_to_s3_silver = BashOperator(
        task_id='sync_to_s3_silver',
        bash_command='aws s3 sync /staging/users_silver_output s3://$SILVER_S3_BUCKET/users/ --delete && rm -rf /staging/users_silver_output/*',
    )

    # Set dependency chain: Task 1 -> Task 2 -> Task 3
    prepare_silver_staging >> spark_submit_silver >> sync_to_s3_silver