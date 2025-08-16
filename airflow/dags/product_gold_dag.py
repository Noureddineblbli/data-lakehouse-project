from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Define your S3 Gold bucket name here
S3_GOLD_BUCKET = "data-lakehouse-gold-cwzr7tyw"

with DAG(
    dag_id="product_gold_aggregation",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval=None, catchup=False, tags=["products", "gold"],
) as dag:
    # Task 1: Prepare the staging directory
    prepare_staging_directory = BashOperator(
        task_id='prepare_gold_staging_directory',
        bash_command="mkdir -p /staging/product_gold_output && chmod -R 777 /staging"
    )

    # Task 2: Your Spark job, now reading from S3 and writing to staging
    submit_gold_job = SparkSubmitOperator(
        task_id="submit_product_gold_job",
        conn_id="spark_default",
        application="/opt/spark/jobs/gold_aggregation.py",
        verbose=True,
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
    )

    # Task 3: Upload the final aggregated data to the S3 Gold bucket
    upload_to_s3_and_cleanup = BashOperator(
        task_id='upload_to_gold_and_cleanup',
        bash_command=f"""
        aws s3 sync /staging/product_gold_output/ s3://{S3_GOLD_BUCKET}/product_category_summary/ && \
        rm -rf /staging/product_gold_output
        """
    )

    # Define the task dependencies
    prepare_staging_directory >> submit_gold_job >> upload_to_s3_and_cleanup