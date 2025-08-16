from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Define your S3 bucket name here
S3_BRONZE_BUCKET = "data-lakehouse-bronze-cwzr7tyw"

with DAG(
    dag_id="product_bronze_ingestion",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["bronze", "products"],
) as dag:
    # Task 1: Your staging directory preparation task (unchanged).
    prepare_staging_directory = BashOperator(
        task_id='prepare_staging_directory',
        bash_command="""
        echo "Creating and setting permissions for the staging directory."
        mkdir -p /staging/products_bronze_output
        chmod -R 777 /staging
        echo "Staging directory is ready."
        """
    )

    # Task 2: Your Spark job submission task (unchanged).
    submit_bronze_job = SparkSubmitOperator(
        task_id='submit_product_bronze_job',
        application='/opt/spark/jobs/bronze_ingestion.py',
        conn_id='spark_default',
        name='arrow-spark',
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    # Task 3: (MODIFIED) - Upload from staging to S3 and then clean up.
    upload_to_s3_and_cleanup = BashOperator(
        task_id='upload_to_s3_and_cleanup',
        bash_command=f"""
        echo "Syncing data from staging to S3."
        aws s3 sync /staging/products_bronze_output/ s3://{S3_BRONZE_BUCKET}/products/
        
        echo "Sync to S3 complete. Now cleaning up staging area."
        rm -rf /staging/products_bronze_output
        """
    )

    # Define the task dependencies (unchanged logic).
    prepare_staging_directory >> submit_bronze_job >> upload_to_s3_and_cleanup