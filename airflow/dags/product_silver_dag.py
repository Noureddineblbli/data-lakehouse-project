from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Define your S3 Silver bucket name here
S3_SILVER_BUCKET = "data-lakehouse-silver-cwzr7tyw"

with DAG(
    dag_id="product_silver_transformation",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["products", "silver"],
) as dag:
    # Task 1: Prepare the staging directory for this specific job's output.
    prepare_staging_directory = BashOperator(
        task_id='prepare_silver_staging_directory',
        bash_command="""
        echo "Creating staging directory for Silver job."
        mkdir -p /staging/products_silver_output
        chmod -R 777 /staging
        """
    )

    # Task 2: Submit the Spark job. It reads from S3 Bronze and writes to /staging/.
    submit_silver_job = SparkSubmitOperator(
        task_id="submit_product_silver_job",
        conn_id="spark_default",
        application="/opt/spark/jobs/silver_transformation.py",
        verbose=True,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    # Task 3: Upload the staged Silver data to S3 and then clean up the staging area.
    upload_to_s3_and_cleanup = BashOperator(
        task_id='upload_to_silver_and_cleanup',
        bash_command=f"""
        echo "Syncing Silver data from staging to S3."
        aws s3 sync /staging/products_silver_output/ s3://{S3_SILVER_BUCKET}/products/
        
        echo "Sync to S3 complete. Now cleaning up staging area."
        rm -rf /staging/products_silver_output
        """
    )

    # Define the task dependencies
    prepare_staging_directory >> submit_silver_job >> upload_to_s3_and_cleanup