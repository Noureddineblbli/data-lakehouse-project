from __future__ import annotations
import os
import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Environment variables that will be passed to the tasks
BRONZE_S3_BUCKET = os.getenv('BRONZE_S3_BUCKET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')

# Using the standard mount path from your docker-compose file for the AWS and MySQL jars
# These are included for consistency with other jobs, even if not all are used here.
jar_paths = (
    "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,"
    "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar"
)

with DAG(
    dag_id="xml_bronze_ingestion",
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
    tags=["bronze", "ingestion", "xml"],
) as dag:
    # Task 1: Prepare the staging area by cleaning any previous output.
    # This makes the pipeline idempotent.
    prepare_staging = BashOperator(
        task_id="prepare_staging_area",
        bash_command="rm -rf /staging/weather_bronze"
    )

    # Task 2: Submit the Spark job.
    # The `packages` parameter tells Spark to download the XML library.
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_xml_ingestion_job",
        application="/opt/spark/jobs/xml_bronze_ingestion.py",
        conn_id="spark_default",
        
        # Here we add the new XML package alongside our existing jars
        jars=jar_paths,
        packages="com.databricks:spark-xml_2.12:0.17.0",

        # Provide standard S3 configuration for the AWS CLI to use later
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": f"s3.{AWS_REGION}.amazonaws.com",
        },

        # Pass environment variables to the AWS CLI in the next task
        env_vars={
            'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY
        }
    )

    # Task 3: Upload the processed data to S3 and clean up the local staging area.
    # The `&&` ensures that the local `rm` command only runs if the `aws s3 sync` is successful.
    upload_and_cleanup = BashOperator(
        task_id="upload_to_s3_and_cleanup",
        bash_command=f"""
            echo "Uploading processed data to S3 bronze bucket..."
            aws s3 sync /staging/weather_bronze s3://{BRONZE_S3_BUCKET}/weather/
            echo "Upload complete. Cleaning up staging area..."
            rm -rf /staging/weather_bronze
            echo "Cleanup complete."
        """
    )

    # Define the DAG's task flow
    prepare_staging >> submit_spark_job >> upload_and_cleanup