from __future__ import annotations
import os
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

# Define your S3 Gold bucket name and AWS region here
S3_GOLD_BUCKET = "data-lakehouse-gold-cwzr7tyw"
AWS_REGION = "eu-west-3"

# --- Securely Read Credentials from Environment ---
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Create a dictionary for the BashOperator's environment
aws_cli_env = {
    "AWS_ACCESS_KEY_ID": aws_access_key_id,
    "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
    "AWS_DEFAULT_REGION": AWS_REGION
}
# --- End of Secure Credential Handling ---

with DAG(
    dag_id="product_gold_aggregation",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule_interval=None,
    catchup=False,
    tags=["products", "gold"],
) as dag:
    prepare_staging_directory = BashOperator(
        task_id='prepare_gold_staging_directory',
        bash_command="mkdir -p /staging/product_gold_output && chmod -R 777 /staging"
    )

    submit_gold_job = SparkSubmitOperator(
        task_id="submit_product_gold_job",
        conn_id="spark_default",
        application="/opt/spark/jobs/gold_aggregation.py",
        verbose=True,
        conf={
            "spark.hadoop.aws.region": AWS_REGION,
            "spark.sql.warehouse.dir": f"s3a://{S3_GOLD_BUCKET}/warehouse/",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.access.key": aws_access_key_id,
            "spark.hadoop.fs.s3a.secret.key": aws_secret_access_key,
        }
    )
    upload_to_s3_and_cleanup = BashOperator(
        task_id='upload_to_gold_and_cleanup',
        env=aws_cli_env,
        bash_command=f"""
        aws s3 sync /staging/product_gold_output/ s3://{S3_GOLD_BUCKET}/product_category_summary/ && \
        rm -rf /staging/product_gold_output
        """
    )

    prepare_staging_directory >> submit_gold_job >> upload_to_s3_and_cleanup