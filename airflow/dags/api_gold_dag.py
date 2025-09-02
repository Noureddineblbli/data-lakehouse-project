from __future__ import annotations
import os
import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Environment variables that the Spark job will need
AWS_REGION = os.getenv('AWS_REGION', 'eu-west-3')
SILVER_S3_BUCKET = os.getenv('SILVER_S3_BUCKET')
GOLD_S3_BUCKET = os.getenv('GOLD_S3_BUCKET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

# Define the paths to the jars inside the container, using the proven correct path
# These are only needed for Spark to talk to S3 and for Boto3 to work.
jar_paths = (
    "/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,"
    "/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
)

with DAG(
    dag_id="api_gold_dag",
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 30, tz="UTC"),
    catchup=False,
    tags=["data-lakehouse", "gold"],
) as dag:
    spark_gold_aggregation = SparkSubmitOperator(
        task_id="spark_gold_aggregation",
        application="/opt/spark/jobs/api_gold_aggregation.py",
        conn_id="spark_default",
        
        # We only need the jars for S3 access
        jars=jar_paths,
        
        # The configuration is now very simple. We REMOVED all Hive/Glue settings.
        conf={
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": f"s3.{AWS_REGION}.amazonaws.com",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        },

        # Pass ALL necessary environment variables for both S3 access (Spark) and Glue access (Boto3)
        env_vars={
            'SILVER_S3_BUCKET': SILVER_S3_BUCKET,
            'GOLD_S3_BUCKET': GOLD_S3_BUCKET,
            'AWS_REGION': AWS_REGION,
            'AWS_ACCESS_KEY_ID': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY': AWS_SECRET_ACCESS_KEY,
        }
    )