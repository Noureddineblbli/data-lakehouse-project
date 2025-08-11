from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator # 1. Import BashOperator

with DAG(
    dag_id="product_bronze_ingestion",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["bronze", "products"],
) as dag:
    submit_bronze_job = SparkSubmitOperator(
        task_id='submit_product_bronze_job',
        application='/opt/spark/jobs/bronze_ingestion.py',
        conn_id='spark_default',
        name='arrow-spark',
        verbose=True,
        conf={
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2"
        }
    )

    # 2. Add the BashOperator to move the data
    move_files_to_bronze = BashOperator(
        task_id='move_files_to_bronze_layer',
        bash_command="""
        rm -rf /opt/data/bronze/products && \
        mv /tmp/products_bronze /opt/data/bronze/products
        """
    )

    # 3. Set the task dependency
    submit_bronze_job >> move_files_to_bronze