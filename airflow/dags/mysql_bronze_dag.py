from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="mysql_bronze_ingestion",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["bronze", "database", "transactions"],
) as dag:
    submit_bronze_job = SparkSubmitOperator(
        task_id='submit_mysql_bronze_job',
        application='/opt/spark/jobs/mysql_bronze_ingestion.py',
        conn_id='spark_default',
        name='mysql_bronze_ingestion',
        verbose=True,
        jars='/opt/airflow/jars/mysql-connector-j-8.0.33.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar',
        conf={
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.driver.extraClassPath': '/opt/airflow/jars/mysql-connector-j-8.0.33.jar:/opt/airflow/jars/hadoop-aws-3.3.4.jar:/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar',
            'spark.executor.extraClassPath': '/opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar:/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar',
            'spark.executor.instances': '1',
        }
    )