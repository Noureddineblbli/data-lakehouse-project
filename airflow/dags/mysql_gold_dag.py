from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="mysql_gold_aggregation",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["gold", "aggregation", "transactions"],
) as dag:
    submit_gold_aggregation_job = SparkSubmitOperator(
        task_id='submit_gold_aggregation_job',
        application='/opt/spark/jobs/mysql_gold_aggregation.py',
        conn_id='spark_default',
        name='mysql_gold_aggregation',
        verbose=True,
        jars='/opt/airflow/jars/mysql-connector-j-8.0.33.jar,'
             '/opt/airflow/jars/hadoop-aws-3.3.4.jar,'
             '/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,'
             '/opt/airflow/jars/hive3-shims-3.4.0-SNAPSHOT.jar',
        conf={
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.catalogImplementation': 'hive',
            'spark.hadoop.hive.metastore.client.factory.class':
                'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
            'spark.hadoop.fs.s3a.warehouse': 's3a://data-lakehouse-gold-cwzr7tyw/',
            'spark.hadoop.hive.metastore.schema.verification': 'false',
            # Updated to match Spark 3.5.0's built-in Hive version
            'spark.sql.hive.metastore.version': '2.3.9',
            'spark.sql.hive.metastore.jars': 'builtin',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.driver.extraClassPath':
                '/opt/airflow/jars/mysql-connector-j-8.0.33.jar:'
                '/opt/airflow/jars/hadoop-aws-3.3.4.jar:'
                '/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar:'
                '/opt/airflow/jars/hive3-shims-3.4.0-SNAPSHOT.jar',
            'spark.executor.extraClassPath':
                '/opt/bitnami/spark/jars/mysql-connector-j-8.0.33.jar:'
                '/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar:'
                '/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar:'
                '/opt/bitnami/spark/jars/hive3-shims-3.4.0-SNAPSHOT.jar',
            'spark.executor.instances': '1',
        }
    )
