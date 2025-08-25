import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, sum as sum_, count
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, LongType
import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_glue_table(database_name, table_name, s3_location, columns):
    """
    Create or update a table in AWS Glue Data Catalog using boto3.
    """
    glue_client = boto3.client(
        'glue', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

    # Define the table structure for Glue
    storage_descriptor = {
        'Columns': columns,
        'Location': s3_location,
        'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
        'SerdeInfo': {
            'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        },
        'StoredAsSubDirectories': False
    }

    table_input = {
        'Name': table_name,
        'StorageDescriptor': storage_descriptor,
        'PartitionKeys': [],  # Add partition keys if needed
        'TableType': 'EXTERNAL_TABLE'
    }

    try:
        # Try to update the table first
        glue_client.update_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        logger.info(
            f"Updated table {database_name}.{table_name} in Glue Data Catalog")
    except glue_client.exceptions.EntityNotFoundException:
        # If table doesn't exist, create it
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        logger.info(
            f"Created table {database_name}.{table_name} in Glue Data Catalog")


def ensure_glue_database_exists(database_name):
    """
    Ensure the Glue database exists, create it if it doesn't.
    """
    glue_client = boto3.client(
        'glue', region_name=os.environ.get('AWS_REGION', 'us-east-1'))

    try:
        glue_client.get_database(Name=database_name)
        logger.info(
            f"Database {database_name} already exists in Glue Data Catalog")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Data lakehouse database for transaction analytics'
            }
        )
        logger.info(f"Created database {database_name} in Glue Data Catalog")


def get_spark_session():
    """
    Get the existing Spark session created by SparkSubmit
    """
    try:
        # Get the existing SparkSession instead of creating a new one
        spark = SparkSession.getActiveSession()
        if spark is None:
            # Fallback: get or create (but this should not happen with SparkSubmit)
            spark = SparkSession.builder.getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Using existing Spark session")
        return spark

    except Exception as e:
        logger.error(f"Failed to get Spark session: {str(e)}")
        raise


def read_silver_data(spark, silver_path):
    """
    Read Parquet data from the Silver S3 bucket
    """
    try:
        logger.info(f"Reading Parquet data from: {silver_path}")
        df = spark.read.parquet(silver_path)
        record_count = df.count()
        logger.info(f"Successfully read {record_count} records from Silver")
        return df

    except Exception as e:
        logger.error(
            f"Failed to read Silver data from {silver_path}: {str(e)}")
        raise


def aggregate_data(df):
    aggregated_df = (
        df.groupBy(to_date(col("transaction_date")).alias("transaction_date"))
          .agg(
              sum_("transaction_amount").cast("double").alias("total_amount"),
              count("transaction_id").alias("transaction_count")
        )
    )
    return aggregated_df


def write_to_gold_and_register(spark, df, gold_path, database_name, table_name):
    """
    Write aggregated data to Gold S3 bucket and register as an external table in Glue database
    """
    try:
        logger.info(f"Writing aggregated data to Gold S3: {gold_path}")
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .format("parquet") \
            .save(gold_path)

        logger.info("Registering table with AWS Glue Data Catalog...")
        ensure_glue_database_exists(database_name)

        # Define the table schema for Glue
        columns = [
            {'Name': 'transaction_date', 'Type': 'date'},
            {'Name': 'total_amount', 'Type': 'double'},
            {'Name': 'transaction_count', 'Type': 'bigint'}
        ]

        # Convert s3a:// to s3:// for Glue registration (Athena compatibility)
        glue_s3_path = gold_path.replace("s3a://", "s3://")
        logger.info(f"Registering table with S3 path: {glue_s3_path}")

        create_glue_table(database_name, table_name, glue_s3_path, columns)

        logger.info(
            f"Successfully written and registered data to {gold_path} and {database_name}.{table_name}")

        # Create the database in Spark catalog if it doesn't exist
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        
        # Refresh the table metadata to ensure Spark can see the Glue table
        try:
            spark.sql(f"REFRESH TABLE {database_name}.{table_name}")
        except Exception as refresh_error:
            logger.warning(f"Could not refresh table (this is normal for new tables): {refresh_error}")
            # Create the table in Spark catalog pointing to the S3 location
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
                    transaction_date DATE,
                    total_amount DOUBLE,
                    transaction_count BIGINT
                )
                USING PARQUET
                LOCATION '{gold_path}'
            """)
        
        # Verify
        verify_df = spark.sql(f"SELECT * FROM {database_name}.{table_name}")
        written_count = verify_df.count()
        logger.info(f"Verification: {written_count} records in Gold table")

    except Exception as e:
        logger.error(f"Failed to write or register data: {str(e)}")
        raise


def main():
    """
    Main execution function
    """
    spark = None

    try:
        logger.info("Starting Gold aggregation job")

        # Get the existing Spark session (created by SparkSubmit)
        spark = get_spark_session()

        # Define S3 paths and Glue details
        silver_bucket = os.getenv(
            "SILVER_S3_BUCKET", "data-lakehouse-silver-cwzr7tyw")
        gold_bucket = os.getenv(
            "GOLD_S3_BUCKET", "data-lakehouse-gold-cwzr7tyw")
        silver_path = f"s3a://{silver_bucket}/transactions/"
        gold_path = f"s3a://{gold_bucket}/daily_sales_summary/"
        database_name = "data_lakehouse_db"
        table_name = "daily_sales_summary"

        # Read Silver data
        df = read_silver_data(spark, silver_path)

        # Aggregate data
        aggregated_df = aggregate_data(df)

        aggregated_df.printSchema()

        # Write to Gold and register
        write_to_gold_and_register(
            spark, aggregated_df, gold_path, database_name, table_name)

        logger.info("Gold aggregation completed successfully")

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        sys.exit(1)

    finally:
        # Don't stop the Spark session - let SparkSubmit handle it
        logger.info("Job completed")


if __name__ == "__main__":
    main()