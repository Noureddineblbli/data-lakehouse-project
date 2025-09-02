import os
import sys
import logging
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, date_format
from pyspark.sql.types import LongType, StringType

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def ensure_glue_database_exists(database_name):
    """Ensure the Glue database exists, create it if it doesn't, using Boto3."""
    glue_client = boto3.client(
        'glue', region_name=os.environ.get('AWS_REGION', 'eu-west-3'))
    try:
        glue_client.get_database(Name=database_name)
        logger.info(
            f"Database {database_name} already exists in Glue Data Catalog.")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={'Name': database_name,
                           'Description': 'Data lakehouse database'}
        )
        logger.info(f"Created database {database_name} in Glue Data Catalog.")


def create_glue_table(database_name, table_name, s3_location, columns):
    """Create or update a table in AWS Glue Data Catalog using Boto3."""
    glue_client = boto3.client(
        'glue', region_name=os.environ.get('AWS_REGION', 'eu-west-3'))

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
        'TableType': 'EXTERNAL_TABLE'
    }

    try:
        glue_client.update_table(
            DatabaseName=database_name, TableInput=table_input)
        logger.info(
            f"Updated table {database_name}.{table_name} in Glue Data Catalog.")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(
            DatabaseName=database_name, TableInput=table_input)
        logger.info(
            f"Created table {database_name}.{table_name} in Glue Data Catalog.")


def main():
    """Main execution function."""
    spark = None
    try:
        logger.info("Starting API Gold aggregation job")
        spark = SparkSession.builder.appName("APIGoldBoto3").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        # 1. Define paths and details
        silver_bucket = os.getenv("SILVER_S3_BUCKET")
        gold_bucket = os.getenv("GOLD_S3_BUCKET")
        database_name = "data_lakehouse_db"
        table_name = "monthly_user_growth"

        silver_path = f"s3a://{silver_bucket}/users/"
        gold_path = f"s3a://{gold_bucket}/api_aggregations/{table_name}/"

        # 2. Read and transform data
        logger.info(f"Reading from Silver path: {silver_path}")
        silver_df = spark.read.parquet(silver_path)

        final_df = silver_df \
            .withColumn("year_month", date_format(col("created_at"), "yyyy-MM")) \
            .groupBy("year_month") \
            .count() \
            .withColumnRenamed("count", "monthly_user_count") \
            .orderBy("year_month") \
            .select(col("year_month").cast(StringType()), col("monthly_user_count").cast(LongType()))

        logger.info("Data aggregation complete.")
        final_df.printSchema()
        final_df.show(5)

        # 3. Write data to S3
        logger.info(f"Writing aggregated data to Gold S3 path: {gold_path}")
        final_df.coalesce(1).write.mode(
            "overwrite").format("parquet").save(gold_path)
        logger.info("Data written to S3 successfully.")

        # 4. Register with Glue using Boto3
        logger.info("Registering table with AWS Glue Data Catalog via Boto3...")
        ensure_glue_database_exists(database_name)

        columns = [
            {'Name': 'year_month', 'Type': 'string'},
            {'Name': 'monthly_user_count', 'Type': 'bigint'}
        ]

        # Convert s3a:// to s3:// for Glue/Athena compatibility
        glue_s3_path = gold_path.replace("s3a://", "s3://")
        create_glue_table(database_name, table_name, glue_s3_path, columns)

        logger.info(
            "Gold aggregation and Glue registration completed successfully!")

    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
