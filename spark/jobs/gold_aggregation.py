"""
This is the Spark job for creating an aggregated Gold layer table.

It reads the clean data from the Silver layer, performs an aggregation,
and writes the result to S3, then registers it with AWS Glue Data Catalog using boto3.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType, LongType
import boto3
import os

def create_glue_table(database_name, table_name, s3_location, columns):
    """
    Create or update a table in AWS Glue Data Catalog using boto3.
    """
    glue_client = boto3.client('glue', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
    
    # Define the table structure for Glue
    storage_descriptor = {
        'Columns': columns,
        'Location': s3_location,
        'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
        'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
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
        print(f"Updated table {database_name}.{table_name} in Glue Data Catalog")
    except glue_client.exceptions.EntityNotFoundException:
        # If table doesn't exist, create it
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput=table_input
        )
        print(f"Created table {database_name}.{table_name} in Glue Data Catalog")

def ensure_glue_database_exists(database_name):
    """
    Ensure the Glue database exists, create it if it doesn't.
    """
    glue_client = boto3.client('glue', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
    
    try:
        glue_client.get_database(Name=database_name)
        print(f"Database {database_name} already exists in Glue Data Catalog")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Data lakehouse database for analytics'
            }
        )
        print(f"Created database {database_name} in Glue Data Catalog")

def main():
    """
    Main function to run the Gold layer aggregation job.
    """
    # Create Spark session without Hive/Glue metastore dependencies
    spark = SparkSession.builder \
        .appName("GoldProductAggregation") \
        .config("spark.sql.warehouse.dir", "s3a://data-lakehouse-gold-cwzr7tyw/warehouse/") \
        .getOrCreate()

    print("Spark session created successfully.")

    # Get the Glue Catalog database name
    glue_db_name = "data_lakehouse_db"
    table_name = "product_category_summary"

    # Define the source (Silver) path and the destination (Gold) path
    silver_source_path = "s3a://data-lakehouse-silver-cwzr7tyw/products/"
    gold_output_path = "s3a://data-lakehouse-gold-cwzr7tyw/product_category_summary/"

    print(f"Reading data from silver layer at: {silver_source_path}")
    silver_df = spark.read.format("parquet").load(silver_source_path)
    
    print("Silver data read successfully.")

    # Aggregation Logic
    gold_df = silver_df.groupBy("category") \
                       .agg(
                           count("product_id").alias("product_count"),
                           avg("price").alias("average_price")
                       )

    final_gold_df = gold_df.withColumn("product_count", col("product_count").cast(LongType())) \
                             .withColumn("average_price", col("average_price").cast(DecimalType(10, 2)))

    print("Aggregation complete. Gold data schema:")
    final_gold_df.printSchema()

    # Write the data to S3 in Parquet format
    print(f"Writing data to S3 at: {gold_output_path}")
    final_gold_df.coalesce(1).write \
        .mode("overwrite") \
        .format("parquet") \
        .save(gold_output_path)

    print("Data written to S3 successfully.")

    # Now register the table with AWS Glue Data Catalog using boto3
    print("Registering table with AWS Glue Data Catalog...")
    
    # Ensure the database exists
    ensure_glue_database_exists(glue_db_name)
    
    # Define the table schema for Glue
    columns = [
        {
            'Name': 'category',
            'Type': 'string'
        },
        {
            'Name': 'product_count',
            'Type': 'bigint'
        },
        {
            'Name': 'average_price',
            'Type': 'decimal(10,2)'
        }
    ]
    
    # Create/update the table in Glue Data Catalog
    create_glue_table(glue_db_name, table_name, gold_output_path, columns)

    print("Gold layer aggregation and cataloging job completed successfully!")
    print(f"Table {glue_db_name}.{table_name} is now available in AWS Glue Data Catalog")

    spark.stop()

if __name__ == '__main__':
    main()