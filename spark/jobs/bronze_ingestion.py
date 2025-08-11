"""
This is the Spark job for the Bronze layer ingestion of product data.

It reads the raw CSV file and writes it to a partitioned Parquet format
in the Bronze layer location.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from datetime import datetime


def main():
    """
    Main function to run the Spark job.
    """
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("BronzeProductIngestion") \
        .getOrCreate()

    print("Spark session created successfully.")

    # Define the source and destination paths
    # Note: These paths are inside the Docker container's filesystem.
    # We mounted our local './data' directory to '/opt/bitnami/spark/data' in docker-compose.
    source_path = "/opt/data/products.csv"
    bronze_output_path = "/tmp/products_bronze"

    # Get the current date for partitioning
    # In a real pipeline, this would come from the DAG's execution date.
    ingestion_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Using ingestion date: {ingestion_date}")

    # Read the raw CSV data
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)

    print("Raw data read successfully. Schema:")
    df.printSchema()
    df.show(5)

    # Add an ingestion_date column for partitioning our data lake
    df_with_date = df.withColumn("ingestion_date", lit(ingestion_date))

    # Write the data to the bronze layer, partitioned by the ingestion date
    # The format is Parquet, which is a highly efficient columnar storage format.
    # The mode is 'overwrite' to ensure idempotency for a given day's run.
    print(f"Writing data to bronze layer at: {bronze_output_path}")
    df_with_date.write \
        .mode("overwrite") \
        .parquet(bronze_output_path)

    print("Bronze layer ingestion job completed successfully!")

    # Stop the Spark session
    spark.stop()


if __name__ == '__main__':
    main()
