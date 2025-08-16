from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime


def main():
    spark = SparkSession.builder \
        .appName("BronzeProductIngestion") \
        .getOrCreate()

    print("Spark session created successfully.")

    # Input path (on host-mounted volume, read-only)
    source_path = "/opt/data/products.csv"

    # Staging path (on the shared Docker named volume)
    # This path is accessible by both Spark worker and Airflow containers
    staging_output_path = "/staging/products_bronze_output"

    ingestion_date = datetime.now().strftime("%Y-%m-%d")
    print(f"Using ingestion date: {ingestion_date}")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(source_path)

    df_with_date = df.withColumn("ingestion_date", lit(ingestion_date))

    # Write the output to the shared STAGING volume. This is a reliable operation.
    print(f"Writing data to staging area at: {staging_output_path}")
    df_with_date.write \
        .mode("overwrite") \
        .parquet(staging_output_path)

    print("Spark job finished. Data is now in the staging area.")
    spark.stop()


if __name__ == '__main__':
    main()
