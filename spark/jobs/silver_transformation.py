"""
This is the Spark job for transforming product data from the Bronze to the Silver layer.

It reads the partitioned Parquet data from Bronze, applies data quality checks,
cleanses and standardizes the data, and writes it back to a partitioned
Parquet format in the Silver layer.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DecimalType

def main():
    """
    Main function to run the Spark job for Silver transformation.
    """
    spark = SparkSession.builder \
        .appName("SilverProductTransformation") \
        .getOrCreate()

    print("Spark session created successfully.")

    # --- Manually define the schema for the Bronze data ---
    # MODIFIED: Changed last_updated from StringType to TimestampType to match
    # the actual data type in the Parquet file.
    bronze_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("last_updated", TimestampType(), True), # It is a timestamp in the Bronze Parquet
        StructField("ingestion_date", StringType(), True)
    ])

    # Define the source (Bronze) and destination (Silver) paths
    bronze_source_path = "s3a://data-lakehouse-bronze-cwzr7tyw/products/"
    silver_output_path = "/staging/products_silver_output"

    print(f"Reading data from bronze layer at: {bronze_source_path}")
    
    # --- Read the Bronze data using the defined schema ---
    bronze_df = spark.read \
        .format("parquet") \
        .schema(bronze_schema) \
        .load(bronze_source_path)

    print("Bronze data read successfully. Schema:")
    bronze_df.printSchema()
    
    # --- Transformations and Data Quality ---
    # MODIFIED: The to_timestamp transformation is no longer needed since
    # the data is now read correctly as a timestamp.
    silver_df = bronze_df.withColumn("price", col("price").cast(DecimalType(10, 2))) \
                         .withColumn("category", lower(col("category")))
    
    print("Transformations applied successfully. Transformed schema:")
    silver_df.printSchema()
    silver_df.show(5)

    # Write the cleaned and transformed data to the Silver layer
    print(f"Writing data to silver layer at: {silver_output_path}")
    silver_df.write \
        .partitionBy("ingestion_date") \
        .mode("overwrite") \
        .parquet(silver_output_path)

    print("Silver layer transformation job completed successfully!")

    spark.stop()

if __name__ == '__main__':
    main()