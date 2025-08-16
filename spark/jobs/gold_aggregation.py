"""
This is the Spark job for creating an aggregated Gold layer table.

It reads the clean data from the Silver layer, performs an aggregation
to calculate metrics per product category, and writes the result to a final,
non-partitioned Parquet file in the Gold layer.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType

def main():
    """
    Main function to run the Gold layer aggregation job.
    """
    spark = SparkSession.builder \
        .appName("GoldProductAggregation") \
        .getOrCreate()

    print("Spark session created successfully.")

    # --- Manually define the schema for the Silver data ---
    # This reflects the transformations we performed in the Silver job.
    silver_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DecimalType(10, 2), True), # Correct, transformed type
        StructField("last_updated", TimestampType(), True), # Correct, transformed type
        StructField("ingestion_date", StringType(), True)
    ])

    # Define the source (Silver) and destination (Gold) paths
    silver_source_path = "s3a://data-lakehouse-silver-cwzr7tyw/products/"
    gold_output_path = "/staging/product_gold_output"

    print(f"Reading data from silver layer at: {silver_source_path}")
    
    # --- Read the Silver data using the defined schema ---
    silver_df = spark.read \
        .format("parquet") \
        .schema(silver_schema) \
        .load(silver_source_path)

    print("Silver data read successfully. Schema:")
    silver_df.printSchema()

    # --- Aggregation Logic ---
    gold_df = silver_df.groupBy("category") \
                       .agg(
                           count("product_id").alias("product_count"),
                           avg("price").alias("average_price")
                       )

    print("Aggregation complete. Gold data schema:")
    gold_df.printSchema()
    gold_df.show()

    # Write the aggregated data to the Gold layer.
    print(f"Writing aggregated data to gold layer at: {gold_output_path}")
    gold_df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(gold_output_path)

    print("Gold layer aggregation job completed successfully!")

    spark.stop()

if __name__ == '__main__':
    main()