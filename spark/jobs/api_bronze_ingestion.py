from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import sys

def main():
    # Get input file path from command-line argument
    input_path = sys.argv[1]
    output_path = "/staging/users_bronze_output"

    # Define schema for users.json
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("APIBronzeIngestion") \
        .getOrCreate()

    # Read JSON file with schema
    df = spark.read.schema(schema).json(input_path)

    # Add ingestion_date column with current date
    df_with_date = df.withColumn("ingestion_date", current_date())

    # Write to Parquet in /staging/users_bronze_output
    df_with_date.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    main()