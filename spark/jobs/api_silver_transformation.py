from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DateType
from pyspark.sql.functions import to_timestamp
import os

def main():
    # Get S3 bucket name from environment variable
    bronze_s3_bucket = os.environ.get("BRONZE_S3_BUCKET")
    input_path = f"s3a://{bronze_s3_bucket}/users/"

    # Define schema for the Parquet data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("ingestion_date", DateType(), True)
    ])

    # Initialize Spark session with S3 configurations
    spark = SparkSession.builder \
        .appName("APISilverTransformation") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.environ.get('AWS_REGION')}.amazonaws.com") \
        .getOrCreate()

    # Read Parquet data from S3
    df = spark.read.schema(schema).parquet(input_path)

    # Ensure created_at is a timestamp
    df_transformed = df.withColumn("created_at", to_timestamp("created_at"))

    # Write to Parquet in /staging/users_silver_output
    df_transformed.write.mode("overwrite").parquet("/staging/users_silver_output")

    spark.stop()

if __name__ == "__main__":
    main()