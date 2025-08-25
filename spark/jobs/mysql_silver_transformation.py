import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Initialize Spark session with S3 configurations
    """
    try:
        spark = SparkSession.builder \
            .appName("MySQL_Silver_Transformation") \
            .config("spark.jars", "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "s3.amazonaws.com")) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
    
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def read_bronze_data(spark, bronze_path):
    """
    Read Parquet data from the Bronze S3 bucket
    """
    try:
        logger.info(f"Reading Parquet data from: {bronze_path}")
        df = spark.read.parquet(bronze_path)
        record_count = df.count()
        logger.info(f"Successfully read {record_count} records from Bronze")
        return df
    
    except Exception as e:
        logger.error(f"Failed to read Bronze data from {bronze_path}: {str(e)}")
        raise

def transform_data(df):
    """
    Apply transformations to the DataFrame (cast types, handle nulls)
    """
    try:
        logger.info("Applying transformations to DataFrame")
        
        # Use 'transaction_amount' instead of 'amount' based on the error suggestion
        transformed_df = df \
            .withColumn("transaction_amount", col("transaction_amount").cast("decimal(10,2)").alias("transaction_amount")) \
            .withColumn("transaction_date", col("transaction_date").cast("date")) \
            .na.drop(subset=["transaction_amount", "transaction_date"])  # Drop rows with nulls in key columns
        
        # Add transformation metadata
        transformed_df = transformed_df \
            .withColumn("transformation_timestamp", current_timestamp()) \
            .withColumn("data_quality_check", lit("passed"))  # Placeholder for quality check
        
        logger.info("Transformations applied successfully")
        return transformed_df
    
    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}")
        raise
def write_to_silver(df, silver_path):
    """
    Write transformed DataFrame to the Silver S3 bucket in Parquet format
    """
    try:
        logger.info(f"Writing data to Silver S3: {silver_path}")
        df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .format("parquet") \
            .save(silver_path)
        
        logger.info(f"Successfully written data to {silver_path}")
        
        # Verify write operation
        verify_df = SparkSession.getActiveSession().read.parquet(silver_path)
        written_count = verify_df.count()
        logger.info(f"Verification: {written_count} records written to Silver")
        
    except Exception as e:
        logger.error(f"Failed to write data to Silver: {str(e)}")
        raise

def main():
    """
    Main execution function
    """
    spark = None
    
    try:
        logger.info("Starting Silver transformation job")
        
        # Initialize Spark session
        spark = create_spark_session()
        
        # Define S3 paths
        bronze_bucket = os.getenv("BRONZE_S3_BUCKET", "data-lakehouse-bronze-cwzr7tyw")
        silver_bucket = os.getenv("SILVER_S3_BUCKET", "data-lakehouse-silver-cwzr7tyw")
        bronze_path = f"s3a://{bronze_bucket}/transactions/"
        silver_path = f"s3a://{silver_bucket}/transactions/"
        
        # Read Bronze data
        df = read_bronze_data(spark, bronze_path)
        
        # Apply transformations
        transformed_df = transform_data(df)
        
        # Write to Silver
        write_to_silver(transformed_df, silver_path)
        
        logger.info("Silver transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        sys.exit(1)
        
    finally:
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")

if __name__ == "__main__":
    main()