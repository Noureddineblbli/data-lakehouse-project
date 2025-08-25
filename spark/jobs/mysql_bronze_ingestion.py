import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_spark_session():
    """
    Initialize Spark session with MySQL connector and S3 configurations
    """
    try:
        spark = SparkSession.builder \
            .appName("MySQL_Bronze_Ingestion") \
            .config("spark.jars", "/opt/airflow/jars/mysql-connector-j-8.0.33.jar,/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar") \
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

def get_mysql_connection_properties():
    """
    Get MySQL connection properties from environment variables
    """
    try:
        properties = {
            "user": os.getenv("MYSQL_USER", "sales_user"),
            "password": os.getenv("MYSQL_PASSWORD", "salespass123"),
            "driver": "com.mysql.cj.jdbc.Driver",
            "useSSL": "false",
            "allowPublicKeyRetrieval": "true",
            "serverTimezone": "UTC"
        }
        
        # MySQL connection URL
        mysql_host = os.getenv("MYSQL_HOST", "mysql")  # Docker service name
        mysql_port = os.getenv("MYSQL_PORT", "3306")
        mysql_database = os.getenv("MYSQL_DATABASE", "sales")
        
        jdbc_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_database}"
        
        logger.info(f"MySQL connection configured for: {mysql_host}:{mysql_port}/{mysql_database}")
        return jdbc_url, properties
    
    except Exception as e:
        logger.error(f"Failed to configure MySQL connection: {str(e)}")
        raise

def read_mysql_table(spark, jdbc_url, properties, table_name):
    """
    Read data from MySQL table
    """
    try:
        logger.info(f"Reading data from MySQL table: {table_name}")
        
        df = spark.read \
            .jdbc(url=jdbc_url, 
                  table=table_name, 
                  properties=properties)
        
        record_count = df.count()
        logger.info(f"Successfully read {record_count} records from {table_name}")
        
        # Log schema for verification
        logger.info("DataFrame Schema:")
        df.printSchema()
        
        # Add ingestion metadata
        df_with_metadata = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_system", lit("mysql")) \
            .withColumn("source_table", lit(table_name))
        
        return df_with_metadata
    
    except Exception as e:
        logger.error(f"Failed to read from MySQL table {table_name}: {str(e)}")
        raise

def write_to_s3_bronze(df, s3_path, partition_columns=None):
    """
    Write DataFrame to S3 Bronze layer in Parquet format
    """
    try:
        logger.info(f"Writing data to S3 Bronze: {s3_path}")
        
        write_operation = df.write \
            .mode("overwrite") \
            .option("compression", "snappy") \
            .format("parquet")
        
        # Add partitioning if specified
        if partition_columns:
            write_operation = write_operation.partitionBy(*partition_columns)
            logger.info(f"Data will be partitioned by: {partition_columns}")
        
        write_operation.save(s3_path)
        
        logger.info(f"Successfully written data to {s3_path}")
        
        # Verify write operation
        verify_df = SparkSession.getActiveSession().read.parquet(s3_path)
        written_count = verify_df.count()
        logger.info(f"Verification: {written_count} records written to S3")
        
    except Exception as e:
        logger.error(f"Failed to write data to S3: {str(e)}")
        raise

def main():
    """
    Main execution function
    """
    spark = None
    
    try:
        logger.info("Starting MySQL to Bronze S3 ingestion job")
        
        # Initialize Spark session
        spark = create_spark_session()
        
        # Get MySQL connection details
        jdbc_url, properties = get_mysql_connection_properties()
        
        # Read data from MySQL
        table_name = "transactions"
        df = read_mysql_table(spark, jdbc_url, properties, table_name)
        
        # Show sample data for verification
        logger.info("Sample data preview:")
        df.show(5, truncate=False)
        
        # Configure S3 Bronze path
        bronze_bucket = os.getenv("BRONZE_S3_BUCKET", "data-lakehouse-bronze-cwzr7tyw")
        s3_path = f"s3a://{bronze_bucket}/transactions/"
        
        # Write to S3 Bronze layer
        # Optionally partition by transaction_date (you can modify this based on your needs)
        write_to_s3_bronze(df, s3_path)
        
        logger.info("MySQL to Bronze S3 ingestion completed successfully")
        
        # Optional: Show final statistics
        final_df = spark.read.parquet(s3_path)
        logger.info(f"Final record count in Bronze: {final_df.count()}")
        
    except Exception as e:
        logger.error(f"Job failed with error: {str(e)}")
        sys.exit(1)
        
    finally:
        # Clean up resources
        if spark:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {str(e)}")

if __name__ == "__main__":
    main()