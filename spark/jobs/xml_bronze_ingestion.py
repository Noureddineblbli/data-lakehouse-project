import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main Spark job logic."""
    spark = None
    try:
        logger.info("Initializing Spark session...")
        # The session now inherits all configurations from the SparkSubmitOperator
        spark = SparkSession.builder \
            .appName("XMLBronzeIngestion") \
            .getOrCreate()
        logger.info("Spark session created.")

        # Corrected path inside the Docker container
        input_path = "/opt/data/weather.xml"
        logger.info(f"Reading XML data from {input_path}...")
        
        # Read the XML file into a DataFrame
        df = spark.read \
            .format("xml") \
            .option("rowTag", "observation") \
            .load(input_path)
        
        logger.info("XML data loaded successfully. Schema:")
        df.printSchema()

        # Add ingestion date for the bronze layer
        df_with_ingestion_date = df.withColumn("ingestion_date", current_date())

        # Define the output path in the shared staging volume
        output_path = "/staging/weather_bronze"
        logger.info(f"Writing DataFrame to temporary Parquet path: {output_path}")
        df_with_ingestion_date.write.mode("overwrite").parquet(output_path)
        logger.info("Temporary write to staging successful!")

    except Exception as e:
        logger.error(f"An error occurred during the Spark job: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()