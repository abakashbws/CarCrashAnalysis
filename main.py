# Import necessary modules

from pyspark.sql import SparkSession
import logging
from CarCrashAnalysis.analysis import Analysis


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Car Crash Analysis") \
        .getOrCreate()
    
    # Path to your config.json in the resources directory
    config_path = "./config/config.json"
    
    # Initialize and execute analysis
    analysis = Analysis(spark, config_path)
    analysis.start()
    
    # Stop the Spark session
    spark.stop()
