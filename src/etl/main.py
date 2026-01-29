import sys
import os
import logging
from pyspark.sql import SparkSession
import src.config as global_config          
import src.etl.settings as spark_settings 
import src.etl.schemas as schemas
import src.etl.transformations as transformations

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETLlogger")

def create_session():
    return SparkSession.builder \
        .appName(spark_settings.APP_NAME) \
        .config("spark.jars.packages", spark_settings.SPARK_PACKAGES) \
        .config("spark.sql.shuffle.partitions", spark_settings.SHUFFLE_PARTITIONS) \
        .getOrCreate()

def run_pipeline():

    spark = create_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Pipeline started, listening to: {spark_settings.KAFKA_TOPIC}")

    # Data Ingestion
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", spark_settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", spark_settings.KAFKA_TOPIC) \
        .option("startingOffsets", spark_settings.STARTING_OFFSETS) \
        .load()

    # Apply Transform
    parsed_df = transformations.parse_kafka_message(raw_stream, schemas.get_schema())
    
    # Applichiamo la business logic
    final_df = transformations.apply_business_logic(parsed_df)

    # Load sink
    logger.info("streaming starting")
    
    query = final_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_pipeline()