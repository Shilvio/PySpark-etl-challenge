import sys
import os
import shutil
import logging
from pyspark.sql import SparkSession
import src.config as global_config          
import src.etl.settings as spark_settings 
import src.etl.schemas as schemas
import src.etl.transformations as transformations
from kafka import KafkaConsumer

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ETLlogger")

def wait_for_kafka_topic(topic_name, bootstrap_servers, max_retries=20):
    # Wait until kafka responds
    logger.info(f"waiting for the topic '{topic_name}' to be created")
    
    retries = 0
    while retries < max_retries:
        try:
            # reads Kafka metadata
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=1000
            )
            # Ottieni la lista dei topic esistenti
            available_topics = consumer.topics()
            
            if topic_name in available_topics:
                logger.info(f"Topic '{topic_name}' found, activating streaming")
                consumer.close()
                return True
            
            consumer.close()
        except NoBrokersAvailable:
            logger.warning("Kafka unavailable, retry")
        except Exception as e:
            logger.warning(f"Error checking topic: {e}")

        time.sleep(5) # Aspetta 5 secondi prima di riprovare
        retries += 1
        logger.info(f"Retry number: {retries}/{max_retries}: Topic not present")

    raise TimeoutError(f"topic {topic_name} not ready afrter {max_retries*5} seconds.")


def create_session():
    return SparkSession.builder \
        .appName(spark_settings.APP_NAME) \
        .config("spark.jars.packages", spark_settings.SPARK_PACKAGES) \
        .config("spark.sql.shuffle.partitions", spark_settings.SHUFFLE_PARTITIONS) \
        .getOrCreate()

def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    
    print(f"writing batch {batch_id} on destination PostgresDB")
    
    # Scrittura standard JDBC (Modalità Append)
    batch_df.write \
        .format("jdbc") \
        .option("url", spark_settings.DEST_DB_CONFIG["url"]) \
        .option("dbtable", spark_settings.DEST_DB_CONFIG["table"]) \
        .option("user", spark_settings.DEST_DB_CONFIG["user"]) \
        .option("password", spark_settings.DEST_DB_CONFIG["password"]) \
        .option("driver", spark_settings.DEST_DB_CONFIG["driver"]) \
        .mode("append") \
        .save()

def run_pipeline():
    #waits for kafka to be ready
    wait_for_kafka_topic(spark_settings.KAFKA_TOPIC, spark_settings.KAFKA_BOOTSTRAP_SERVERS)

    spark = create_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Pipeline started, listening to: {spark_settings.KAFKA_TOPIC}")

    # Data Ingestion
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", spark_settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", spark_settings.KAFKA_TOPIC) \
        .option("startingOffsets", spark_settings.STARTING_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load()
    
    checkpoint_dir = "/tmp/checkpoint_orders"

    # Apply Transform
    parsed_df = transformations.parse_kafka_message(raw_stream, schemas.get_schema())
    
    # Apply business logic
    final_df = transformations.apply_business_logic(parsed_df)

    # Load sink
    logger.info("streaming starting")
    
    try:
        shutil.rmtree(checkpoint_dir)
    except FileNotFoundError:
        pass

    query = final_df.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_dir) \
        .foreachBatch(write_to_postgres) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_pipeline()