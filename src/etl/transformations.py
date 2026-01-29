from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, current_timestamp, when
from pyspark.sql.types import TimestampType
import src.config as config

def parse_kafka_message(df: DataFrame, schema) -> DataFrame:
    # decode JSON from binaries to kafka
    return df.selectExpr("CAST(value AS STRING)") \
             .select(from_json(col("value"), schema).alias("data")) \
             .select("data.*")

def apply_business_logic(df: DataFrame) -> DataFrame:
    # exaple of transformation business logic
    return df \
        .withColumn("event_timestamp", col("created_at").cast(TimestampType())) \
        .filter(col("amount").isNotNull() & (col("amount") > 0)) \
        .withColumn("tax_amount", col("amount") * config.TAX_RATE) \
        .withColumn("total_amount", col("amount") * (1 + config.TAX_RATE)) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn(
            "is_valid_sale", 
            when(col("status") == "COMPLETED", True).otherwise(False)
        )