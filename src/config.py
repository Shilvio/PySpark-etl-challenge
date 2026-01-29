import os
from dotenv import load_dotenv

# Load env variables fom .env
load_dotenv()

class Config:
    # Database
    DB_CONFIG = {
        "host": os.getenv("DB_HOST", "localhost"),
        "database": os.getenv("DB_NAME", "challenge_db"),
        "user": os.getenv("DB_USER", "user"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "port": os.getenv("DB_PORT", "5432")
    }

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = [os.getenv("KAFKA_BROKER", "localhost:9092")]
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")

    # CDC settings
    POLLING_INTERVAL = int(os.getenv("POLLING_INTERVAL", 5))
    BATCH_LIMIT = 100
    STATE_FILE = "cdc_state.json"
    
    # Spark settings
    SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "2")

    # Spark Business Logic settings
    TAX_RATE = float(os.getenv("TAX_RATE", 0.22))
    SPARK_APP_NAME = os.getenv("APP_NAME", "ETL")

TAX_RATE = Config.TAX_RATE
KAFKA_BOOTSTRAP_SERVERS = Config.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = Config.KAFKA_TOPIC
SPARK_APP_NAME = Config.SPARK_APP_NAME
SPARK_SHUFFLE_PARTITIONS = Config.SPARK_SHUFFLE_PARTITIONS