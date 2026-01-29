import time
import json
import psycopg2
import os
import logging
from kafka import KafkaProducer
from datetime import datetime
from src.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("CDC_Poller")

# Configuration Constants
DB_CONFIG = {
    "host": "localhost",
    "database": "challenge_db",
    "user": "user",
    "password": "password"
}

KAFKA_TOPIC = "orders"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
STATE_FILE = "cdc_state.json"
POLLING_INTERVAL = 5
BATCH_LIMIT = 100

class StateManager:
    
    # Handles reading and writing the CDC state (checkpoint) to a file.
    def __init__(self, filepath):
        self.filepath = filepath

    def get_last_processed_time(self):
        # Returns default timestamp if file doesn't exist
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, 'r') as f:
                    state = json.load(f)
                    return state.get('last_updated_at', '1970-01-01T00:00:00')
            except Exception as e:
                logger.error(f"Failed to read state file: {e}")
        return '1970-01-01T00:00:00'

    def save_last_processed_time(self, timestamp):
        try:
            with open(self.filepath, 'w') as f:
                json.dump({'last_updated_at': str(timestamp)}, f)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")

class CDCService:
    
    # Main service to poll Database and push changes to Kafka
    def __init__(self):
        self.state_manager = StateManager(Config.STATE_FILE)
        self.producer = self._init_kafka_producer()

    def _init_kafka_producer(self):
        try:
            return KafkaProducer(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                api_version=(2, 0, 0),
                value_serializer=lambda v: json.dumps(v, default=self._json_serializer).encode('utf-8')
            )
        except Exception as e:
            logger.critical(f"Failed to connect to Kafka: {e}")
            raise

    @staticmethod
    def _json_serializer(data):
        if isinstance(data, datetime):
            return data.isoformat()
        raise TypeError(f"Type {type(data)} not serializable")

    def _get_db_connection(self):
        return psycopg2.connect(**Config.DB_CONFIG)

    def fetch_and_process(self):
        conn = None
        try:
            conn = self._get_db_connection()
            last_time = self.state_manager.get_last_processed_time()

            with conn.cursor() as cur:
                # Select records updated after the last checkpoint
                query = """
                    SELECT order_id, user_id, amount, status, created_at, updated_at
                    FROM orders
                    WHERE updated_at > %s
                    ORDER BY updated_at ASC
                    LIMIT %s;
                """
                cur.execute(query, (last_time, Config.BATCH_LIMIT))
                rows = cur.fetchall()

                if rows:
                    logger.info(f"Found {len(rows)} new records updated after {last_time}")
                    self._push_to_kafka(rows)
                
        except Exception as e:
            logger.error(f"Error in polling loop: {e}")
        finally:
            if conn:
                conn.close()

    def _push_to_kafka(self, rows):
        latest_timestamp = None

        for row in rows:
            record = {
                "order_id": row[0],
                "user_id": row[1],
                "amount": float(row[2]),
                "status": row[3],
                "created_at": row[4],
                "updated_at": row[5]
            }

            self.producer.send(Config.KAFKA_TOPIC, record)
            latest_timestamp = row[5]

        self.producer.flush()
        
        if latest_timestamp:
            self.state_manager.save_last_processed_time(latest_timestamp)
            logger.info(f"Batch processed. New checkpoint: {latest_timestamp}")

    def run(self):
        logger.info("CDC Service started. Waiting for data...")
        try:
            while True:
                self.fetch_and_process()
                time.sleep(Config.POLLING_INTERVAL)
        except KeyboardInterrupt:
            logger.info("CDC Service stopping...")
            self.producer.close()
            logger.info("Stopped.")

if __name__ == "__main__":
    service = CDCService()
    service.run()