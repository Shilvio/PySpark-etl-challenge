import time
import json
import psycopg2
import os
from kafka import KafkaProducer
from datetime import datetime

# DB Configuration
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


def json_serializer(data):
    # Convert data to JSON string
    if isinstance(data, datetime):
        return data.isoformat()
    raise TypeError(f"Type {type(data)} not serializable")

def get_last_processed_time():
    # Reads last timestamp
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            return state.get('last_updated_at', '1970-01-01T00:00:00')
    return '1970-01-01T00:00:00'

def save_last_processed_time(timestamp):
    # Saves last checkpoint
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_updated_at': str(timestamp)}, f)

# CDC loop

def run_cdc():
    print("Connecting to Kafka")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        api_version=(2, 0, 0),
        value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
    )

    print("Connecting to database")
    conn = psycopg2.connect(**DB_CONFIG)
    
    print("Poller active, waiting for new data")
    
    try:
        while True:
            # gets last checkpoint
            last_time = get_last_processed_time()
            
            # finds new records
            cur = conn.cursor()
            query = """
                SELECT order_id, user_id, amount, status, created_at, updated_at
                FROM orders
                WHERE updated_at > %s
                ORDER BY updated_at ASC
                LIMIT 100;
            """
            cur.execute(query, (last_time,))
            rows = cur.fetchall()
            
            # processes new records
            if rows:
                print(f"found {len(rows)} new orders")
                
                latest_timestamp = last_time
                
                for row in rows:
                    
                    record = {
                        "order_id": row[0],
                        "user_id": row[1],
                        "amount": float(row[2]),
                        "status": row[3],
                        "created_at": row[4],
                        "updated_at": row[5]
                    }
                    
                    # Send to Kafka instance
                    producer.send(KAFKA_TOPIC, record)
                    
                    # Updates the latest timestamp
                    latest_timestamp = row[5]

                producer.flush()
                
                # Saves new checkpoint
                save_last_processed_time(latest_timestamp)
                print(f"New checkpoint saved, timestamp: {latest_timestamp}")
            
            cur.close()
            
            # Polling interval (5 seconds)
            time.sleep(POLLING_INTERVAL)

    except KeyboardInterrupt:
        print("\nCDC Poller stopped")
    finally:
        conn.close()
        producer.close()

if __name__ == "__main__":
    run_cdc()