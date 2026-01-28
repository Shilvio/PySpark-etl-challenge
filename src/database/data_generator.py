import time
import random
import psycopg2
from faker import Faker
from datetime import datetime

# Db config
DB_HOST = "localhost"
DB_NAME = "challenge_db"
DB_USER = "user"
DB_PASS = "password"
DB_PORT = "5432"

fake = Faker()

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"DB connection error: {e}")
        return None

def generate_order(conn):
    cur = conn.cursor()
    
    # Simulated data using faker
    user_id = fake.user_name()
    product_id = f"PROD-{random.randint(100, 999)}"
    amount = round(random.uniform(10.0, 500.0), 2)
    status = random.choice(['PENDING', 'COMPLETED', 'CANCELLED'])
    
    query = """
        INSERT INTO orders (user_id, product_id, amount, status, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        RETURNING order_id;
    """
    
    cur.execute(query, (user_id, product_id, amount, status))
    new_id = cur.fetchone()[0]
    
    conn.commit()
    cur.close()
    print(f"[INSERT] Order #{new_id} created fot {amount}€ (User: {user_id})")

def main():
    print("Waiting for db to be ready . . .")
    time.sleep(5) 
    
    conn = get_db_connection()
    if not conn:
        return

    print("Faker data generator active, Press CTRL + C to stop")
    try:
        while True:
            generate_order(conn)
            # simulates random interval for generating orders (random range from 0.5 to 2 seconds)
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\n Faker data generator stopped")
    finally:
        conn.close()

if __name__ == "__main__":
    main()