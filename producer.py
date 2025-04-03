import os
import time
import psycopg2
from kafka import KafkaProducer
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
print(KAFKA_BROKER)
KAFKA_TOPIC = "users_topic"

producer = KafkaProducer(
    # KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SSL",
    ssl_cafile="keys/ca.pem",
    ssl_certfile="keys/service.cert",
    ssl_keyfile="keys/service.key",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pg_conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
)
pg_cursor = pg_conn.cursor()

# Fetch all users
pg_cursor.execute("SELECT id, name, email FROM users;")
rows = pg_cursor.fetchall()

# Send to Kafka
for row in rows:
    message = {"id": row[0], "name": row[1], "email": row[2]}
    producer.send(KAFKA_TOPIC, message)
    print(f"Sent: {message}")

pg_cursor.close()
producer.flush()
print("All data sent to Kafka!")

"""
while True:
    pg_cursor.execute("SELECT * FROM user_changes ORDER BY change_id ASC")
    rows = pg_cursor.fetchall()

    for row in rows:
        change_data = {
            "change_id": row[0],
            "operation": row[1],
            "user_id": row[2],
            "name": row[3],
            "email": row[4],
            "change_time": str(row[5])
        }
        print(f"Sending to Kafka: {change_data}")
        producer.send("users_topic", value=change_data)
        pg_cursor.execute("DELETE FROM user_changes WHERE change_id = %s", (row[0],))
        pg_conn.commit()

    time.sleep(5)
"""