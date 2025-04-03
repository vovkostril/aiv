import psycopg2
import csv
import json
import os
from faker import Faker
from kafka import KafkaProducer, KafkaConsumer
from dotenv import load_dotenv
import db_script
import producer
import consumer
import time
import random

# nvironment variables .env
load_dotenv()

def insert_test_messages_user():
    conn = db_script.get_db_connection()
    if conn is None:
        return

    cursor = conn.cursor()
    random_id = random.randint(1000, 9999)  # Generate random user ID
    test_name = f"TestUser{random_id}"  # Generate random user unique Name
    test_email = f"TestUserEmail{random_id}@example.com"

    try:
        cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id;", (test_name, test_email))
        user_id = cursor.fetchone()[0]
        conn.commit()
        print(f"Inserted test user with ID {user_id}")
    except psycopg2.Error as e:
        print(f"Error inserting user: {e}")
    finally:
        cursor.close()
        conn.close()


# Main execution
if __name__ == "__main__":
    # db_script.create_table()  # Ensure table exists
    # db_script.generate_csv()  # Generate CSV
    # db_script.bulk_insert_from_csv()  # Bulk Insert
    insert_test_messages_user() # Incerting one user with random name 
    time.sleep(2)
    producer.produce_messages()  # Produce Kafka messages
    time.sleep(2)
    consumer.consume_messages()  # Consume Kafka messages