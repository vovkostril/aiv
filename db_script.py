import psycopg2
import os
from dotenv import load_dotenv
import random
from faker import Faker
import csv

load_dotenv()

DB_PARAMS = {
    "DB_NAME" : os.getenv("DB_NAME"),
    "DB_USER" : os.getenv("DB_USER"),
    "DB_PASSWORD" : os.getenv("DB_PASSWORD"),
    "DB_HOST" : os.getenv("DB_HOST"),
    "DB_PORT" : os.getenv("DB_PORT")
}

def get_db_connection():
    """Establish a connection to PostgreSQL service given."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL service: {e}")
        return None

def create_tables():
    conn = get_db_connection()
    if conn is None:
        return

    cursor = conn.cursor()
    
    queries = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS user_changes (
            change_id SERIAL PRIMARY KEY,
            operation TEXT NOT NULL,
            user_id INT NOT NULL,
            name TEXT,
            email TEXT,
            change_time TIMESTAMP DEFAULT now()
        );
        """,
        """
        CREATE OR REPLACE FUNCTION log_user_changes() RETURNS TRIGGER AS $$
        BEGIN
            IF (TG_OP = 'INSERT') THEN
                INSERT INTO user_changes(operation, user_id, name, email)
                VALUES ('INSERT', NEW.id, NEW.name, NEW.email);
            ELSIF (TG_OP = 'UPDATE') THEN
                INSERT INTO user_changes(operation, user_id, name, email)
                VALUES ('UPDATE', NEW.id, NEW.name, NEW.email);
            ELSIF (TG_OP = 'DELETE') THEN
                INSERT INTO user_changes(operation, user_id)
                VALUES ('DELETE', OLD.id);
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        """,
        """
        CREATE TRIGGER user_changes_trigger
        AFTER INSERT OR UPDATE OR DELETE ON users
        FOR EACH ROW EXECUTE FUNCTION log_user_changes();
        """
    ]
    
    for query in queries:
        cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()
    print("Tables and triggers set up successfully!")

# 
# Generate fake user email data and save to CSV
def generate_csv(num_rows=1000):
    fake = Faker()
    with open(f"data.csv", mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "email"])  
        for _ in range(num_rows):
            writer.writerow([fake.name(), fake.email()])

def insert_user(name, email):
    """Insert a new user into the users table."""
    conn = get_db_connection()
    if conn is None:
        return

    cursor = conn.cursor()
    
    try:
        cursor.execute("INSERT INTO users (name, email) VALUES (%s, %s) RETURNING id;", (name, email))
        user_id = cursor.fetchone()[0]
        conn.commit()
        print(f"User {name} added with ID {user_id}")
    except psycopg2.Error as e:
        print(f"Error inserting user: {e}")
    
    cursor.close()
    conn.close()

def fetch_user_changes():
    """Fetch user changes from PostgreSQL."""
    conn = get_db_connection()
    if conn is None:
        return []

    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM user_changes ORDER BY change_id ASC")
    rows = cursor.fetchall()
    
    changes = []
    for row in rows:
        changes.append({
            "change_id": row[0],
            "operation": row[1],
            "user_id": row[2],
            "name": row[3],
            "email": row[4],
            "change_time": str(row[5])
        })

    cursor.close()
    conn.close()
    
    return changes

# Bulk insert fake data using COPY
def bulk_insert(file_path="data.csv"):
    conn = get_db_connection()
    if conn is None:
        return
    
    cursor = conn.cursor()
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            cursor.copy_expert("COPY users(name, email) FROM STDIN WITH CSV HEADER", file)
        conn.commit()
        print("Bulk insert completed!")
    except psycopg2.Error as e:
        print(f"Error bulk inserting users: {e}")
    finally:
        cursor.close()
        conn.close()
