from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = "users-topic"

# Create Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SSL",
    ssl_cafile="keys/ca.pem",
    ssl_certfile="keys/service.cert",
    ssl_keyfile="keys/service.key",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print(f"Listening to Kafka topic '{KAFKA_TOPIC}'...")

for message in consumer:
    print(f"Received from Kafka: {message.value}")
