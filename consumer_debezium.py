from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER_1")
KAFKA_TOPIC = "dbserver1.analytics.events"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    security_protocol="SSL",
    ssl_cafile="keys/ca_1.pem",
    ssl_certfile="keys/service_1.cert",
    ssl_keyfile="keys/service_1.key",
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print("Received change event:", message.value)
