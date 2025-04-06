This project sets up a PostgreSQL database, Kafka topic, producer, and consumer using AWS services. The goal is to stream data from a database table to Kafka in real time.

Features

PostgreSQL Setup: Creates a table and inserts bulk test data.

Kafka Producer: Sends data to a Kafka topic.

Kafka Consumer: Reads messages from Kafka.

Environment Handling: Uses .env to manage secrets securely.

Bulk Inserts: Uses COPY for efficient data loading.

Debezium Setup: Pending database change capture (CDC).


## **Screenshots**
### **Kafka Producer Running**
![Kafka Producer](screenshots_of_progress/progress_kafka.PNG)

### Kafka Consumer Receiving Messages**
![Kafka Consumer](screenshots_of_progress/progress_kafka.PNG)

### Console Showing Services**
![Console](screenshots_of_progress/progress_kafka_aiven_console.PNG)

Debezium Connector is Running

Kafka Consumer Receiving Messages
This screenshot shows that the Kafka Consumer is receiving messages from the Kafka topic (dbserver1.analytics.events).

Console Showing Kafka Connect Services
![Console](screenshots_of_progress/check_kafka_manual_consumer.PNG)

Database Change Capture 
This screenshot shows that Debezium is successfully capturing changes from the database and pushing them to the corresponding Kafka topic.
![Console](screenshots_of_progress/connector_topc_log.PNG)
