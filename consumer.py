from confluent_kafka import Consumer
import json

# Kafka Consumer Configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",  # Kafka broker address
    "group.id": "emp_department_group",     # Consumer group ID
    "auto.offset.reset": "earliest"         # Start reading from the beginning if no offset is found
}

consumer = Consumer(KAFKA_CONFIG)  # Create Kafka consumer
consumer.subscribe(["emp_department_data"])  # Subscribe to topic

print("=== Listening for messages from Kafka ===")
try:
    while True:
        msg = consumer.poll(1.0)  # Poll messages (timeout = 1 second)

        if msg is None:
            continue  # No message received, continue polling
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Decode and print the message
        message_value = msg.value().decode("utf-8")
        data = json.loads(message_value)  # Convert JSON string to Python object
        print(f"✅ Received: {data}")

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()  # Close the consumer
