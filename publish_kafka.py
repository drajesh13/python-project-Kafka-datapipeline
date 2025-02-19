import psycopg2
import json
from decimal import Decimal
from datetime import datetime
from confluent_kafka import Producer

# Database Configuration
DB_CONFIG = {
    "dbname": "Inventory_management",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Kafka Producer Configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(KAFKA_CONFIG)

def json_serializer(obj):
    """Convert non-serializable types (Decimal, datetime) to strings"""
    if isinstance(obj, Decimal):
        return str(obj)  # Convert Decimal to string
    elif isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to string
    return str(obj)  # Convert anything else to string as a fallback

def delivery_report(err, msg):
    """Kafka delivery report callback."""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_and_publish(query, topic):
    """Fetch data from PostgreSQL and publish to Kafka."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            data = json.dumps(row, default=json_serializer)  # Convert non-serializable data
            producer.produce(topic, key=str(row[0]), value=data, callback=delivery_report)
            producer.flush()  # Ensure message is sent immediately
        
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error:", e)

# Publish Only Emp_Department Data
print("=== Publishing Emp_Department Table Data to Kafka ===")
fetch_and_publish("SELECT * FROM Emp_department;", "emp_department_data")
