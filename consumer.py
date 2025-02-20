from confluent_kafka import Consumer
import psycopg2
import json

# PostgreSQL Database Configuration
DB_CONFIG = {
    "dbname": "Inventory_management",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": "5432"
}

# Kafka Consumer Configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",  
    "group.id": "emp_department_group",     
    "auto.offset.reset": "earliest"         
}

# Initialize Kafka Consumer
consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(["emp_department_data"])

# Function to insert aggregated data into Processed_Emp_Department
def update_processed_table():
    """Perform aggregations and update Processed_Emp_Department."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        aggregation_query = """
        INSERT INTO Processed_Emp_Department (
            department, total_employees, avg_salary, max_salary, min_salary, total_budget, last_updated
        )
        SELECT 
            department,
            COUNT(*) AS total_employees,
            ROUND(AVG(salary), 2) AS avg_salary,
            MAX(salary) AS max_salary,
            MIN(salary) AS min_salary,
            SUM(budget) AS total_budget,
            CURRENT_TIMESTAMP AS last_updated
        FROM Emp_department
        GROUP BY department
        ON CONFLICT (department) 
        DO UPDATE SET 
            total_employees = EXCLUDED.total_employees,
            avg_salary = EXCLUDED.avg_salary,
            max_salary = EXCLUDED.max_salary,
            min_salary = EXCLUDED.min_salary,
            total_budget = EXCLUDED.total_budget,
            last_updated = EXCLUDED.last_updated;
        """
        
        cursor.execute(aggregation_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Aggregated data updated in Processed_Emp_Department")

    except Exception as e:
        print("❌ Error updating Processed_Emp_Department:", e)

print("=== Listening for messages from Kafka ===")
try:
    while True:
        msg = consumer.poll(1.0)  # Poll messages

        if msg is None:
            continue  
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Decode and print the message
        message_value = msg.value().decode("utf-8")
        data = json.loads(message_value)  
        print(f"✅ Received: {data}")

        # Perform aggregations & store in Processed_Emp_Department
        update_processed_table()

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()  # Close the consumer
