To create a folder through the terminal→ open the terminal→ enter these commands: 
cd ~/Documents
mkdir python-project
cd python-project

To create an empty Python file, touch fetch_data.py
Then install: pip install psycopg2  —-> psycopg2 is a Python library that allows us to connect to and interact with a PostgreSQL database.

The command below defines the PostgreSQL database connection.
DB_CONFIG = {
    "dbname": "your database",  # Name of your PostgreSQL database
    "user": "your_username",          # Your PostgreSQL username
    "password": "your_password",      # Your PostgreSQL password
    "host": "localhost",              # Use 'localhost' if running locally
    "port": "5432"                    # Default PostgreSQL port
}

Then, after moving the data into a single table, we can publish it in Kafka

To publish in Kafka, we need to install the services of Kafka and Zookeeper by using the commands: brew services start zookeeper
                   brew services start kafka

Then we need to create the Kafka topic for the table we are going to publish by command:
kafka-topics --create --topic “write topic name” --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Then write the required logic for publishing the data. After running the logic you can check the status by this command:
kafka-console-consumer --topic employee_data --from-beginning --bootstrap-server localhost:9092

Same as Publish we will write logic for Consume and check its subscribed. 

Instead of Running each file we can create this as a data pipe line by writing a logic in “main.py”
Once you run the main.py then the entire pipeline runs and updated the records.




