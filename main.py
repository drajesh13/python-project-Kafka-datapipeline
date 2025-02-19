import subprocess
import time

def start_process(command):
    """Start a subprocess and return the process"""
    return subprocess.Popen(command, shell=True)

if __name__ == "__main__":
    print("Starting the Data Pipeline...\n")

    # Start Kafka Producer (publish data)
    producer_process = start_process("python publish_kafka.py")
    time.sleep(5)  # Give Kafka some time to receive messages

    # Start Kafka Consumer (consume and analyze data)
    consumer_process = start_process("python consumer.py")

    try:
        # Keep the script running until processes are complete
        producer_process.wait()
        consumer_process.wait()
    except KeyboardInterrupt:
        print("\n Stopping the Data Pipeline...")
        producer_process.terminate()
        consumer_process.terminate()
