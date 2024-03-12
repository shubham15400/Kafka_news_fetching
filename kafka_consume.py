import csv
from kafka import KafkaConsumer
import json

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# Create Kafka Consumer instance
consumer = KafkaConsumer('news-topic', bootstrap_servers=bootstrap_servers)

# Define the CSV file path
csv_file_path = 'output.csv'

# Open the CSV file in write mode
with open(csv_file_path, 'w', newline='') as csvfile:
    # Create a CSV writer object
    csv_writer = csv.writer(csvfile)

    # Poll for new messages
    for messages in consumer:
        message = json.loads(messages.value.decode('utf-8'))

        # Extract and flatten the nested JSON structure
        flattened_data = {}

        def flatten(data, prefix=''):
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, dict):
                    flatten(value, new_key)
                else:
                    flattened_data[new_key] = value

        flatten(message)

        # Write header to the CSV file (if not already written)
        if not csvfile.tell():
            header = list(flattened_data.keys())
            csv_writer.writerow(header)

        # Write the row to the CSV file
        row = list(flattened_data.values())
        csv_writer.writerow(row)

        print(f"Received message: {message}")

# Close consumer
consumer.close()

print(f"CSV file '{csv_file_path}' has been created.")
