import csv
from kafka import KafkaConsumer
import json

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# Create Kafka Consumer instance
consumer = KafkaConsumer('news-topic', bootstrap_servers=bootstrap_servers)

# Define the CSV file path
csv_file_path = 'output.csv'

# Specify the keys you want to store in the CSV
selected_keys = ['author', 'title', 'description', 'content', 'publishedAt']  # Column names to be extracted

# Open the CSV file in write mode
with open(csv_file_path, 'w', newline='') as csvfile:
    # Create a CSV writer object
    csv_writer = csv.writer(csvfile)

    # Write header to the CSV file
    csv_writer.writerow(selected_keys)

    # Poll for new messages
    for messages in consumer:
        message = json.loads(messages.value.decode('utf-8'))

        # Extract selected key-value pairs
        row = [message.get(key, '') for key in selected_keys]

        # Write the row to the CSV file
        csv_writer.writerow(row)

        print(f"Received message: {message}")

# Close consumer
consumer.close()

print(f"CSV file '{csv_file_path}' has been created.")
