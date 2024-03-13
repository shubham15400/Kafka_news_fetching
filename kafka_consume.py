import json
from kafka import KafkaConsumer
from datetime import datetime

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# Create Kafka Consumer instance
consumer = KafkaConsumer('news-topic', bootstrap_servers=bootstrap_servers)

# Define the JSON file path
json_file_path = 'output.json'

# Specify the keys you want to store in the JSON
selected_keys = ['author', 'title', 'description', 'content', 'publishedAt']  # Keys to be extracted

# Open the JSON file in write mode
with open(json_file_path, 'w') as jsonfile:
    # Poll for new messages
    for message in consumer:
        message_data = json.loads(message.value.decode('utf-8'))

        published_at_str = message_data.get('publishedAt', '')
        if published_at_str:
            try:
                published_at_dt = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
                message_data['publishedAt'] = published_at_dt.timestamp()
            except ValueError as e:
                print(f"Error parsing publishedAt: {e}")
                # Optionally, you can handle the error or skip this message
                continue

        # Extract selected key-value pairs
        filtered_data = {key: message_data.get(key, '') for key in selected_keys}

        # Write the data to the JSON file
        json.dump(filtered_data, jsonfile)
        jsonfile.write('\n')  # Write a new line for each JSON object

        print(f"Received message: {filtered_data}")

# Close consumer
consumer.close()