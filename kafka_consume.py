from kafka import KafkaConsumer
import json

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# Create Kafka Consumer instance
consumer = KafkaConsumer('bishesh', bootstrap_servers=bootstrap_servers)

# Poll for new messages
for messages in consumer:
    message = json.loads(messages.value.decode('utf-8'))
    print(f"Received message: {message}")

# Close consumer
consumer.close()