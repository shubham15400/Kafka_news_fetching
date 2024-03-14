import json
from kafka import KafkaConsumer
from datetime import datetime
import pandas as pd

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'

# Create Kafka Consumer instance
consumer = KafkaConsumer('shubham_news-topic', bootstrap_servers=bootstrap_servers)

# Define the keys to store in the DataFrame
selected_keys = ['author', 'title', 'description', 'content', 'publishedAt']

df_list = []

# Read messages from topic
for message in consumer:
    message_data = json.loads(message.value.decode('utf-8'))
    
    # Filter out unwanted data and create a new dictionary with selected keys only
    published_at_str = message_data.get('publishedAt', '')
    if published_at_str:
        try:
            published_at_dt = datetime.strptime(published_at_str, '%Y-%m-%dT%H:%M:%SZ')
            message_data['publishedAt'] = published_at_dt.timestamp()
        except ValueError as e:
            print(f"Error parsing publishedAt: {e}")
            continue

    # Extract selected key-value pairs
    filtered_data = {key: message_data.get(key, '') for key in selected_keys}
    print(filtered_data)
    # Create a DataFrame from the filtered data
    # Append DataFrame to df_list
    df_list.append(filtered_data)

    # Break out of loop after the messages are received.
    # The loop breaks when there is no more message available on the topic after the set time.
    if not consumer.poll(timeout_ms=6000):
        print('no messages available, stopping consumer')
        break
df = pd.DataFrame(df_list)
df = df.replace({',':''}, regex=True) # Remove commas from the content so as not to interfere with hive delimiter.
print(df)
df.to_csv('output.csv', index=False)
# Close consumer
consumer.close()

