from kafka import KafkaConsumer
import json

topic = 'news'
group_id = "test-news-group"
consumer = KafkaConsumer(bootstrap_servers='localhost:9092', group_id=group_id, api_version=(0, 10, 1))

def process_message(msg):
    try:
        data = msg.value.decode('utf-8')
        obj = json.loads(data)
        print("Received message on topic %s: %s" % (msg.topic + "-" + str(msg.partition), obj))
    except ValueError as e:
        # ignore messages that aren't JSON
        pass 

# Subscribe to the specified topic
consumer.subscribe([topic])


while True:
    messages = consumer.poll()
    for message in messages:
        process_message(message)
