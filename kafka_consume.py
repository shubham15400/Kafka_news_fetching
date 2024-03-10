import psycopg2
from confluent_kafka import Consumer


# # Replace with your Kafka bootstrap server and topic names
# bootstrap_servers = "localhost:9092"
# topic_names = ["names", "values", "coins"]

# # Create a Kafka consumer
# kafka_consumer = Consumer({'bootstrap.servers': bootstrap_servers, 'group.id': "my_group"})

# # Subscribe to the topics
# kafka_consumer.subscribe(topic_names)

# # Connect to the PostgreSQL database
# conn = psycopg2.connect(database="api", user="postgres", password="HelloWorld", host="localhost", port="5432")
# cur = conn.cursor()

# # Function to insert data into the "customers" table
# def insert_data(name, value, coin):
#     cur.execute("INSERT INTO customers (name, value) VALUES (%s, %s)", (name, value))

def print_error(err):
    print("Error: {}".format(err))

def print_message(msg):
    print("topic = {}, partition = {}, offset = {}, key = {}, value = {}".format(
        msg.topic(), msg.partition(), msg.offset(),
        msg.key(), msg.value()))

def consume_messages(conf):
    consumer = Consumer(conf)
    try:
        consumer.subscribe([topic])
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print_error(msg.error())
            else:
                print_message(msg)
                consumer.commit(msg)
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        consumer.close()

topic = 'news-topic'
conf = {'bootstrap.servers': "localhost:9092", 'group.id': "News"}

