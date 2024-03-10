import requests
from confluent_kafka import Producer
from newsapi import NewsApiClient

# Replace with your Kafka bootstrap server
bootstrap_servers = "localhost:9092"

# Create a Kafka producer to send data
kafka_producer = Producer({'bootstrap.servers': bootstrap_servers})
def produce_message(topic, message):
    kafka_producer.produce(topic=topic, value=message.encode())

# Initialize API endpoint
newsapi = NewsApiClient(api_key="56ba1070c68b4258a0bb25638215c516")

# Define the list of media sources
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail,hindustan-times'

# /v2/everything - note there are other parameters you can set
all_articles = newsapi.get_everything(q='Canada', #  Search query term (optional)
                                      sources=sources, # List of identifiers (IDs) for the
                                      language='en') # The 2-letter ISO code of the language

# Print the titles of the articles
for article in all_articles['articles']:
    produce_message('news-topic', article['title'])

kafka_producer.flush()