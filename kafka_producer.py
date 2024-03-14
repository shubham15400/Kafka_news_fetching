from newsapi import NewsApiClient
import json
from kafka import KafkaProducer

# API Key for the NewsAPI.org account
key = "56ba1070c68b4258a0bb25638215c516"


newsapi = NewsApiClient(api_key=key) #  Initialize the client

# Define the list of media sources
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'

all_articles = newsapi.get_everything(q='Canada', #  Search query term
                                      sources=sources, # Adding all the sources
                                      language='en')    # Language parameter

# Create a Kafka producer to send messages to consumer via news-topic in localhost:9092
for article in all_articles['articles']:
    print(article)
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('shubham_news-topic', json.dumps(article).encode('utf-8'))