from newsapi import NewsApiClient
import json
from kafka import KafkaProducer

# Get your free API key from https://newsapi.org/, just need to sign up for an account
key = "56ba1070c68b4258a0bb25638215c516"

# Initialize api endpoint
newsapi = NewsApiClient(api_key=key)

# Define the list of media sources
sources = 'bbc-news,cnn,fox-news,nbc-news,the-guardian-uk,the-new-york-times,the-washington-post,usa-today,independent,daily-mail'

# /v2/everything
all_articles = newsapi.get_everything(q='Canada',
                                      sources=sources,
                                      language='en')

# Print the titles of the articles
for article in all_articles['articles']:
    print(article['title'])
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send('news', json.dumps(article['title']))