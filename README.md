<h1>ETL course project using News API</h1>
<h2>Description</h2>
In this project, the data is extracted from the API of https://newsapi.org/, and a Kafka producer sends the data through a topic
received by a Kafka consumer. The data is manipulated and stored in a comma-separated value(.csv) file. This file is stored in the local system. I use **Google Cloud Platform** for this project, which has hdfs and hive pre-installed.<be>
The CSV file is then stored in the hdfs folder. The file is later accessed by hive to where several aggregation functions are used.
