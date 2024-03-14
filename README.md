<h1>ETL course project using News API</h1>
<h2>Description</h2>
In this project, the data is extracted from the API of https://newsapi.org/, and a Kafka producer sends the data through a topic
received by a Kafka consumer. The data is manipulated and stored in a comma-separated value(.csv) file. This file is stored in the local system. I use <strong>Google Cloud Platform</strong> for this project, which has HDFS and HIVE pre-installed.<br>
The CSV file is then stored in the hdfs folder. The file is later accessed by HIVE, where several aggregation functions are used.<br>
<h2>HIVE table</h2>
The hive table stores the columns from the HDFS folder's CSV file. The following is the query to create the table:<br>
