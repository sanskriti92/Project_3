# Kafka_Spark_Streaming
## Project Description
This project aims to read the streaming data and process it and store it in a parquet file. 

## Technologies Used
- Hadoop - version 3.2.2
- Apache Spark - version 3.1.2
- Apache Kafka - version 2.8.0
- Apache Zookeeper - version 3.5.9
- Python - version 3.8.10

## Features
List of features :
- Producer program can generate 10 million distinct records randomly.
- Consumer program reads the live streaming data and process it.
- Consumer program categorizes the data and pushes it to the respective topics.

To-do list :
- More processing has to be done

## Getting Started
- Clone the project
```
$ git clone https://github.com/sanskriti92/Project_3
```
- Install python from [here](https://www.python.org/downloads/).
- Install Apache Hadoop from [here](https://hadoop.apache.org/releases.html). Refer [this](https://phoenixnap.com/kb/install-hadoop-ubuntu) for installation.
- Install Apache Spark from [here](https://spark.apache.org/downloads.html). Refer [this](https://sparkbyexamples.com/spark/spark-installation-on-linux-ubuntu/) for installation.
- Install Apache Kafka from [here](https://kafka.apache.org/downloads). Refer [this](https://www.tutorialkart.com/apache-kafka/install-apache-kafka-on-ubuntu/) for installation.
- Install pyspark module
```
$ pip install pyspark
```
- Install kafka-python module 
```
$ python3 -m pip install kafka-python
```
- Set environment variable in .bashrc file.
```
$ sudo vi ~/.bashrc
ADD "export KAFKA_HOME=/home/hdoop/kafka" at the end of the file
$ source ~/.bashrc
```


## Usage
1. Start your linux system and start the dfs and yarn servers.
```
$ start-all.sh
```
2. Start the zookeeper server in one terminal and and kafka server in another terminal.
```
$ cd $KAFKA_HOME
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```
```
$ cd $KAFKA_HOME
$ bin/kafka-server-start.sh config/server.properties
```
3. Create a kafka topic with any name.(Remember to change the topic name in code)
```
$ $KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper localhost:2181 --topic project3 --replication-factor 1 --partitions 1
```
4. Run consumer.
```
$ $KAFKA_HOME/bin/kafka-console-consumer.sh --topic project3 --bootstrap-server localhost:9092 --from-beginning
```
5. Open another terminal and run producer.py file.
```
$ python3 producer.py
```
6. Open another terminal and run processing.py or category.py file.
```
$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 processing.py
```
