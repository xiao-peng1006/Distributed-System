# Distributed System

## Getting Started

Follow the stpes in Installing section to setup the environment for development. Some builds is required for the application to function correctly. More information in Build section.

### Prerequisites

This project is worked on ubuntu version 18.04...

### Installing

A step by step series of examples that tell you how to get a development env running

Install docker
```
sudo apt-get install docker
```

Install pyspark
```
pip3 instal pyspark
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Start a Zookeeper Container
```
docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper
```

Start a Kafka Container
```
docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=172.17.0.3 -e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka
```

Start Spark Streaming
```
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 data-stream.py analyzer average-price 127.0.0.1:9092 5
```

Start a Redis Container
```
docker run -d -p 6379:6379 --name redis redis:alpine
```
