# flinkits
A model of an intelligent transportation system (ITS) using Flink and Kafka

This is basically a clone of https://github.com/BenjaminBush/nsq-spark-receiver with a few modifications. First and foremost, we use Apache Flink as opposed to Apache Spark Streaming for true stream processing. Additionally, we use Kafka as our messaging middleware. Some versions of the NSQ-Spark-Receiver have a Kafka implementation, but most have NSQ. 


# How to Build
Run a maven clean install in the root directory: `mvn clean install`


This will generate a jar file in the `flinkits/target/` folder with the name `flinkits-0.1.jar`. This jar can then be submitted to a Flink cluster. 
