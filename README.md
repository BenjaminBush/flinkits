# flinkits
A model of an intelligent transportation system (ITS) using Flink and Kafka. Performs traffic flow prediction using a pretrained LSTM network, then calculates the optimal path from a start to goal location using a A* search. 

This is the evolution of https://github.com/BenjaminBush/nsq-spark-receiver 


# How to Build
Run a maven clean install in the root directory: `mvn clean install`


This will generate a jar file in the `flinkits/target/` folder with the name `flinkits-0.1.jar`. This jar can then be submitted to a Flink cluster. 


# How to Submit JAR to Flink Cluster
1. Make sure you grab a copy of `flink_config.properties` and modify any important information (such as the path the the h5 file of the stored LSTM, Kafka input/output topics and ports, etc)
2. Run:
`./bin/flink run flinkits-0.1.jar --config flink_config.properties`
3. WAIT a minute or two until the Flink Web UI shows the Job actually running. This isually takes a few seconds. Once the Job is running, you can run the producers/consumers from <https://github.com/BenjaminBush/research/kafka_scripts/>
