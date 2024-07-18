CDC implementation using Apache Flink and Debezium. Includes logic to reconstruct Database transactions. Assumes the CDC data messages are on a single partitioned Kafka topic. 


Command to run the artifact in Flink: 
flink run build/libs/cdc-via-flink-1.0-SNAPSHOT.jar