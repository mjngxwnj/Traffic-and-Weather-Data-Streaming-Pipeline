#!/bin/bash

echo "Run Spark Streaming Job"
docker exec -it \
  spark-master bash -c "spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --packages \
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  spark_script/stream_job.py"