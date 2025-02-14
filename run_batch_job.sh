#!/bin/bash

echo "Run Spark Batch Job at $(date)"
docker exec \
  spark-master bash -c "spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 1 \
  --packages \
    org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1 \
  spark_script/batch_job.py"
