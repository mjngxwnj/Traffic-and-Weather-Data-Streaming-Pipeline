from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (from_json, dayofweek, dayofmonth, 
                                   month, year, date_format, hour, when, col)
from utils_spark import (get_SparkSession, 
                         convert_kafka_data_to_df, 
                         generate_traffic_features,
                         generate_weather_features)


""" Function to process traffic data from kafka"""
def process_kafka_traffic_data(spark: SparkSession,
                               kafka_server: str = 'kafka1:19092', 
                               topic: str = 'traffic_data') -> DataFrame:
    """
    Args:
        spark: SparkSession object
        kafka_server: kafka server url
        topic: kafka topic
    Returns:
        traffic_query: spark streaming query
    """

    #check params
    if not isinstance(spark, SparkSession):
        raise ValueError("spark should be a SparkSession object")
    
    #readstream data from kafka
    traffic_df = spark.readStream \
                      .format("kafka") \
                      .option("kafka.bootstrap.servers", kafka_server) \
                      .option("subscribe", topic) \
                      .option("startingOffsets", "earliest") \
                      .load()

    #parse json data
    traffic_df = convert_kafka_data_to_df(traffic_df, "traffic")
        
    #rename columns
    traffic_df = traffic_df.withColumnRenamed("traffic light", "traffic_light") \
                           .withColumnRenamed("stop sign", "stop_sign")
    
    #generate traffic features
    traffic_df = generate_traffic_features(traffic_df)

    #add execution hour
    traffic_df = traffic_df.withColumn("execution_hour", hour("execution_time")) \

    #write to cassandra
    traffic_query = traffic_df.writeStream \
                              .format("org.apache.spark.sql.cassandra") \
                              .option("keyspace", "traffic_weather_keyspace") \
                              .option("table", "stream_traffic_table") \
                              .option("checkpointLocation", "checkpoints_traffic") \
                              .start()
    return traffic_query
    

""" Function to process weather data from kafka"""
def process_kakfa_weather_data(spark: SparkSession,
                               kafka_server: str = 'kafka1:19092',
                               topic: str = 'weather_data') -> DataFrame:
    """
    Args:
        spark: SparkSession object
        kafka_server: kafka server url
        topic: kafka topic
    Returns:
        weather_query: spark streaming query
    """

    #check params
    if not isinstance(spark, SparkSession):
        raise ValueError("spark should be a SparkSession object")
    
    #readstream data from kafka
    weather_df = spark.readStream \
                      .format("kafka") \
                      .option("kafka.bootstrap.servers", kafka_server) \
                      .option("subscribe", topic) \
                      .option("startingOffsets", "earliest") \
                      .load()
    
    #parse json data
    weather_df = convert_kafka_data_to_df(weather_df, "weather")

    #rename columns
    weather_df = weather_df.withColumnRenamed("main", "weather_main") \
                           .withColumnRenamed("description", "weather_description") \
                           .withColumnRenamed("temp", "temperature") \
                           .withColumnRenamed("feels_like", "feels_like_temperature")
    
    #generate weather features
    weather_df = generate_weather_features(weather_df)

    #add execution hour
    weather_df = weather_df.withColumn("execution_hour", hour("execution_time")) \

    #write to cassandra
    weather_query = weather_df.writeStream \
                              .format("org.apache.spark.sql.cassandra") \
                              .option("keyspace", "traffic_weather_keyspace") \
                              .option("table", "stream_weather_table") \
                              .option("checkpointLocation", "checkpoints_weather") \
                              .start()
    
    return weather_query


""" Main function"""
if __name__ == "__main__":
    #create spark session
    with get_SparkSession(name = "Spark Streaming App") as spark:
        traffic_df = process_kafka_traffic_data(spark)
        weather_df = process_kakfa_weather_data(spark)

        #wait for termination
        traffic_df.awaitTermination()
        weather_df.awaitTermination()