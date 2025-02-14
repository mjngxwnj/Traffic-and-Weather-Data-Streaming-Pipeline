from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (from_json, dayofweek, dayofmonth, 
                                   date_format, when, col, hour, month, year)
from utils_spark import (get_SparkSession, 
                         convert_kafka_data_to_df, 
                         generate_traffic_features,
                         generate_weather_features)


""" Function to generate date and time columns"""
def generate_date_time_columns(df: DataFrame) -> DataFrame:
    """
    Args:
        df: Spark DataFrame
    Returns:
        df: Spark DataFrame with additional date and time columns
    """

    #add part of day column
    df = df.withColumn("part_of_day", \
                        when((hour(df['execution_time']).between(0, 5)), "night"). \
                        when((hour(df['execution_time']).between(6, 11)), "morning"). \
                        when((hour(df['execution_time']).between(12, 17)), "afternoon"). \
                        otherwise("evening"))
    
    #add date and time columns
    df = df.withColumn("day_of_week", date_format(df['execution_time'], "EEEE")) \
           .withColumn("day", dayofmonth(df['execution_time'])) \
           .withColumn("month", month(df['execution_time'])) \
           .withColumn("year", year(df['execution_time'])) \
           .withColumn("time_of_day", date_format(df['execution_time'], "HH:mm:ss"))
    
    return df
    

""" Function to load data to HDFS"""
def load_to_HDFS(df: DataFrame, direct: str):
    """
    Args:
        df: Spark DataFrame
        direct: HDFS directory
    """
    
    #check params
    if not isinstance(df, DataFrame):
        raise ValueError("df should be a Spark DataFrame")
    
    HDFS_PATH = f"hdfs://namenode:9000/{direct}"
    
    print("Writing to HDFS" + HDFS_PATH + "at time " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "...")
    
    try:
        df.write.mode("append") \
                .format("parquet") \
                .partitionBy("year", "month", "day") \
                .save(HDFS_PATH)
    except Exception as e:
        print("Error writing to HDFS: ", e)
    
    print("Write to HDFS completed at time " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


""" Function to aggregate traffic data to Cassandra"""
def aggregate_traffic_to_Cassandra(spark: SparkSession, 
                                   HDFS_direct: str, 
                                   keyspace: str, table: str):
    """
    Args:
        HDFS_direct: HDFS directory
        keyspace: Cassandra keyspace
        table: Cassandra table
    """

    #check params
    if not isinstance(spark, SparkSession):
        raise ValueError("spark should be a SparkSession object")
                         
    HDFS_PATH = f"hdfs://namenode:9000/{HDFS_direct}"

    #read from HDFS
    print("Start reading from HDFS: " + HDFS_PATH + "...")
    traffic_df = spark.read.parquet(HDFS_PATH)

    #drop execution time column
    traffic_df = traffic_df.drop("execution_time", "traffic_light", "stop_sign", "traffic_density")

    traffic_df.createOrReplaceTempView("traffic_table")
    traffic_df = spark.sql("""
        SELECT street,
                SUM(bicycle) / COUNT(bicycle) as bicycle_per_observation,
                MAX(bicycle) as max_bicycle_count,
                           
                SUM(car) / COUNT(car) as car_per_observation,
                MAX(car) as max_car_count,
                           
                SUM(motorcycle) / COUNT(motorcycle) as motorcycle_per_observation,
                MAX(motorcycle) as max_motorcycle_count,
                           
                SUM(bus) / COUNT(bus) as bus_per_observation, 
                MAX(bus) as max_bus_count,
                           
                SUM(truck) / COUNT(truck) as truck_per_observation,
                MAX(truck) as max_truck_count,
                           
                part_of_day,
                day_of_week,
                day,
                month,
                year
                           
        FROM traffic_table
        GROUP BY street, part_of_day, day_of_week, day, month, year 
        """)
    
    print("Writing to Cassandra at time " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "...")
    
    try:
        traffic_df.write \
                  .format("org.apache.spark.sql.cassandra") \
                  .options(table=table, keyspace=keyspace) \
                  .mode("append") \
                  .save()
    except Exception as e:
        print("Error writing to Cassandra: ", e)
    
    print("Write to Cassandra completed at time " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def aggregate_weather_to_Cassandra(spark: SparkSession,
                                   HDFS_direct: str, 
                                   keyspace: str, table: str):
    """
    Args:
        HDFS_direct: HDFS directory
        keyspace: Cassandra keyspace
        table: Cassandra table
    """

    #check params
    if not isinstance(spark, SparkSession):
        raise ValueError("spark should be a SparkSession object")
    
    HDFS_PATH = f"hdfs://namenode:9000/{HDFS_direct}"

    #read from HDFS
    print("Start reading from HDFS: " + HDFS_PATH + "...")
    weather_df = spark.read.parquet(HDFS_PATH)

    #drop execution time column
    weather_df = weather_df.drop("execution_time", "weather_main", "weather_description")

    weather_df.createOrReplaceTempView("weather_table")
    weather_df = spark.sql("""
        SELECT street,
                SUM(temperature) / COUNT(temperature) as avg_temperature,
                MAX(temperature) as max_temperature,
                MIN(temperature) as min_temperature,
                           
                SUM(feels_like_temperature) / COUNT(feels_like_temperature) as avg_feels_like_temperature,
                           
                SUM(humidity) / COUNT(humidity) as avg_humidity,
                MAX(humidity) as max_humidity,
                MIN(humidity) as min_humidity,
                           
                SUM(pressure) / COUNT(pressure) as avg_pressure,
                MAX(pressure) as max_pressure,
                MIN(pressure) as min_pressure,
                           
                SUM(wind_speed) / COUNT(wind_speed) as avg_wind_speed,
                MAX(wind_speed) as max_wind_speed,
                MIN(wind_speed) as min_wind_speed,
                
                SUM(humidex) / COUNT(humidex) as avg_humidex,
                MAX(humidex) as max_humidex,
                MIN(humidex) as min_humidex,
                           
                SUM(heat_index) / COUNT(heat_index) as avg_heat_index,
                MAX(heat_index) as max_heat_index,
                MIN(heat_index) as min_heat_index,
                           
                part_of_day,
                day_of_week,
                day,
                month,
                year
                           
        FROM weather_table
        GROUP BY street, part_of_day, day_of_week, day, month, year
    """)
    
    print("Writing to Cassandra at time " + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "...")
    
    try:
        weather_df.write \
                  .format("org.apache.spark.sql.cassandra") \
                  .options(table=table, keyspace=keyspace) \
                  .mode("append") \
                  .save()
    except Exception as e:
        print("Error writing to Cassandra: ", e)
    
    print("Write to Cassandra completed at time " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


""" Function to process traffic data from kafka"""
def process_kafka_traffic_data(spark: SparkSession,
                               kafka_server: str = 'kafka1:19092',
                               topic: str = 'traffic_data'):
    """
    Args:
        spark: SparkSession object
        kafka_server: kafka server url
        topic: kafka topic
    """
    
    #check params
    if not isinstance(spark, SparkSession):
        raise ValueError("spark should be a SparkSession object")
    
    traffic_df = spark.read \
                      .format("kafka") \
                      .option("kafka.bootstrap.servers", kafka_server) \
                      .option("subscribe", topic) \
                      .option("startingOffsets", "earliest") \
                      .load()
    
    #parse json data
    traffic_df = convert_kafka_data_to_df(traffic_df, "traffic")

    #filter data by execution time
    traffic_df = traffic_df.filter(date_format(traffic_df['execution_time'], "yyyy-MM-dd") == \
                                    datetime.now().strftime("%Y-%m-%d"))
    
    #rename columns
    traffic_df = traffic_df.withColumnRenamed("traffic light", "traffic_light") \
                           .withColumnRenamed("stop sign", "stop_sign")
    
    #generate traffic features
    traffic_df = generate_traffic_features(traffic_df)
    traffic_df = generate_date_time_columns(traffic_df)

    #load to HDFS
    load_to_HDFS(traffic_df, "batch_data/traffic_data")


def process_kafka_weather_data(spark: SparkSession,
                               kafka_server: str = 'kafka1:19092',
                               topic: str = 'weather_data'):
    """
    Args:
        spark: SparkSession object
        kafka_server: kafka server url
        topic: kafka topic
    """

    #check params
    if not isinstance(spark, SparkSession):
        raise ValueError("spark should be a SparkSession object")
    
    weather_df = spark.read \
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
    
    #filter data by execution time
    weather_df = weather_df.filter(date_format(weather_df['execution_time'], "yyyy-MM-dd") == \
                                    datetime.now().strftime("%Y-%m-%d"))
    
    #generate weather features
    weather_df = generate_weather_features(weather_df)
    weather_df = generate_date_time_columns(weather_df)

    #load to HDFS
    load_to_HDFS(weather_df, "batch_data/weather_data")


""" Main function to read kafka data in batch"""
if __name__ == "__main__":
    with get_SparkSession("Spark batch") as spark:
        process_kafka_traffic_data(spark)
        process_kafka_weather_data(spark)
        aggregate_traffic_to_Cassandra(spark, 
                                       "batch_data/traffic_data", 
                                       "traffic_weather_keyspace", 
                                       "batch_traffic_table")
        aggregate_weather_to_Cassandra(spark, 
                                       "batch_data/weather_data", 
                                       "traffic_weather_keyspace", 
                                       "batch_weather_table")