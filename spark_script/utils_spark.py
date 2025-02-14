from pyspark.sql import SparkSession 
from pyspark.sql import DataFrame
from pyspark.sql.types import (StructType, StructField, StringType, 
                               IntegerType, TimestampType, FloatType)
from pyspark.sql.functions import from_json, col, when, exp, pow, date_format
from contextlib import contextmanager


""" Function for creating a SparkSession"""
@contextmanager
def get_SparkSession(name: str):
    # Create a SparkSession
    spark = SparkSession.builder \
                        .appName(name) \
                        .config("spark.cassandra.connection.host", "cassandra") \
                        .config("spark.cassandra.connection.port", "9042") \
                        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    try:
        yield spark

    except Exception as e: 
        raise Exception(f"Error while creating Spark Session: {e}")
    
    finally:
        spark.stop()
        

""" Function for getting schema"""
def get_schema(schema_name: str) -> StructType:
    """
    Args:
        table_name: name of the data table
    Returns:
        schema: schema of the data table
    """

    # Define schema for traffic data
    traffic_schema = StructType([
        StructField("street",         StringType(), True),
        StructField("bicycle",        IntegerType(), True),
        StructField("car",            IntegerType(), True),
        StructField("motorcycle",     IntegerType(), True),
        StructField("bus",            IntegerType(), True),
        StructField("truck",          IntegerType(), True),
        StructField("traffic light",  IntegerType(), True),
        StructField("stop sign",      IntegerType(), True),
        StructField("execution_time", TimestampType(), True)
    ])

    # Define schema for weather data
    weather_schema = StructType([
        StructField("street",         StringType(), True),
        StructField("main",           StringType(), True),
        StructField("description",    StringType(), True),
        StructField("temp",           FloatType(), True),
        StructField("feels_like",     FloatType(), True),
        StructField("temp_min",       FloatType(), True),
        StructField("temp_max",       FloatType(), True),
        StructField("pressure",       FloatType(), True),
        StructField("humidity",       FloatType(), True),
        StructField("wind_speed",     FloatType(), True),
        StructField("wind_deg",       FloatType(), True),
        StructField("execution_time", TimestampType(), True)
    ])

    # Return schema based on the table name
    return traffic_schema if schema_name == "traffic" else weather_schema


""" Function for processing Kafka data (convert json to DataFrame)"""
def convert_kafka_data_to_df(df: DataFrame, schema: str) -> DataFrame:
    """
    Args:
        df: Spark DataFrame
        schema: schema of the data
    Returns:
        df: processed Spark DataFrame
    """
    
    #check params
    if not isinstance(df, DataFrame):
        raise ValueError("df should be a Spark DataFrame")
    if schema not in ["traffic", "weather"]:
        raise ValueError("schema should be either 'traffic' or 'weather'")
    
    #implement processing
    df = df.selectExpr("CAST(value AS STRING) as message")

    df = df.withColumn("value", from_json("message", get_schema(schema)))

    df = df.select("value.*")

    return df


""" Function for generating traffic features"""
def generate_traffic_features(traffic_df: DataFrame) -> DataFrame:
    """
    Args:
        df: Spark DataFrame
    Returns:
        df: Spark DataFrame with added traffic features
    """
    
    #check params
    if not isinstance(traffic_df, DataFrame):
        raise ValueError("df should be a Spark DataFrame")
    
    #add traffic density feature
    traffic_df = traffic_df.withColumn("traffic_density", \
                            col("bicycle")*0.5 + col("car")*5 + \
                            col("motorcycle")*1 + col("bus")*10 + \
                            col("truck")*8)
    
    #calculate traffic density
    traffic_df = traffic_df.withColumn("traffic_density", 
                            when(col("traffic_density") < 40, "low") \
                           .when(col("traffic_density") < 80, "medium")\
                           .otherwise("high"))
    
    return traffic_df


""" Function for generating weather features"""
def generate_weather_features(weather_df: DataFrame) -> DataFrame:
    """
    Args:
        df: Spark DataFrame
    Returns:
        df: Spark DataFrame with added weather features
    """
    
    #check params
    if not isinstance(weather_df, DataFrame):
        raise ValueError("df should be a Spark DataFrame")
    
    #change temperature from Kelvin to Celsius
    weather_df = weather_df.withColumn("temperature", col("temperature") - 273.15) \
                           .withColumn("feels_like_temperature", col("feels_like_temperature") - 273.15) \
                           .withColumn("temp_min", col("temp_min") - 273.15) \
                           .withColumn("temp_max", col("temp_max") - 273.15)

    #add weather features
    weather_df = weather_df.withColumn("humidity_level",when(col("humidity") < 30, "low") \
                                                       .when(col("humidity") < 70, "medium") \
                                                       .otherwise("high")) \
                           .withColumn("thermal_comfort",when(col("temperature") < 20, "cold") \
                                                        .when(col("temperature") < 30, "comfortable") \
                                                        .otherwise("hot")) \
                           .withColumn("wind_direction", \
                                        when(col("wind_deg").between(0, 22.5), "N"). \
                                        when(col("wind_deg").between(22.5, 67.5), "NE"). \
                                        when(col("wind_deg").between(67.5, 112.5), "E"). \
                                        when(col("wind_deg").between(112.5, 157.5), "SE"). \
                                        when(col("wind_deg").between(157.5, 202.5), "S"). \
                                        when(col("wind_deg").between(202.5, 247.5), "SW"). \
                                        when(col("wind_deg").between(247.5, 292.5), "W"). \
                                        when(col("wind_deg").between(292.5, 337.5), "NW"). \
                                        otherwise("N"))
    
    """
    Add humidex and heat index
    Notes:
        Humidex:
        + A measure of how hot the weather feels to the average person, 
        accounting for humidity.
        + It is useful because it considers the combined effect of temperature and humidity, 
        giving a better sense of the perceived temperature.

        Heat Index:
        + A measure of how hot the weather feels to the average person, 
        accounting for humidity.
        + It is useful because it factors in the humidity's impact on the body's ability to cool itself 
        through evaporation of sweat.
    """

    #add Humidex
    weather_df = weather_df.withColumn("humidex", \
    col("temperature") + 0.5555*(6.11*exp(5417.7530*(1/273.15 - 1/(273.15 + (100 - col("humidity")/100)/5))) - 10) )

    #add Heat Index
    weather_df = weather_df.withColumn("heat_index", 
                            - 42.379 + 2.04901523 * (col("temperature")*1.8+32) + 10.14333127 * col("humidity")
                            - 0.22475541 * (col("temperature")*1.8+32) * col("humidity")
                            - 0.00683783 * pow((col("temperature")*1.8+32),2) - 0.05481717 * pow(col("humidity"),2)
                            + 0.00122874 * pow((col("temperature")*1.8+32),2) * col("humidity")
                            + 0.00085282 * (col("temperature")*1.8+32) * pow(col("humidity"),2)
                            - 0.00000199 * pow((col("temperature")*1.8+32),2) * pow(col("humidity"),2))
    #convert heat index back to Celsius
    weather_df = weather_df.withColumn("heat_index", (col("heat_index") - 32) / 1.8)
    
    return weather_df