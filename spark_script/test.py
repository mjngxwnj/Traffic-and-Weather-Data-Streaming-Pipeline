from utils_spark import get_SparkSession
with get_SparkSession("test") as spark:
    direct = "batch_data/traffic_data"
    df = spark.read.parquet(f"hdfs://namenode:9000/{direct}")
    df.show()