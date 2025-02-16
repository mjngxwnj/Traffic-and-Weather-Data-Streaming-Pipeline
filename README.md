# Traffic-and-Weather-Data-Streaming-Pipeline
## Table of Contents
- [Introduction](#Introduction)
- [Architecture](#Architecture)
## Introduction
This project develops a **near real-time streaming pipeline** that processes both **traffic** and **weather data**. The data is streamed through **Kafka** and processed using **Spark** to provide near real-time analysis of traffic flow and weather conditions.
## Architecture
![architecture](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/Architecture.png)  
- **Data Ingestion**: Capturing real-time images from [traffic cameras](https://giaothong.hochiminhcity.gov.vn/Map.aspx), processing them with **YOLOv8** to **detect** and **count vehicles**, and **retrieving weather data** (temperature, humidity, wind speed,...) for specific road locations using [Open Weather Map API](https://openweathermap.org/api).
- **Kafka**: Stores ingested data temporarily to support both **stream** and **batch procesing**, ensuring fault tolerance, enabling multiple consumers, and allowing flexible data handling.
- **Stream Layer**: Use **Spark Streaming** to consume near real-time data from **Kafka**, process vehicle counts and weather information, and store the results in **Cassandra** for low-latency queries and real-time analytics.
- **Batch Layer**: Use **Spark** to consume historical traffic and weather data from Kafka in scheduled batches (at the end of each day), partition and store it in **HDFS**, then perform aggregations to derive insights before loading the results into **Cassandra** for analysis.
- **Serving Layer**: 
