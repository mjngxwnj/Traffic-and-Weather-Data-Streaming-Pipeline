# Traffic-and-Weather-Data-Streaming-Pipeline
## Table of Contents
- [Introduction](#Introduction)
- [Architecture](#Architecture)
- [Project Overview](#Project-overview)
## Introduction
This project develops a **near real-time streaming pipeline** that processes both **traffic** and **weather data**. The data is streamed through **Kafka** and processed using **Spark** to provide near real-time analysis of traffic flow and weather conditions.
## Architecture
![architecture](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/Architecture.png)  
- **Data Ingestion**: Capturing real-time images from [Traffic Cameras](https://giaothong.hochiminhcity.gov.vn/Map.aspx), processing them with **YOLOv8** to **detect** and **count vehicles**, and **retrieving weather data** (temperature, humidity, wind speed,...) for specific road locations using [Open Weather Map API](https://openweathermap.org/api).
- **Kafka**: Stores ingested data temporarily to support both **stream** and **batch procesing**, ensuring fault tolerance, enabling multiple consumers, and allowing flexible data handling.
- **Stream Layer**: Use **Spark Streaming** to consume near real-time data from **Kafka**, process vehicle counts and weather information, and store the results in **Cassandra** for low-latency queries and real-time analytics.
- **Batch Layer**: Use **Spark** to consume historical traffic and weather data from **Kafka** in scheduled batches (at the end of each day), partition and store it in **HDFS**, then perform aggregations to derive insights before loading the results into **Cassandra** for analysis.
- **Serving Layer**: Processed traffic and weather data is stored in Cassandra for fast querying, while Streamlit serves as the front-end application, providing an interactive real-time dashboard for monitoring traffic flow, weather conditions, and historical trends.
## Project Overview
### Directory Structure
```
├───application
│   │   dockerfile
│   │   main.py
│   │   requirements.txt
│   │   streamlit_backend.py
│   │   streamlit_frontend.py
│   │
│   ├───.streamlit
│   │       config.toml
│   │
│   └───__pycache__
│           streamlit_backend.cpython-39.pyc
│           streamlit_frontend.cpython-39.pyc
│
├───cql
│       keyspace_table.cql
│
├───images
│       Architecture.png
│
├───images_processing
│   │   images_processor.py
│   │
│   ├───screenshot_picture
│   │       screenshot_cmt8_truongson.png
│   │       screenshot_hoangvanthu_conghoa.png
│   │       screenshot_nguyenthaison_phanvantri2.png
│   │       screenshot_truongchinh_tankitanquy.png
│   │
│   └───__pycache__
│           images_processor.cpython-312.pyc
│
├───spark_script
│   │   batch_job.py
│   │   stream_job.py
│   │   utils_spark.py
│   │
│   └───__pycache__
│           spark_utils.cpython-312.pyc
│           utils_spark.cpython-312.pyc
│
└───weather_fetching
    │   weather_utils.py
    │
    └───__pycache__
            weather_utils.cpython-312.pyc
```
