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
├───application                        # Streamlit application folder
│   │   dockerfile                     # Docker configuration file
│   │   main.py                        # Main script to run Streamlit
│   │   requirements.txt               # List of required libraries
│   │   streamlit_backend.py           
│   │   streamlit_frontend.py         
│   │
│   ├───.streamlit
│           config.toml                 # Streamlit configuration file
│
├───cql
│       keyspace_table.cql              # CQL script for creating keyspace and tables in Cassandra
│
├───images
│       Architecture.png                
│
├───images_processing                    # Image processing module
│   │   images_processor.py              # Image processing class using YOLOv8
│   │
│   ├───screenshot_picture               # Captured images from street cameras
│           screenshot_cmt8_truongson.png
│           screenshot_hoangvanthu_conghoa.png
│           screenshot_nguyenthaison_phanvantri2.png
│           screenshot_truongchinh_tankitanquy.png
│
├───spark_script                         # Spark scripts for batch & streaming applications
│       batch_job.py                     # Spark batch processing script
│       stream_job.py                    # Spark streaming processing script
│       utils_spark.py                   # Utility functions for Spark
│
├───weather_fetching                     # Weather data fetching module
│       weather_utils.py                 # Utility functions for fetching weather data from API
│
├───docker-compose.yaml                  # Docker Compose configuration file
├───hadoop.env                           # Hadoop environment configuration
├───kafka_stream.py                      # Script to run Kafka producer/consumer
├───run_batch_job.sh                     # Shell script to start the batch job
├───run_stream_job.sh                    # Shell script to start the streaming job
```
### Data Sources
This project collects and processes data from the following sources:
1. **Traffic Camera Data (Ho Chi Minh City)**
- Traffic images are continuosly captured from **Ho Chi Minh City's** public surveillance cameras using **Selenium**.
- ![Screenshot](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/screenshot.png)
- Then, we will use YOLOv8 and CV2 to count vehicles on streets.
- ![Output_screenshot](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/output_screenshot.png)
