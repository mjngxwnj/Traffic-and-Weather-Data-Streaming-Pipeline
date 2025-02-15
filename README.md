# Traffic-and-Weather-Data-Streaming-Pipeline
## Table of Contents
- [Introduction](#Introduction)
- [Architecture](#Architecture)
## Introduction
This project develops a near real-time streaming pipeline that processes both traffic and weather data. The data is streamed through Kafka and processed using Spark Streaming to provide near real-time analysis of traffic flow and weather conditions.
## Architecture
![architecture](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/Architecture.png)  
- **Data Ingestion**: Capturing real-time images from [traffic cameras](https://giaothong.hochiminhcity.gov.vn/Map.aspx), processing them with **YOLOv8** to **detect** and **count vehicles**, and **retrieving weather data** (temperature, humidity, wind speed,...) for specific road locations using [Open Weather Map API](https://openweathermap.org/api).
