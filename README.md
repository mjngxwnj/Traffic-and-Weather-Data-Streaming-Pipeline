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
#### 1. **Traffic Camera Data (Ho Chi Minh City)**
- Traffic images are continuously captured from **Ho Chi Minh City's** public surveillance cameras using **Selenium**.
  
  ![Screenshot](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/screenshot.png)
- The capture images are then processed using **YOLOv8** and **OpenCV (CV2)** to detect and count vehicles on the streets.
  
  ![Output_screenshot](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/output_screenshot.png)
- After processing, the system generates structured data containing vehicle counts at specific timestamps. This includes the number of **bicycle**, **cars**, **motorcycles**, **buses**, and **trucks** detected on each street.
  ```
  0: 320x640 2 cars, 17 motorcycles, 267.0ms
  Speed: 4.0ms preprocess, 267.0ms inference, 2.0ms postprocess per image at shape (1, 3, 320, 640)
  {
     'street': truongchinh_tankitanquy,
     'bicycle': 0,
     'car': 2,
     'motorcycle': 17,
     'bus': 1,
     'truck': 0, 
     'traffic light': 0,
     'stop sign': 0,
     'execution_time': 2025-02-17 16:02:01
  }
  ```
#### 2. **OpenWeather API**
- Weather data is retrieved from the **OpenWeather API**, providing real-time weather conditions for **Ho Chi Minh City**.
  
  ```python
  API_CALL = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={API_KEY}"
  ```
  + **latitude** & **longtitude**: Coordinates of the desired location.
  + **API_KEY**: Personal API key from **OpenWeather**.
- The collected data includes key meteorological parameters such as:
  + **Temperature** (°K)
  + **Humidity** (%)
  + **Wind Speed** (m/s)
  + **Weather Condition** (Clear, Cloudy, Rainy,...)
  + **Timestamp** of the observation
- For example, if we use the following ```latitude``` and ```longitude``` for **Truong Chinh - Tan Ki Tan Quy Street**:
  + **latitude**: 10.80418
  + **longitude**: 106.63588
- The following weather data as a respone from the API:
  ```
  {
     'main': 'Clouds',
     'description': 'scattered clouds',
     'temp': 305.14,
     'feels_like': 308.77,
     'temp_min': 305.14,
     'temp_max': 305.39,
     'pressure': 1009,
     'humidity': 55,
     'wind_speed': 3.13,
     'wind_deg': 204,
     'street': 'truongchinh_tankitanquy',
     'execution_time': '2025-02-17 16:52:31'
  }
  ```
### Kafka Integration
- **Kafka** is used to stream both traffic and weather data, providing fault tolerance and enabling real-time processing.
- The system supports streaming and batch processing, ensuring flexible data handling.
### Stream Layer
- Data from **Kafka** is consumed to process vehicle counts and weather information in near real-time.
- New features are created and processed using Spark Streaming:
  + **traffic_density**: Calculate based on vehicle counts.
  + **temperature, feels_like_temperature, temp_min, temp_max**: Convert from Kelvin (°K) to Celsius (°C).
  + **wind_direction**: Convert wind degree to a categorical feature (e.g., North, South, East, West).
  + **humidex**: Reflects perceived heat, considering temperature and humidity.
  + **heat_index**: Combines temperature and humidity to indicate heat and **health risks**.
- Then, data will be loaded into **Cassandra** for low latency queries and for streaming dashboard application.
### Batch Layer
- The **Batch Layer** processes historical traffic and weather data in scheduled batches (daily) using **Spark**.
- Data is consumed from **Kafka** at regular daily intervals and stored in HDFS with **partitioning** for efficient storage and processing.
- Aggregations are performed to derive insights such as **daily traffic trends**, **average weather conditions** for different parts of the day.
- The results are then loaded into **Cassandra** for fast querying and analysis.
- HDFS is partition by **year**, **month**, **day**:
  ![HDFS]()
- **Cassandra** with two tables **batch_traffic_table** and **batcht_weather_table**:
  ```sql
  SELECT * FROM traffic_weather_keyspace.batch_traffic_table;
  ```
  | Year | Month | Day | Street | Part of Day | Bicycle per Obs | Bus per Obs | Car per Obs | Day of Week | Max Bicycle Count | Max Bus Count | Max Car Count | Max Motorcycle Count | Max Truck Count | Motorcycle per Obs | Truck per Obs |
  |------|-------|-----|----------------------------|-------------|-----------------|-------------|-------------|-------------|-------------------|--------------|--------------|----------------------|--------------|------------------|---------------|
  | 2025 | 2 | 13 | cmt8_truongson | Evening | 0.01222 | 0.171079 | 2.91039 | Thursday | 2 | 4 | 11 | 45 | 3 | 10.18126 | 0.183299 |
  | 2025 | 2 | 13 | hoangvanthu_conghoa | Evening | 0.02444 | 0.201629 | 7.17108 | Thursday | 1 | 3 | 18 | 30 | 5 | 7.23422 | 1.0835 |
  | 2025 | 2 | 13 | nguyenthaison_phanvantri2 | Evening | 0.095723 | 0.02444 | 3.63951 | Thursday | 3 | 2 | 12 | 38 | 3 | 16.37475 | 0.539715 |
  | 2025 | 2 | 13 | truongchinh_tankitanquy | Evening | 0.026477 | 0.384929 | 5.39919 | Thursday | 2 | 5 | 25 | 44 | 4 | 11.5723 | 0.533605 |
  | 2025 | 2 | 16 | cmt8_truongson | Evening | 0.057143 | 0.171429 | 4.05714 | Sunday | 1 | 1 | 9 | 31 | 1 | 12.91429 | 0.142857 |
  | 2025 | 2 | 16 | hoangvanthu_conghoa | Evening | 0 | 0.2 | 8.31429 | Sunday | 0 | 1 | 16 | 25 | 3 | 9.11429 | 0.742857 |
  | 2025 | 2 | 16 | nguyenthaison_phanvantri2 | Evening | 0 | 0.028571 | 4.4 | Sunday | 0 | 1 | 10 | 34 | 2 | 18.31429 | 0.2 |
  | 2025 | 2 | 16 | truongchinh_tankitanquy | Evening | 0.028571 | 0.771429 | 8.17143 | Sunday | 1 | 4 | 19 | 39 | 3 | 14.85714 | 1.17143 |
  | 2025 | 2 | 6 | cmt8_truongson | Evening | 0.01528 | 0.052632 | 2.44822 | Thursday | 1 | 2 | 13 | 29 | 4 | 8.691 | 0.220713 |
  | 2025 | 2 | 6 | cmt8_truongson | Night | 0.004484 | 0.017937 | 1.70852 | Thursday | 1 | 1 | 8 | 14 | 2 | 1.95516 | 0.116592 |
  | 2025 | 2 | 6 | hoangvanthu_conghoa | Evening | 0.050847 | 0.150847 | 7.47119 | Thursday | 2 | 3 | 21 | 27 | 3 | 9.23456 | 0.6789 |
  | ... |


