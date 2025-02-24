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
  ```stream_traffic_table```:
  ```
  -------------------------------------------
  Batch: 1
  -------------------------------------------
  +--------------------+-------+---+----------+---+-----+-------------+---------+-------------------+---------------+--------------+
  |              street|bicycle|car|motorcycle|bus|truck|traffic_light|stop_sign|     execution_time|traffic_density|execution_hour|
  +--------------------+-------+---+----------+---+-----+-------------+---------+-------------------+---------------+--------------+
  |nguyenthaison_pha...|      0|  8|        11|  0|    0|            0|        0|2025-02-24 16:22:19|         medium|            16|
  | hoangvanthu_conghoa|      0| 11|        14|  0|    0|            0|        0|2025-02-24 16:22:34|         medium|            16|
  |truongchinh_tanki...|      0| 11|         3|  0|    0|            1|        0|2025-02-24 16:22:40|         medium|            16|
  |      cmt8_truongson|      0| 11|         9|  0|    0|            0|        0|2025-02-24 16:22:45|         medium|            16|
  +--------------------+-------+---+----------+---+-----+-------------+---------+-------------------+---------------+--------------+
  
  -------------------------------------------
  Batch: 2
  -------------------------------------------
  +--------------------+-------+---+----------+---+-----+-------------+---------+-------------------+---------------+--------------+
  |              street|bicycle|car|motorcycle|bus|truck|traffic_light|stop_sign|     execution_time|traffic_density|execution_hour|
  +--------------------+-------+---+----------+---+-----+-------------+---------+-------------------+---------------+--------------+
  |nguyenthaison_pha...|      0|  3|        20|  0|    0|            0|        0|2025-02-24 16:22:50|            low|            16|
  | hoangvanthu_conghoa|      0|  6|         6|  0|    1|            1|        0|2025-02-24 16:22:54|         medium|            16|
  |truongchinh_tanki...|      0|  6|        30|  0|    0|            0|        0|2025-02-24 16:22:59|         medium|            16|
  |      cmt8_truongson|      0| 10|        14|  0|    0|            0|        0|2025-02-24 16:23:04|         medium|            16|
  +--------------------+-------+---+----------+---+-----+-------------+---------+-------------------+---------------+--------------+
  ...
  ```stream_weather_table```:
  ```
  -------------------------------------------
Batch: 19
-------------------------------------------
+--------------------+------------+-------------------+-----------------+----------------------+-----------------+-----------------+--------+--------+----------+--------+-------------------+--------------+---------------+--------------+------------------+-----------------+--------------+
|              street|weather_main|weather_description|      temperature|feels_like_temperature|       
  temp_min|         temp_max|pressure|humidity|wind_speed|wind_deg|     execution_time|humidity_level|thermal_comfort|wind_direction|           humidex|       heat_index|execution_hour|
+--------------------+------------+-------------------+-----------------+----------------------+-----------------+-----------------+--------+--------+----------+--------+-------------------+--------------+---------------+--------------+------------------+-----------------+--------------+
|nguyenthaison_pha...|      Clouds|   scattered clouds|33.32000122070315|     38.55001220703127|32.95998535156252|33.32000122070315|  1010.0|    55.0|      4.12|   100.0|2025-02-24 16:27:30|        medium| 
           hot|             E|40.808509913328535|38.54975901887922|            16|
| hoangvanthu_conghoa|      Clouds|   scattered clouds|33.36000976562502|      38.6400085449219|32.99001464843752|33.36000976562502|  1010.0|    55.0|      4.12|   100.0|2025-02-24 16:27:30|        medium| 
           hot|             E| 40.84851845825041|38.64328366151326|            16|
|truongchinh_tanki...|      Clouds|   scattered clouds|32.98000488281252|     36.83001098632815|32.98000488281252|33.33999023437502|  1010.0|    52.0|      4.12|   100.0|2025-02-24 16:27:30|        medium| 
           hot|             E| 40.47345195402629| 36.8346746480809|            16|
|      cmt8_truongson|      Clouds|   scattered clouds| 33.3799987792969|     38.68999633789065| 33.0100036621094| 33.3799987792969|  1010.0|    55.0|      4.12|   100.0|2025-02-24 16:27:30|        medium| 
           hot|             E|40.868507471922285|38.69012844583227|            16|
+--------------------+------------+-------------------+-----------------+----------------------+-----------------+-----------------+--------+--------+----------+--------+-------------------+--------------+---------------+--------------+------------------+-----------------+--------------+
  ```
### Batch Layer
- The **Batch Layer** processes historical traffic and weather data in scheduled batches (daily) using **Spark**.
- Data is consumed from **Kafka** at regular daily intervals and stored in HDFS with **partitioning** for efficient storage and processing.
- Aggregations are performed to derive insights such as **daily traffic trends**, **average weather conditions** for different parts of the day.
- The results are then loaded into **Cassandra** for fast querying and analysis.
- HDFS is partition by **year**, **month**, **day**:
  
  ![HDFS](https://github.com/mjngxwnj/Traffic-and-Weather-Data-Streaming-Pipeline/blob/main/images/HDFS.PNG)
### Serving Layer
- **Cassandra** stores both **batch** and **stream** data to support fast and direct querying.
- The data is structured into four tables in **Cassandra**:
  + stream_traffic_table: Stores real-time traffic data, continuously updated from **Kafka** and **Spark Streaming**.
  + stream_weather_table: Stores real-time weather data for immediate analysis.
  + batch_traffic_table: Stores aggregated traffic data from the batch processing layer, updated daily.
  + batch_weather_table: Contains daily aggregated weather data, such as **temperature**, **humidity**, **windspeed**,..
  
  ```sql
  SELECT * FROM traffic_weather_keyspace.batch_traffic_table;
  ```
  | Year | Month | Day | Street | Part of Day | Bicycle per Observation | Bus per Observation | Car per Observation | Day of Week | Max Bicycle Count | Max Bus Count | Max Car Count | Max Motorcycle Count | Max Truck Count | Motorcycle per Observation | Truck per Observation |
  |------|-------|-----|--------|-------------|-------------------------|---------------------|---------------------|-------------|-------------------|--------------|--------------|----------------------|----------------|--------------------------|----------------------|
    | 2025 | 2 | 13 | cmt8_truongson | Evening | 0.01222 | 0.171079 | 2.91039 | Thursday | 2 | 4 | 11 | 45 | 3 | 10.18126 | 0.183299 |
    | 2025 | 2 | 13 | hoangvanthu_conghoa | Evening | 0.02444 | 0.201629 | 7.17108 | Thursday | 1 | 3 | 18 | 30 | 5 | 7.23422 | 1.0835 |
    | 2025 | 2 | 13 | nguyenthaison_phanvantri2 | Evening | 0.095723 | 0.02444 | 3.63951 | Thursday | 3 | 2 | 12 | 38 | 3 | 16.37475 | 0.539715 |
    | 2025 | 2 | 13 | truongchinh_tankitanquy | Evening | 0.026477 | 0.384929 | 5.39919 | Thursday | 2 | 5 | 25 | 44 | 4 | 11.5723 | 0.533605 |
    | ... |

  ```cql
  SELECT * FROM traffic_weather_keyspace.batch_weather_table;
  ```
  | Year | Month | Day | Street | Part of Day | Avg Feels Like Temperature | Avg Heat Index | Avg Humidex | Avg Humidity | Avg Pressure | Avg Temperature | Avg Wind Speed | Day of Week | Max Heat Index | Max Humidex | Max Humidity | Max Pressure | Max Temperature | Max Wind Speed | Min Heat Index | Min Humidex | Min Humidity | Min Pressure | Min Temperature | Min Wind Speed |
  |------|-------|-----|---------------------------|-------------|----------------------------|----------------|-------------|--------------|--------------|-----------------|----------------|-------------|----------------|-------------|--------------|--------------|-----------------|----------------|----------------|-------------|--------------|--------------|-----------------|----------------|
  | 2025 | 2 | 13 | cmt8_truongson | Evening | 27.03657 | 28.46759 | 33.75731 | 83.25355 | 1012.30829 | 26.31523 | 3.34345 | Thursday | 29.36735 | 34.16413 | 84 | 1013 | 26.72 | 3.6 | 28.0974 | 33.57085 | 82 | 1012 | 26.13 | 3.09 |
  | 2025 | 2 | 13 | hoangvanthu_conghoa | Evening | 27.03904 | 28.40921 | 33.73001 | 83.28803 | 1012.34076 | 26.28798 | 3.34966 | Thursday | 29.27495 | 34.14415 | 84 | 1013 | 26.70001 | 3.6 | 28.03099 | 33.56084 | 82 | 1012 | 26.11999 | 3.09 |
  | 2025 | 2 | 13 | nguyenthaison_phanvantri2 | Evening | 26.37361 | 28.37168 | 33.71249 | 83.28803 | 1012.68152 | 26.27047 | 3.34345 | Thursday | 30.09914 | 34.47249 | 84 | 1013 | 27.02999 | 3.6 | 27.98829 | 33.54086 | 82 | 1012 | 26.1 | 3.09 |
  | 2025 | 2 | 13 | truongchinh_tankitanquy | Evening | 28.61871 | 29.31873 | 34.07823 | 85.81541 | 1012.29816 | 26.64035 | 3.34345 | Thursday | 30.00318 | 34.43251 | 94 | 1013 | 26.99002 | 3.6 | 27.95119 | 33.40446 | 83 | 1012 | 25.98001 | 3.09 |
  | ... |




