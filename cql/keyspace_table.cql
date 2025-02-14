--create keyspace
CREATE KEYSPACE IF NOT EXISTS traffic_weather_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};


--create stream_traffic_table
CREATE TABLE IF NOT EXISTS traffic_weather_keyspace.stream_traffic_table(
    street TEXT,
    bicycle INT,
    car INT,
    motorcycle INT, 
    bus INT,
    truck INT,
    traffic_light INT,
    stop_sign INT,
    execution_time TIMESTAMP,
    traffic_density TEXT,
    execution_hour INT,
    PRIMARY KEY ((execution_hour, street), execution_time)
) WITH gc_grace_seconds = 3600;


--create stream_weather_table
CREATE TABLE IF NOT EXISTS traffic_weather_keyspace.stream_weather_table(
    street TEXT,
    weather_main TEXT,
    weather_description TEXT,
    temperature FLOAT,
    feels_like_temperature FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    wind_deg FLOAT,
    execution_time TIMESTAMP,
    humidity_level TEXT,
    thermal_comfort TEXT,
    wind_direction TEXT,
    humidex FLOAT,
    heat_index FLOAT,
    execution_hour TIME,
    PRIMARY KEY ((execution_hour, street), execution_time)
) WITH gc_grace_seconds = 3600;


CREATE TABLE IF NOT EXISTS traffic_weather_keyspace.batch_traffic_table(
    street TEXT,
    bicycle_per_observation FLOAT,
    max_bicycle_count INT,
    car_per_observation FLOAT,
    max_car_count INT,
    motorcycle_per_observation FLOAT,
    max_motorcycle_count INT,
    bus_per_observation FLOAT,
    max_bus_count INT,
    truck_per_observation FLOAT,
    max_truck_count INT,
    part_of_day TEXT,
    day_of_week TEXT,
    day INT,
    month INT,
    year INT,
    PRIMARY KEY((year, month, day), street, part_of_day)
);


CREATE TABLE IF NOT EXISTS traffic_weather_keyspace.batch_weather_table(
    street TEXT,
    avg_temperature FLOAT,
    max_temperature FLOAT,
    min_temperature FLOAT,
    avg_feels_like_temperature FLOAT,
    avg_humidity FLOAT,
    max_humidity FLOAT,
    min_humidity FLOAT,
    avg_pressure FLOAT,
    max_pressure FLOAT,
    min_pressure FLOAT,
    avg_wind_speed FLOAT,
    max_wind_speed FLOAT,
    min_wind_speed FLOAT,
    avg_humidex FLOAT,
    max_humidex FLOAT,
    min_humidex FLOAT,
    avg_heat_index FLOAT,
    max_heat_index FLOAT,
    min_heat_index FLOAT,
    part_of_day TEXT,
    day_of_week TEXT,
    day INT,
    month INT,
    year INT,
    PRIMARY KEY((year, month, day), street, part_of_day)
);