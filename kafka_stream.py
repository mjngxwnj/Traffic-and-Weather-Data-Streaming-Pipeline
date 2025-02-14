import json
from selenium import webdriver
from images_processing.images_processor import Images_Processor
from weather_fetching.weather_utils import Weather_Fetcher
from confluent_kafka import Producer

""" Link camera """
CAMERA_NGUYENTHAISON_PHANVANTRI2_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=5a6060e08576340017d0660f&camLocation=Nguy%E1%BB%85n%20Th%C3%A1i%20S%C6%A1n%20-%20Phan%20V%C4%83n%20Tr%E1%BB%8B%202&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CAMERA_HOANGVANTHU_CONGHOA_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=5d8cdbdc766c88001718896a&camLocation=Ho%C3%A0ng%20V%C4%83n%20Th%E1%BB%A5%20-%20C%E1%BB%99ng%20H%C3%B2a&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CAMERA_TRUONGCHINH_TANKITANQUY_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=586e25e1f9fab7001111b0ae&camLocation=Tr%C6%B0%E1%BB%9Dng%20Chinh%20-%20T%C3%A2n%20K%E1%BB%B3%20T%C3%A2n%20Qu%C3%BD&camMode=camera&videoUrl=http://camera.thongtingiaothong.vn/s/586e25e1f9fab7001111b0ae/index.m3u8"
CAMERA_CMT8_TRUONGSON_STREET = "https://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae79aabfd3d90017e8f26a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20Tr%C6%B0%E1%BB%9Dng%20S%C6%A1n&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"

""" List of camera """
camera_list = {"nguyenthaison_phanvantri2": CAMERA_NGUYENTHAISON_PHANVANTRI2_STREET, 
               "hoangvanthu_conghoa": CAMERA_HOANGVANTHU_CONGHOA_STREET,
               "truongchinh_tankitanquy": CAMERA_TRUONGCHINH_TANKITANQUY_STREET,
               "cmt8_truongson": CAMERA_CMT8_TRUONGSON_STREET}

""" List of locations and their corresponding latitude and longitude"""
lon_lat_dict = {"nguyenthaison_phanvantri2": {"lat": 10.82642, "lon": 106.68930}, 
                "hoangvanthu_conghoa": {"lat": 10.80081, "lon": 106.66209},
                "truongchinh_tankitanquy": {"lat": 10.80418, "lon": 106.63588},
                "cmt8_truongson": {"lat": 10.78645, "lon": 106.66673}}

if __name__ == "__main__":
    """ Create driver for taking screenshot """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options = options)

    """ Create objects for processing images and fetching weather data """
    image_processor = Images_Processor(driver, camera_list)
    weather_data_fetcher = Weather_Fetcher(API_KEY = "c347802e3c6f24716a91d3cd7670433d", 
                                           lon_lat_dict = lon_lat_dict)

    """ Create producer for sending data to Kafka """
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    while True:

        vehicle_count_data = image_processor.implement()
        weather_data = weather_data_fetcher.implement()
        print(weather_data)
        
        """ Send data to Kafka """
        for data in vehicle_count_data:
            message = json.dumps(data)
            producer.produce("traffic_data", value = message)
        for data in weather_data:
            message = json.dumps(data)
            producer.produce("weather_data", value = message)
            
        # producer.produce("traffic_data", value = json.dumps(vehicle_count_data))
        # producer.produce("weather_data", value = json.dumps(weather_data))
        

            
        # with open('./data.json', 'a', encoding='utf-8') as f:
        #     json.dump(vehicle_count_data, f, ensure_ascii=False, indent=4)
        #     json.dump(weather_data, f, ensure_ascii=False, indent=4)
            
