from selenium import webdriver
from datetime import datetime
import cv2
from ultralytics import YOLO
import matplotlib.pyplot as plt

""" 
    Create a class for taking screenshot 
    and processing image to count number of vehicles
"""
class Images_Processor:
    """ Constructor for Images_Processing class"""
    def __init__(self, driver: webdriver.Chrome, camera_list: list):
        self._driver = driver
        self._camera_list = camera_list
        self._model = YOLO("yolov8s.pt")

    """ Take screenshot of the street"""
    def screen_shot(self, street: str, link: str):
        self._driver.get(link)
        self._driver.set_window_size(1920, 1080) 
        self._driver.save_screenshot(f"./images_processing/screenshot_picture/screenshot_{street}.png")

    """ Image processing to count number of vehicles"""
    def image_processing(self, street) -> dict:
        #mapping with each vehicle id
        mapping = {1: 'bicycle', 2: 'car', 
                   3: 'motorcycle', 5: 'bus', 
                   7: 'truck', 9: 'traffic light', 
                   11: 'stop sign'}
        
        #initialize vehicle count
        vehicle_count = {'street': street, 'bicycle': 0, 
                    'car': 0, 'motorcycle': 0, 'bus': 0, 'truck': 0, 
                    'traffic light': 0, 'stop sign': 0,
                    'execution_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

        img = cv2.imread(f"./images_processing/screenshot_picture/screenshot_{street}.png")

        results = self._model(img, conf = 0.15)
        results = results[0]

        for box in results.boxes:
            class_id = int(box.cls)
            if class_id in mapping:
                vehicle_count[mapping[class_id]] += 1
        
                #x1, y1, x2, y2 = map(int, box.xyxy[0]) 
                #cv2.rectangle(img, (x1, y1), (x2, y2), (0, 255, 0), 2) 
    
        # for vehicle, cnt in vehicle_count.items():
        #     print(f"number of {vehicle}: {cnt}")

        #plt.imshow(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
        #plt.axis('off')
        #plt.show()

        return vehicle_count
    
    """ Implement function to take screenshot and process image"""
    def implement(self) -> list:
        
        vehicle_count_list = []

        for street, link in self._camera_list.items():
            self.screen_shot(street, link)
            vehicle_count = self.image_processing(street)
            vehicle_count_list.append(vehicle_count)

        return vehicle_count_list
