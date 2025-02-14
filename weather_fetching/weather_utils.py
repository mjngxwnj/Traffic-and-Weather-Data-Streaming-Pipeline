import requests
from datetime import datetime

""" Class to fetch weather data from OpenWeatherMap API """
class Weather_Fetcher:
    """ Initialize the class with API_KEY and a dictionary """
    def __init__(self, API_KEY: str, lon_lat_dict: dict):
        self._API_KEY = API_KEY
        self._lon_lat_dict = lon_lat_dict
        
    """ Function to fetch weather data from OpenWeatherMap API"""
    def fetch_weather_data(self, longitude: float, latitude: float) -> dict:
        """
        Args:
            longitude: longitude of the location
            latitude: latitude of the location
        Returns:
            weather data in json format    
        """
        
        # API call
        API_CALL = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={self._API_KEY}"
        
        # Get response
        response = requests.get(API_CALL)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    """ Function to parse weather data"""
    def parse_weather_data(self, response) -> dict:
        """
        Args:
            response: response from the API call
        Returns:
            weather data in dictionary format
        """

        #initialize variables
        if response:
            main = response['weather'][0]['main']
            description = response['weather'][0]['description']
            temp = response['main']['temp']
            feels_like = response['main']['feels_like']
            temp_min = response['main']['temp_min']
            temp_max = response['main']['temp_max']
            pressure = response['main']['pressure']
            humidity = response['main']['humidity']
            wind_speed = response['wind']['speed']
            wind_deg = response['wind']['deg']
        
            #create a dictionary to store weather data
            weather_data = {
                "main": main,
                "description": description,
                "temp": temp,
                "feels_like": feels_like,
                "temp_min": temp_min,
                "temp_max": temp_max,
                "pressure": pressure,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "wind_deg": wind_deg
            }

        #return weather data
        return weather_data

    """ Function to get weather data for a list of locations"""
    def implement(self) -> list:
        """
        Returns:
            weather data for a list of locations
        """

        #initialize variables
        weather_data_list = []
        
        for street_name, coordinates in self._lon_lat_dict.items():
            #fetch weather data
            response = self.fetch_weather_data(coordinates['lon'], coordinates['lat'])
            weather_data = self.parse_weather_data(response)
            weather_data['street'] = street_name
            weather_data['execution_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            weather_data_list.append(weather_data)

        return weather_data_list