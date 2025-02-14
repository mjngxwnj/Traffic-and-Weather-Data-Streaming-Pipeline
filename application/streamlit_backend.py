from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import pytz

class BackEnd:
    def __init__(self):
        self._cluster = Cluster(['cassandra'])
        self._session = self._cluster.connect()


    def get_cassandra_data(self, query: str) -> pd.DataFrame:
        rows = self._session.execute(query) 
        df = pd.DataFrame(rows)

        return df
    

    def get_stream_traffic(self, street_name: str = None):
        query = """ 
                    SELECT street, execution_time, bicycle, 
                        bus, car, motorcycle, truck 
                    FROM traffic_weather_keyspace.stream_traffic_table
                """
        query += f""" WHERE street = '{street_name}' ALLOW FILTERING; """ \
                    if street_name is not None else ";"
        
        df = self.get_cassandra_data(query)
        
        return df
    
    def get_stream_weather(self, street_name):
        query = f"""
                    SELECT * 
                    FROM traffic_weather_keyspace.stream_weather_table
                    WHERE street = '{street_name}'
                    ALLOW FILTERING;
                """
        
        df = self.get_cassandra_data(query)
        
        return df

    def get_batch_traffic(self, street_name):
        query = f"""
                    SELECT *
                    FROM traffic_weather_keyspace.batch_traffic_table
                    WHERE street = '{street_name}'
                    ALLOW FILTERING;
                """

        df = self.get_cassandra_data(query)
        
        return df


    def get_batch_weather(self, street_name):
        query = f"""
                    SELECT * 
                    FROM traffic_weather_keyspace.batch_weather_table
                    WHERE street = '{street_name}'
                    ALLOW FILTERING;
                """
        
        df = self.get_cassandra_data(query)
        
        return df
    

    # def get_stream_total_vehicle_count(self, street_name):
    #     query = f"""
    #                 SUM(bicycle) AS total_bicycle, 
    #                 SUM(bus) AS total_bus, 
    #                 SUM(car) AS total_car, 
    #                 SUM(motorcycle) AS total_motorcycle, 
    #                 SUM(truck) AS total_truck
    #             FROM traffic_weather_keyspace.stream_traffic_table
    #             WHERE street = '{street_name}'
    #             ALLOW FILTERING;
    #             """
    #     rows = self._session.execute(query)
    #     df = pd.DataFrame(rows)

    #     return df
    

    def get_stream_weather_data(self):
        rows = self._session.execute("SELECT * FROM traffic_weather_keyspace.stream_weather_table")
        data = []
        for row in rows:
            data.append(row)
        return data
