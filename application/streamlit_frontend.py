from streamlit_backend import BackEnd
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
import plotly.express as px
from datetime import datetime, timedelta
import pytz
import time

class Frontend:
    """ Init. """
    def __init__(self):
        self._backend = BackEnd()
        self._street_name = {"Nguyen Thai Son - Phan Van Tri 2" : "nguyenthaison_phanvantri2",
                             "Hoang Van Thu - Cong Hoa" : "hoangvanthu_conghoa",
                             "Truong Chinh - Tan Ki Tan Quy" : "truongchinh_tankitanquy",
                             "Cach Mang Thang 8 - Truong Son" : "cmt8_truongson"}
        
    def create_sidebar(self) -> str:
        with st.sidebar:
            st.title("🚦Traffic and ⛅Weather Streaming Dashboard")
            
            selected_street = st.selectbox('Select a street', ["Nguyen Thai Son - Phan Van Tri 2",
                                                               "Hoang Van Thu - Cong Hoa",
                                                               "Truong Chinh - Tan Ki Tan Quy",
                                                               "Cach Mang Thang 8 - Truong Son"])

            return selected_street

    """------------------------------------ STREAMING -------------------------------------------"""
    def show_streaming_weather(self, weather_df: pd.DataFrame):
        weather_df = weather_df.iloc[-1]
        st.markdown("### **Weather**🌈")  
        st.write(f"⛅**Description**: {weather_df['weather_description'].title()}") 
        st.write(f"🌡️**Temperature**: {round(weather_df['temperature'], 2)} °C")
        st.write(f"💧**Humidity**: {weather_df['humidity']}%")
        st.write(f"🧭**Wind Direction**: {weather_df['wind_direction']}")
        st.write(f"💨**Wind Speed**: {round(weather_df['wind_speed'], 1)} m/s")
        st.markdown(f"🌬️**Pressure**: {round(weather_df['pressure'], 2)} hPa")
        st.markdown(f"🔥**Humidex**: {round(weather_df['humidex'], 2)} °C")
        st.markdown("- Humidex shows how hot it feels, considering temperature and humidity.")
        st.markdown(f"💥**Heat Index**: {round(weather_df['heat_index'], 2)} °C")
        st.markdown("- Heat Index combines temperature and humidity to indicate perceived heat and health risk.")


    def plot_streaming_traffic(self, traffic_df: pd.DataFrame):
        #get current time
        saigon_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        saigon_time = datetime.now(saigon_tz)
        time_interval = saigon_time - timedelta(minutes=10)
        time_interval = time_interval.strftime('%Y-%m-%d %H:%M:%S')

        traffic_df = traffic_df.drop(columns = ['street'])
        traffic_df = traffic_df[traffic_df['execution_time'] >= time_interval]
        traffic_df = traffic_df.set_index('execution_time')

        #plot line chart
        st.markdown('### Live Vehicle Count Estimation 🚗')
        st.line_chart(traffic_df)

    
    def plot_piechart_vehicle(self, traffic_df: pd.DataFrame):
        traffic_df = traffic_df.drop(columns = ['execution_time']).sum()

        #plot pie chart
        fig = px.pie(
            names = traffic_df.index,
            values = traffic_df.values,
            color = traffic_df.index,
            height = 400, 
            width = 400,
        )

        fig.update_layout(
            legend = dict(
                x = 0.1,  
                y = -0.05,  
                xanchor = 'center',
                yanchor = 'top')
        )

        fig.update_traces(
            hole=0.05,  
            pull=[0.05, 0.05, 0.05, 0.05, 0.05] 
        )
        #plot radar chart
        st.markdown("### Traffic Pie Chart 🚗")
        st.plotly_chart(fig, key = f"pie_chart_at_{time.time()}")

    
    def plot_vehicle_compare(self, traffic_df: pd.DataFrame):
        traffic_df = traffic_df.drop(columns = ['execution_time'])
        traffic_df = traffic_df.groupby('street')[['bicycle', 'bus', 'car', 'motorcycle', 'truck']].mean().reset_index()

        #plot bar chart
        st.markdown("### Average Vehicle Count 🚗")
        fig = px.bar(
            traffic_df,
            y = 'street',
            x = ['bicycle', 'bus', 'car', 'motorcycle', 'truck'],
            labels={"value": "Average Vehicle Count", "street": "Street"},
            height = 300,
            orientation = 'h'
        )

        fig.update_layout(
            legend = dict(
                x = -0.3,  
                y = -0.05,  
                xanchor = 'center',
                yanchor = 'top')
        )

        st.plotly_chart(fig, use_container_width = False, key = f"bar_chart_at_{time.time()}")

    """---------------------------------------------------------------------------------------"""

    """--------------------------------------- BATCH -----------------------------------------"""
    def plot_weather_trends(self, weather_df: pd.DataFrame):
        plot_weather_df = weather_df.copy()
        plot_weather_df['part_of_day'] = plot_weather_df['part_of_day']\
                            + ' ' + plot_weather_df['date'].dt.strftime('%d-%m-%Y')
        #plot
        st.write("### Daily Weather Chart ⛅")
        fig = px.line(
            plot_weather_df,
            x = 'part_of_day',
            y = ['avg_temperature', 'avg_humidex', 'avg_heat_index'],

        )
        st.plotly_chart(fig, key = f"line_chart_at_{time.time()}")

    def plot_traffic_trends(self, traffic_df: pd.DataFrame):
        plot_traffic_df = traffic_df.copy()
        plot_traffic_df['part_of_day'] = plot_traffic_df['part_of_day']\
                            + ' ' + plot_traffic_df['date'].dt.strftime('%d-%m-%Y')
        #plot
        fig = px.bar(
            plot_traffic_df,
            x = 'part_of_day',
            y = ['bicycle_per_observation', 'car_per_observation',
                 'motorcycle_per_observation', 'bus_per_observation',
                 'truck_per_observation']
        )
        st.write("### Daily Vehicle Count 🚗")
        st.plotly_chart(fig, key = f"bar_chart_at_{time.time()}")
        
    def run_streaming_dashboard(self):
        selected_street = self.create_sidebar()
        selected_street = self._street_name[selected_street]
        placeholder = st.empty()
        st.session_state.clear()
        st.session_state['last_update'] = time.time()
        #for batch data
        batch_weather_df = self._backend.get_batch_weather(selected_street)
        batch_weather_df['date'] = pd.to_datetime(batch_weather_df[['year','month','day']])

        batch_traffic_df = self._backend.get_batch_traffic(selected_street)
        batch_traffic_df['date'] = pd.to_datetime(batch_traffic_df[['year','month','day']])
        
        while True:
            with placeholder.container():
                col = st.columns((1.5, 4.5, 2), gap = 'medium')
                traffic_df = self._backend.get_stream_traffic(selected_street)
                traffic_fullstreet_df = self._backend.get_stream_traffic()
                weather_df = self._backend.get_stream_weather(selected_street)
                with col[0]: 
                    self.show_streaming_weather(weather_df)
                with col[1]:
                    self.plot_streaming_traffic(traffic_df)
                    self.plot_vehicle_compare(traffic_fullstreet_df)
                with col[2]:
                    self.plot_piechart_vehicle(traffic_df)
                self.plot_weather_trends(batch_weather_df) 
                self.plot_traffic_trends(batch_traffic_df)
                time.sleep(5)

            #rerun page
            if time.time() - st.session_state["last_update"] > 60:
                st.session_state.clear()  
                st.session_state["last_update"] = time.time()
                st.rerun()  