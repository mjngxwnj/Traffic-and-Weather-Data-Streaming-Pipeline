from cassandra.cluster import Cluster
from streamlit_frontend import Frontend
import streamlit as st
import time

st.set_page_config(
    page_title="Traffic And Weather Streaming Dashboard",
    page_icon="🚦⛅", 
    layout="wide",
    initial_sidebar_state="expanded"
)

application = Frontend().run_streaming_dashboard()