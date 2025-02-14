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

st.session_state["last_update"] = time.time()
application = Frontend().run_streaming_dashboard()
 


    


# # Kết nối đến container Cassandra
# cluster = Cluster(['cassandra'])  # Hoặc IP của container nếu không phải localhost
# session = cluster.connect()
# rows = session.execute("SELECT * FROM traffic_weather_keyspace.stream_traffic_table")
# data = []
# for row in rows:
#     data.append(row)

# # Hiển thị dữ liệu
# df = pd.DataFrame(data)
# plt.figure(figsize=(10, 5))
# fig, ax = plt.subplots(figsize=(10, 6))
# sns.lineplot(x = 'execution_time', y = 'motorcycle', data = df, markers = 'o', color = 'red')
# plt.title('Số Lượng Giao Thông Theo Thời Gian', fontsize=14)
# plt.xlabel('Thời Gian', fontsize=12)
# plt.ylabel('Tổng Số Phương Tiện', fontsize=12)
# plt.xticks(rotation=45)
# plt.grid(True)
# st.pyplot(fig)