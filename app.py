import streamlit as st
import pandas as pd
import plotly.express as px
import time
import datetime
import json
from kafka import KafkaConsumer
from utils.fetch_data import fetch_weather_data
from utils.data_processing import process_weather_data, detect_anomalies
from utils.hdfs_utils import get_historical_data

# Page configuration
st.set_page_config(
    page_title="Real-Time Weather Dashboard",
    page_icon="🌦️",
    layout="wide"
)

# Initialize session state for storing data
if 'weather_data' not in st.session_state:
    st.session_state.weather_data = pd.DataFrame(columns=['city', 'temp', 'humidity', 'pressure', 'time'])
    
if 'cities' not in st.session_state:
    st.session_state.cities = ["London", "New York", "Tokyo", "Sydney", "Paris"]
    
if 'selected_cities' not in st.session_state:
    st.session_state.selected_cities = ["London", "New York", "Tokyo"]
    
if 'refresh_interval' not in st.session_state:
    st.session_state.refresh_interval = 60  # seconds
    
if 'data_source' not in st.session_state:
    st.session_state.data_source = "kafka"  # Options: "kafka", "hdfs", "api"
    
if 'historical_days' not in st.session_state:
    st.session_state.historical_days = 7  # Default to 7 days of historical data

# Kafka Consumer setup
@st.cache_resource
def get_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            'weather_data',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='streamlit-dashboard',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000  # 1 second timeout
        )
        return consumer
    except Exception as e:
        st.warning(f"Could not connect to Kafka: {str(e)}")
        return None

# App title and description
st.title("🌦️ Real-Time Weather Dashboard (Kafka + Hadoop)")
st.markdown("This dashboard displays real-time weather data from Kafka streams and historical data from Hadoop HDFS.")

# Sidebar for settings
with st.sidebar:
    st.header("Settings")
    
    # Data source selection
    st.subheader("Data Source")
    data_source = st.radio(
        "Select data source",
        options=["Kafka (Real-time)", "HDFS (Historical)", "API (Direct)"],
        index=0
    )
    
    if "Kafka" in data_source:
        st.session_state.data_source = "kafka"
    elif "HDFS" in data_source:
        st.session_state.data_source = "hdfs"
    else:
        st.session_state.data_source = "api"
    
    # City selection
    st.subheader("Select Cities")
    selected_cities = st.multiselect(
        "Choose cities to monitor",
        options=st.session_state.cities,
        default=st.session_state.selected_cities
    )
    
    if selected_cities:
        st.session_state.selected_cities = selected_cities
    
    # Refresh interval (for Kafka and API)
    if st.session_state.data_source != "hdfs":
        st.subheader("Data Refresh")
        refresh_interval = st.slider(
            "Refresh interval (seconds)",
            min_value=30,
            max_value=300,
            value=st.session_state.refresh_interval,
            step=30
        )
        st.session_state.refresh_interval = refresh_interval
    
    # Historical data range (for HDFS)
    if st.session_state.data_source == "hdfs":
        st.subheader("Historical Data")
        historical_days = st.slider(
            "Days of historical data",
            min_value=1,
            max_value=30,
            value=st.session_state.historical_days
        )
        st.session_state.historical_days = historical_days
    
    # Add custom city
    st.subheader("Add Custom City")
    new_city = st.text_input("Enter city name")
    if st.button("Add City") and new_city and new_city not in st.session_state.cities:
        st.session_state.cities.append(new_city)
        st.success(f"Added {new_city} to the list!")
        st.experimental_rerun()

# Function to update data
def update_weather_data():
    if st.session_state.data_source == "kafka":
        # Fetch from Kafka
        with st.spinner("Fetching data from Kafka stream..."):
            consumer = get_kafka_consumer()
            if consumer:
                try:
                    # Poll for new messages
                    new_records = []
                    for message in consumer:
                        record = message.value
                        if isinstance(record, dict) and 'city' in record:
                            # Convert timestamp if present
                            if 'timestamp' in record:
                                record['time'] = pd.to_datetime(record['timestamp'])
                            elif 'time' in record:
                                record['time'] = pd.to_datetime(record['time'])
                            else:
                                record['time'] = pd.Timestamp.now()
                            
                            new_records.append(record)
                    
                    if new_records:
                        # Add to dataframe
                        new_df = pd.DataFrame(new_records)
                        st.session_state.weather_data = pd.concat([
                            st.session_state.weather_data, new_df
                        ], ignore_index=True)
                        st.success(f"Received {len(new_records)} new records from Kafka")
                    else:
                        st.info("No new data available from Kafka")
                        
                except Exception as e:
                    st.error(f"Error reading from Kafka: {str(e)}")
            else:
                # Fallback to API if Kafka is not available
                st.warning("Kafka connection failed. Falling back to direct API calls.")
                fetch_from_api()
    
    elif st.session_state.data_source == "hdfs":
        # Fetch from HDFS
        with st.spinner("Fetching historical data from HDFS..."):
            all_data = []
            for city in st.session_state.selected_cities:
                try:
                    city_data = get_historical_data(city, st.session_state.historical_days)
                    if not city_data.empty:
                        # Ensure column naming consistency
                        if 'timestamp' in city_data.columns and 'time' not in city_data.columns:
                            city_data = city_data.rename(columns={'timestamp': 'time'})
                        all_data.append(city_data)
                except Exception as e:
                    st.error(f"Error fetching HDFS data for {city}: {str(e)}")
            
            if all_data:
                st.session_state.weather_data = pd.concat(all_data, ignore_index=True)
                st.success(f"Loaded historical data from HDFS for {len(st.session_state.selected_cities)} cities")
            else:
                st.error("No historical data available from HDFS")
    
    else:
        # Fetch directly from API
        fetch_from_api()
    
    # Keep only the last 100 data points per city for memory efficiency
    if len(st.session_state.weather_data) > 0:
        st.session_state.weather_data = st.session_state.weather_data.groupby('city').apply(
            lambda x: x.nlargest(100, 'time')
        ).reset_index(drop=True)

# Helper function to fetch from API
def fetch_from_api():
    with st.spinner("Fetching weather data from API..."):
        for city in st.session_state.selected_cities:
            try:
                weather_data = fetch_weather_data(city)
                processed_data = process_weather_data(city, weather_data)
                
                # Add to dataframe
                st.session_state.weather_data = pd.concat([
                    st.session_state.weather_data,
                    pd.DataFrame([processed_data])
                ], ignore_index=True)
                
            except Exception as e:
                st.error(f"Error fetching data for {city}: {str(e)}")

# Main dashboard layout
st.subheader("Data Source Information")
data_source_info = {
    "kafka": "Showing real-time data from Kafka stream",
    "hdfs": f"Showing historical data from HDFS (last {st.session_state.historical_days} days)",
    "api": "Showing data directly from weather API"
}
st.info(data_source_info[st.session_state.data_source])

# Update button
if st.button("Refresh Data Now"):
    update_weather_data()

# Display data source status
if st.session_state.data_source == "kafka":
    consumer = get_kafka_consumer()
    if consumer:
        st.success("✅ Connected to Kafka")
    else:
        st.error("❌ Kafka connection failed")

# Tabs for different views
tab1, tab2, tab3 = st.tabs(["Current Weather", "Historical Trends", "Data Analysis"])

# Tab 1: Current weather metrics
with tab1:
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("Current Weather Metrics")
        
        # Display current metrics for each city
        if not st.session_state.weather_data.empty:
            # Get the latest data for each selected city
            latest_data = st.session_state.weather_data[
                st.session_state.weather_data['city'].isin(st.session_state.selected_cities)
            ].sort_values('time').groupby('city').last().reset_index()
            
            for _, row in latest_data.iterrows():
                with st.container():
                    st.markdown(f"### {row['city']}")
                    metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
                    
                    with metrics_col1:
                        st.metric("Temperature", f"{row['temp']}°C")
                    
                    with metrics_col2:
                        st.metric("Humidity", f"{row['humidity']}%")
                    
                    with metrics_col3:
                        st.metric("Pressure", f"{row['pressure']} hPa")
                    
                    st.caption(f"Last updated: {row['time'].strftime('%Y-%m-%d %H:%M:%S')}")
                    st.divider()
        else:
            st.info("No data available yet. Click 'Refresh Data Now' to fetch weather data.")
    
    with col2:
        st.subheader("Current Weather Comparison")
        if not st.session_state.weather_data.empty and len(st.session_state.selected_cities) > 0:
            latest_data = st.session_state.weather_data[
                st.session_state.weather_data['city'].isin(st.session_state.selected_cities)
            ].sort_values('time').groupby('city').last().reset_index()
            
            # Bar chart for temperature comparison
            fig_temp_bar = px.bar(
                latest_data,
                x='city',
                y='temp',
                title='Current Temperature by City',
                color='city',
                labels={'temp': 'Temperature (°C)', 'city': 'City'}
            )
            st.plotly_chart(fig_temp_bar, use_container_width=True)
            
            # Bar chart for humidity comparison
            fig_humidity_bar = px.bar(
                latest_data,
                x='city',
                y='humidity',
                title='Current Humidity by City',
                color='city',
                labels={'humidity': 'Humidity (%)', 'city': 'City'}
            )
            st.plotly_chart(fig_humidity_bar, use_container_width=True)
        else:
            st.info("No data available for comparison charts.")

# Tab 2: Historical trends
with tab2:
    st.subheader("Weather Trends Over Time")
    
    if not st.session_state.weather_data.empty and len(st.session_state.selected_cities) > 0:
        # Filter data for selected cities
        filtered_data = st.session_state.weather_data[
            st.session_state.weather_data['city'].isin(st.session_state.selected_cities)
        ]
        
        # Add date filter for historical data
        if st.session_state.data_source == "hdfs":
            min_date = filtered_data['time'].min().date()
            max_date = filtered_data['time'].max().date()
            
            date_range = st.date_input(
                "Select date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date
            )
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                filtered_data = filtered_data[
                    (filtered_data['time'].dt.date >= start_date) & 
                    (filtered_data['time'].dt.date <= end_date)
                ]
        
        # Temperature trend chart
        fig_temp = px.line(
            filtered_data, 
            x='time', 
            y='temp', 
            color='city',
            title='Temperature Trends',
            labels={'temp': 'Temperature (°C)', 'time': 'Time', 'city': 'City'}
        )
        st.plotly_chart(fig_temp, use_container_width=True)
        
        # Humidity trend chart
        fig_humidity = px.line(
            filtered_data, 
            x='time', 
            y='humidity', 
            color='city',
            title='Humidity Trends',
            labels={'humidity': 'Humidity (%)', 'time': 'Time', 'city': 'City'}
        )
        st.plotly_chart(fig_humidity, use_container_width=True)
        
        # Pressure trend chart
        fig_pressure = px.line(
            filtered_data, 
            x='time', 
            y='pressure', 
            color='city',
            title='Pressure Trends',
            labels={'pressure': 'Pressure (hPa)', 'time': 'Time', 'city': 'City'}
        )
        st.plotly_chart(fig_pressure, use_container_width=True)
    else:
        st.info("No data available for charts. Please select cities and refresh data.")

# Tab 3: Data Analysis
with tab3:
    st.subheader("Weather Data Analysis")
    
    analysis_type = st.radio(
        "Select Analysis Type",
        ["Statistical Summary", "Anomaly Detection", "Data Table View"]
    )
    
    if not st.session_state.weather_data.empty and len(st.session_state.selected_cities) > 0:
        # Filter data for selected cities
        filtered_data = st.session_state.weather_data[
            st.session_state.weather_data['city'].isin(st.session_state.selected_cities)
        ]
        
        if analysis_type == "Statistical Summary":
            st.write("Statistical summary of weather data by city:")
            
            # Group by city and calculate statistics
            stats = filtered_data.groupby('city').agg({
                'temp': ['mean', 'min', 'max', 'std'],
                'humidity': ['mean', 'min', 'max', 'std'],
                'pressure': ['mean', 'min', 'max', 'std']
            }).reset_index()
            
            # Format the stats dataframe for display
            stats.columns = ['_'.join(col).strip() for col in stats.columns.values]
            stats = stats.rename(columns={'city_': 'City'})
            
            st.dataframe(stats)
            
            # Show daily averages if historical data is available
            if st.session_state.data_source == "hdfs":
                st.write("Daily average temperature by city:")
                
                # Add date column
                filtered_data['date'] = filtered_data['time'].dt.date
                
                # Calculate daily averages
                daily_avg = filtered_data.groupby(['city', 'date']).agg({
                    'temp': 'mean',
                    'humidity': 'mean',
                    'pressure': 'mean'
                }).reset_index()
                
                # Plot daily temperature averages
                fig_daily = px.line(
                    daily_avg,
                    x='date',
                    y='temp',
                    color='city',
                    title='Daily Average Temperature',
                    labels={'temp': 'Avg Temperature (°C)', 'date': 'Date', 'city': 'City'}
                )
                st.plotly_chart(fig_daily, use_container_width=True)
        
        elif analysis_type == "Anomaly Detection":
            st.write("Anomaly detection in weather data:")
            
            anomalies = detect_anomalies(filtered_data)
            if not anomalies.empty:
                st.dataframe(anomalies)
                
                # Highlight anomalies on chart
                fig_anomalies = px.scatter(
                    anomalies,
                    x='time',
                    y='value',
                    color='metric',
                    title='Detected Anomalies',
                    labels={'value': 'Value', 'time': 'Time', 'metric': 'Metric'}
                )
                st.plotly_chart(fig_anomalies, use_container_width=True)
            else:
                st.info("No anomalies detected in the current data.")
        
        else:  # Data Table View
            st.write("Raw weather data:")
            st.dataframe(filtered_data)

# Auto-refresh mechanism
auto_refresh = st.checkbox("Enable Auto-Refresh", value=True)

if auto_refresh:
    refresh_placeholder = st.empty()
    
    # Calculate time until next refresh
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.datetime.now()
    
    time_since_refresh = (datetime.datetime.now() - st.session_state.last_refresh).total_seconds()
    time_until_refresh = max(0, st.session_state.refresh_interval - time_since_refresh)
    
    refresh_placeholder.info(f"Next auto-refresh in {int(time_until_refresh)} seconds")
    
    # Auto-refresh logic
    if time_until_refresh <= 0:
        update_weather_data()
        st.session_state.last_refresh = datetime.datetime.now()
        st.experimental_rerun()

# Footer
st.markdown("---")
st.caption("Real-Time Weather Dashboard | Data refreshes every {} seconds".format(st.session_state.refresh_interval))