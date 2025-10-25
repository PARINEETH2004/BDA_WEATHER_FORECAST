# Real-Time Weather Data Stream Analysis (Kafka + Hadoop Version)

A comprehensive end-to-end big data pipeline that ingests, streams, stores, processes, and visualizes real-time weather data for multiple cities using Kafka, Hadoop HDFS, and Streamlit.

## Features

- **Real-Time Data Ingestion**: Pulls current weather metrics (temperature, humidity, pressure) from Open-Meteo or OpenWeatherMap API
- **Kafka Streaming**: Publishes and consumes weather data in real-time using Kafka
- **Hadoop Storage**: Stores historical weather data in HDFS for long-term analysis
- **Interactive Dashboard**: Uses Streamlit to visualize both real-time and historical data
- **Multi-City Support**: Displays data for several cities simultaneously
- **Historical Trend Analysis**: Aggregates and visualizes past weather trends from HDFS
- **Anomaly Detection**: Flags sudden temperature/humidity changes
- **Statistical Analysis**: Provides summary statistics and daily averages

## Setup Instructions

### Prerequisites

- Python 3.7 or higher
- pip (Python package installer)
- Kafka (local installation or Docker)
- Hadoop HDFS (local installation or Docker)

### Installation

1. Clone this repository or download the files
2. Navigate to the project directory
3. Install the required packages:

```bash
pip install -r requirements.txt
```

### Setting up Kafka

For local development, you can use Docker to set up Kafka:

```bash
docker run -d --name kafka-zookeeper -p 2181:2181 -p 9092:9092 -e ADVERTISED_HOST=localhost johnnypark/kafka-zookeeper
```

### Setting up Hadoop HDFS

For local development, you can use Docker to set up a single-node Hadoop:

```bash
docker run -d --name hadoop-container -p 9870:9870 -p 9000:9000 -p 8088:8088 bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
```

### Running the Application

1. Start the Kafka producer to ingest weather data:

```bash
python kafka_producer.py
```

2. Start the Kafka consumer to store data in HDFS:

```bash
python kafka_consumer.py
```

3. Run the Streamlit dashboard:

```bash
streamlit run app.py
```

The dashboard will open in your default web browser at `http://localhost:8501`.

### Optional: OpenWeatherMap API Key

The application uses Open-Meteo API by default, which doesn't require an API key. However, if you want to use OpenWeatherMap as a fallback:

1. Get a free API key from [OpenWeatherMap](https://openweathermap.org/api)
2. Set it as an environment variable:

```bash
# On Windows
set OPENWEATHER_API_KEY=your_api_key_here

# On macOS/Linux
export OPENWEATHER_API_KEY=your_api_key_here
```

## Usage

1. Choose your data source (Kafka, HDFS, or direct API) from the sidebar
2. Select cities to monitor from the dropdown menu
3. For real-time data (Kafka/API), adjust the refresh interval as needed
4. For historical data (HDFS), select the number of days to analyze
5. Add custom cities using the "Add Custom City" feature
6. Navigate between tabs to view:
   - Current Weather: Latest metrics and city comparisons
   - Historical Trends: Time-series charts with date filtering
   - Data Analysis: Statistical summaries, anomaly detection, and raw data

## Deployment

For production deployment:

1. Set up Kafka and Hadoop clusters in your environment
2. Configure the connection parameters in the scripts
3. Deploy the Streamlit dashboard on [Streamlit Cloud](https://streamlit.io/cloud)
4. Set up scheduled jobs for the Kafka producer and consumer

## Project Structure

- `app.py`: Main Streamlit dashboard application
- `kafka_producer.py`: Script to fetch weather data and publish to Kafka
- `kafka_consumer.py`: Script to consume Kafka messages and store in HDFS
- `utils/`
  - `fetch_data.py`: API integration for weather data
  - `data_processing.py`: Data processing and anomaly detection
  - `hdfs_utils.py`: Utilities for HDFS integration
- `requirements.txt`: Required Python packages
- `README.md`: Project documentation

## License

This project is open source and available under the MIT License.