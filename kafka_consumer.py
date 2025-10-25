import json
import logging
import time
from datetime import datetime
from kafka import KafkaConsumer
from hdfs import InsecureClient
import pandas as pd
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_kafka_consumer')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'weather_data'
KAFKA_GROUP_ID = 'weather_consumer_group'

# HDFS configuration
HDFS_URL = 'http://localhost:9870'
HDFS_USER = 'hdfs'
HDFS_BASE_PATH = '/weather_data'

# Local buffer configuration
LOCAL_BUFFER_PATH = 'data/buffer'
BUFFER_FLUSH_COUNT = 50  # Number of records before writing to HDFS

def create_kafka_consumer():
    """Create and return a Kafka consumer instance"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=True,
            auto_commit_interval_ms=5000
        )
        logger.info(f"Kafka consumer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {str(e)}")
        raise

def create_hdfs_client():
    """Create and return an HDFS client instance"""
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        logger.info(f"HDFS client connected to {HDFS_URL}")
        return client
    except Exception as e:
        logger.error(f"Failed to create HDFS client: {str(e)}")
        raise

def ensure_hdfs_directory(client, path):
    """Ensure the HDFS directory exists"""
    try:
        if not client.status(path, strict=False):
            client.makedirs(path)
            logger.info(f"Created HDFS directory: {path}")
    except Exception as e:
        logger.error(f"Failed to create HDFS directory {path}: {str(e)}")
        raise

def ensure_local_directory(path):
    """Ensure the local directory exists"""
    try:
        os.makedirs(path, exist_ok=True)
        logger.info(f"Ensured local directory exists: {path}")
    except Exception as e:
        logger.error(f"Failed to create local directory {path}: {str(e)}")
        raise

def write_to_hdfs(client, df, city):
    """Write DataFrame to HDFS as Parquet file"""
    try:
        # Format date for filename
        date_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        hdfs_path = f"{HDFS_BASE_PATH}/{city}/{date_str}.parquet"
        
        # Convert DataFrame to Parquet and save locally first
        local_path = f"{LOCAL_BUFFER_PATH}/{city}_{date_str}.parquet"
        df.to_parquet(local_path, index=False)
        
        # Upload to HDFS
        with open(local_path, 'rb') as f:
            client.write(hdfs_path, f)
        
        # Remove local file after upload
        os.remove(local_path)
        
        logger.info(f"Wrote {len(df)} records to HDFS: {hdfs_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to write to HDFS: {str(e)}")
        return False

def run_consumer():
    """Main function to run the Kafka consumer"""
    try:
        consumer = create_kafka_consumer()
        hdfs_client = create_hdfs_client()
        
        # Ensure directories exist
        ensure_local_directory(LOCAL_BUFFER_PATH)
        ensure_hdfs_directory(hdfs_client, HDFS_BASE_PATH)
        
        # Buffer for collecting messages before writing to HDFS
        buffer = []
        
        logger.info(f"Starting weather data consumer for topic: {KAFKA_TOPIC}")
        logger.info(f"Data will be written to HDFS at: {HDFS_BASE_PATH}")
        
        for message in consumer:
            try:
                # Process message
                data = message.value
                logger.debug(f"Received message: {data}")
                
                # Add to buffer
                buffer.append(data)
                
                # If buffer reaches threshold, write to HDFS
                if len(buffer) >= BUFFER_FLUSH_COUNT:
                    df = pd.DataFrame(buffer)
                    
                    # Group by city and write separate files
                    for city, city_df in df.groupby('city'):
                        ensure_hdfs_directory(hdfs_client, f"{HDFS_BASE_PATH}/{city}")
                        write_to_hdfs(hdfs_client, city_df, city)
                    
                    # Clear buffer
                    buffer = []
            
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
    
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Consumer error: {str(e)}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            logger.info("Kafka consumer closed")

if __name__ == "__main__":
    run_consumer()