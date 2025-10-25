import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from utils.fetch_data import fetch_weather_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('weather_kafka_producer')

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'weather_data'

# Cities to monitor
CITIES = ["London", "New York", "Tokyo", "Sydney", "Paris"]

# Fetch interval in seconds
FETCH_INTERVAL = 60

def create_kafka_producer():
    """Create and return a Kafka producer instance"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {str(e)}")
        raise

def send_weather_data(producer, city):
    """Fetch weather data for a city and send it to Kafka"""
    try:
        # Fetch weather data
        weather_data = fetch_weather_data(city)
        
        # Add timestamp and city
        weather_data['city'] = city
        weather_data['timestamp'] = datetime.now().isoformat()
        
        # Send to Kafka
        future = producer.send(KAFKA_TOPIC, weather_data)
        result = future.get(timeout=60)
        logger.info(f"Sent data for {city} to Kafka: {result}")
        
        return weather_data
    except Exception as e:
        logger.error(f"Error sending data for {city} to Kafka: {str(e)}")
        return None

def run_producer():
    """Main function to run the Kafka producer"""
    try:
        producer = create_kafka_producer()
        
        logger.info(f"Starting weather data producer for cities: {', '.join(CITIES)}")
        logger.info(f"Data will be sent to Kafka topic: {KAFKA_TOPIC}")
        logger.info(f"Fetch interval: {FETCH_INTERVAL} seconds")
        
        while True:
            for city in CITIES:
                send_weather_data(producer, city)
            
            logger.info(f"Sleeping for {FETCH_INTERVAL} seconds...")
            time.sleep(FETCH_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer error: {str(e)}")
    finally:
        if 'producer' in locals():
            producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    run_producer()