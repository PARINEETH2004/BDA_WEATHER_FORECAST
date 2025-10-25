import os
import logging
import pandas as pd
from hdfs import InsecureClient
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('hdfs_utils')

# HDFS configuration
HDFS_URL = 'http://localhost:9870'
HDFS_USER = 'hdfs'
HDFS_BASE_PATH = '/weather_data'

# Local data path for caching
LOCAL_DATA_PATH = 'data/cache'

def get_hdfs_client():
    """Create and return an HDFS client"""
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        return client
    except Exception as e:
        logger.error(f"Failed to create HDFS client: {str(e)}")
        # For development/demo, provide a fallback mechanism
        logger.warning("Using local file system as fallback")
        return None

def ensure_local_directory(path):
    """Ensure the local directory exists"""
    os.makedirs(path, exist_ok=True)

def read_from_hdfs(city, days=1):
    """
    Read weather data for a city from HDFS
    
    Args:
        city (str): City name
        days (int): Number of days of data to retrieve
        
    Returns:
        DataFrame: Weather data for the specified city and time period
    """
    try:
        client = get_hdfs_client()
        
        if client is None:
            # Fallback to local files if HDFS is not available
            return read_from_local_fallback(city, days)
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Path to city data in HDFS
        hdfs_city_path = f"{HDFS_BASE_PATH}/{city}"
        
        # Check if directory exists
        if not client.status(hdfs_city_path, strict=False):
            logger.warning(f"No data found for {city} in HDFS")
            return pd.DataFrame()
        
        # List all files in the directory
        files = client.list(hdfs_city_path)
        
        # Filter files by date if needed
        # This is a simple implementation - in production you might want more sophisticated filtering
        
        # Read and combine all files
        dfs = []
        for file in files:
            if file.endswith('.parquet'):
                hdfs_file_path = f"{hdfs_city_path}/{file}"
                
                # Download to local cache
                ensure_local_directory(LOCAL_DATA_PATH)
                local_file_path = f"{LOCAL_DATA_PATH}/{file}"
                
                with open(local_file_path, 'wb') as f:
                    client.download(hdfs_file_path, f)
                
                # Read parquet file
                df = pd.read_parquet(local_file_path)
                dfs.append(df)
                
                # Clean up
                os.remove(local_file_path)
        
        if not dfs:
            logger.warning(f"No parquet files found for {city} in HDFS")
            return pd.DataFrame()
        
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Convert timestamp to datetime if it's not already
        if 'timestamp' in combined_df.columns:
            combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
            
            # Filter by date range
            combined_df = combined_df[
                (combined_df['timestamp'] >= start_date) & 
                (combined_df['timestamp'] <= end_date)
            ]
        
        return combined_df
    
    except Exception as e:
        logger.error(f"Error reading from HDFS: {str(e)}")
        return read_from_local_fallback(city, days)

def read_from_local_fallback(city, days=1):
    """
    Fallback function to read from local CSV files when HDFS is not available
    
    Args:
        city (str): City name
        days (int): Number of days of data to retrieve
        
    Returns:
        DataFrame: Weather data from local storage
    """
    try:
        # Path to local data
        local_data_path = f"data/local_storage/{city}.csv"
        
        if not os.path.exists(local_data_path):
            logger.warning(f"No local data found for {city}")
            return pd.DataFrame()
        
        # Read CSV
        df = pd.read_csv(local_data_path)
        
        # Convert timestamp to datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Filter by date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            df = df[
                (df['timestamp'] >= start_date) & 
                (df['timestamp'] <= end_date)
            ]
        
        return df
    
    except Exception as e:
        logger.error(f"Error reading from local fallback: {str(e)}")
        return pd.DataFrame()

def save_to_local_storage(df, city):
    """
    Save DataFrame to local storage as CSV
    
    Args:
        df (DataFrame): Data to save
        city (str): City name
    """
    try:
        # Ensure directory exists
        os.makedirs('data/local_storage', exist_ok=True)
        
        # Path to local data
        local_data_path = f"data/local_storage/{city}.csv"
        
        # If file exists, read and append
        if os.path.exists(local_data_path):
            existing_df = pd.read_csv(local_data_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            
            # Remove duplicates if any
            if 'timestamp' in combined_df.columns:
                combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'])
                combined_df = combined_df.drop_duplicates(subset=['timestamp'])
            
            combined_df.to_csv(local_data_path, index=False)
        else:
            # Save new file
            df.to_csv(local_data_path, index=False)
        
        logger.info(f"Saved data to local storage: {local_data_path}")
    
    except Exception as e:
        logger.error(f"Error saving to local storage: {str(e)}")

def get_historical_data(city, days=7):
    """
    Get historical weather data for a city
    
    Args:
        city (str): City name
        days (int): Number of days of historical data
        
    Returns:
        DataFrame: Historical weather data
    """
    # Try to read from HDFS first
    df = read_from_hdfs(city, days)
    
    # If no data from HDFS, try local fallback
    if df.empty:
        df = read_from_local_fallback(city, days)
    
    return df