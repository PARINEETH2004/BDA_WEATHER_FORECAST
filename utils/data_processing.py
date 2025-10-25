import pandas as pd
import numpy as np
from datetime import datetime

def process_weather_data(city, weather_data):
    """
    Process raw weather data into a format suitable for the dashboard
    
    Args:
        city (str): Name of the city
        weather_data (dict): Raw weather data from API
        
    Returns:
        dict: Processed weather data
    """
    # Create a processed data record
    processed_data = {
        'city': city,
        'temp': round(weather_data['temp'], 1),
        'humidity': round(weather_data['humidity'], 1),
        'pressure': round(weather_data['pressure'], 1),
        'time': weather_data['time']
    }
    
    return processed_data

def detect_anomalies(df, window_size=5, std_threshold=2.0):
    """
    Detect anomalies in weather data using a simple statistical approach
    
    Args:
        df (DataFrame): Weather data DataFrame
        window_size (int): Size of the rolling window for calculating statistics
        std_threshold (float): Number of standard deviations to consider as anomaly
        
    Returns:
        DataFrame: DataFrame containing detected anomalies
    """
    if len(df) < window_size:
        return pd.DataFrame()  # Not enough data for anomaly detection
    
    # Create a copy to avoid modifying the original DataFrame
    data = df.copy()
    
    # Initialize empty DataFrame for anomalies
    anomalies = pd.DataFrame(columns=df.columns)
    
    # Process each city separately
    for city in data['city'].unique():
        city_data = data[data['city'] == city].sort_values('time')
        
        if len(city_data) < window_size:
            continue
        
        # Calculate rolling statistics for each metric
        for metric in ['temp', 'humidity', 'pressure']:
            # Calculate rolling mean and standard deviation
            rolling_mean = city_data[metric].rolling(window=window_size).mean()
            rolling_std = city_data[metric].rolling(window=window_size).std()
            
            # Skip the first window_size-1 rows where rolling stats are NaN
            for i in range(window_size, len(city_data)):
                current_value = city_data.iloc[i][metric]
                mean = rolling_mean.iloc[i]
                std = rolling_std.iloc[i]
                
                # Check if the value is an anomaly
                if std > 0 and abs(current_value - mean) > std_threshold * std:
                    anomaly_row = city_data.iloc[i].copy()
                    anomaly_row['anomaly_type'] = f'{metric}_anomaly'
                    anomaly_row['expected_value'] = mean
                    anomaly_row['deviation'] = abs(current_value - mean) / std
                    
                    # Add to anomalies DataFrame
                    anomalies = pd.concat([anomalies, pd.DataFrame([anomaly_row])], ignore_index=True)
    
    return anomalies