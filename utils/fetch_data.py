import requests
import json
import os
from datetime import datetime

# City coordinates for Open-Meteo API
CITY_COORDINATES = {
    "London": {"lat": 51.5074, "lon": -0.1278},
    "New York": {"lat": 40.7128, "lon": -74.0060},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503},
    "Sydney": {"lat": -33.8688, "lon": 151.2093},
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    # Add more cities as needed
}

# Function to fetch weather data from Open-Meteo API (no API key required)
def fetch_weather_data(city):
    """
    Fetch weather data for a given city using Open-Meteo API
    
    Args:
        city (str): Name of the city
        
    Returns:
        dict: Weather data including temperature, humidity, and pressure
    """
    try:
        # Check if city coordinates are available
        if city in CITY_COORDINATES:
            lat = CITY_COORDINATES[city]["lat"]
            lon = CITY_COORDINATES[city]["lon"]
        else:
            # For cities not in our predefined list, use a geocoding service or default coordinates
            # This is a simplified approach - in production, you'd use a proper geocoding service
            lat = 0
            lon = 0
            
            # Try to find coordinates using OpenWeatherMap API if API key is available
            api_key = os.environ.get("OPENWEATHER_API_KEY", "")
            if api_key:
                geocode_url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={api_key}"
                response = requests.get(geocode_url)
                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        lat = data[0]["lat"]
                        lon = data[0]["lon"]
                        # Add to our dictionary for future use
                        CITY_COORDINATES[city] = {"lat": lat, "lon": lon}
        
        # First try Open-Meteo API (no key required)
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,surface_pressure&timezone=auto"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extract the data from the response
            weather_data = {
                "temp": data["current"]["temperature_2m"],
                "humidity": data["current"]["relative_humidity_2m"],
                "pressure": data["current"]["surface_pressure"],
                "time": datetime.now()
            }
            
            return weather_data
        
        # Fallback to OpenWeatherMap if Open-Meteo fails and API key is available
        api_key = os.environ.get("OPENWEATHER_API_KEY", "")
        if api_key:
            url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                # Extract the data from the response
                weather_data = {
                    "temp": data["main"]["temp"],
                    "humidity": data["main"]["humidity"],
                    "pressure": data["main"]["pressure"],
                    "time": datetime.now()
                }
                
                return weather_data
        
        # If both APIs fail, raise an exception
        raise Exception(f"Failed to fetch weather data for {city}. Status code: {response.status_code}")
        
    except Exception as e:
        # For demo purposes, return mock data if API calls fail
        # In production, you'd want to handle this differently
        print(f"Error fetching data for {city}: {str(e)}")
        return {
            "temp": 20.0,  # Mock temperature in Celsius
            "humidity": 65.0,  # Mock humidity percentage
            "pressure": 1013.0,  # Mock pressure in hPa
            "time": datetime.now()
        }