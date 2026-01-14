import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get configuration from environment
api_key = os.getenv("WEATHER_API_KEY")
base_url = os.getenv("WEATHER_API_BASE_URL", "http://api.weatherstack.com/current")

def mock_fetch_data(city="New York"):
    """Return mock data for New York to bypass API limits."""
    # Simulated data based on user example
    return {
        "request": {
            "type": "City",
            "query": "New York, United States of America",
            "language": "en",
            "unit": "m"
        },
        "location": {
            "name": "New York",
            "country": "United States of America",
            "region": "New York",
            "lat": "40.714",
            "lon": "-74.006",
            "timezone_id": "America/New_York",
            "localtime": "2025-01-10 08:14", # Updated to recent date
            "localtime_epoch": 1736500440,
            "utc_offset": "-4.0"
        },
        "current": {
            "observation_time": "12:14 PM",
            "temperature": 13,
            "weather_code": 113,
            "weather_icons": [
                "https://assets.weatherstack.com/images/wsymbols01_png_64/wsymbol_0001_sunny.png"
            ],
            "weather_descriptions": [
                "Sunny"
            ],
            "astro": {
                "sunrise": "06:31 AM",
                "sunset": "05:47 PM",
                "moonrise": "06:56 AM",
                "moonset": "06:47 PM",
                "moon_phase": "Waxing Crescent",
                "moon_illumination": 0
            },
            "air_quality": {
                "co": "468.05",
                "no2": "32.005",
                "o3": "55",
                "so2": "7.4",
                "pm2_5": "6.66",
                "pm10": "6.66",
                "us-epa-index": "1",
                "gb-defra-index": "1"
            },
            "wind_speed": 12,
            "wind_degree": 349,
            "wind_dir": "N",
            "pressure": 1010,
            "precip": 0,
            "humidity": 90,
            "cloudcover": 0,
            "feelslike": 13,
            "uv_index": 4,
            "visibility": 16,
            "is_day": "yes"
        }
    }

def fetch_data(city):
    """Fetch weather data for a specific city"""
    url = f"{base_url}?access_key={api_key}&query={city}"
    print(f"Fetching data for {city}")
    try:
        response = requests.get(url)
        # Check for success field in JSON (API returns 200 even for errors)
        data = response.json()
        if data.get('success') is False:
             print(f"API Error for {city}: {data.get('error')}")
             print("Falling back to mock data due to API error...")
             return mock_fetch_data(city)
             
        response.raise_for_status()
        print(f"API response received successfully for {city}")
        return data

    except requests.RequestException as e :
        print(f"An error occured {e}")
        print("Falling back to mock data due to connection error...")
        return mock_fetch_data(city)
