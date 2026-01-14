import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add /opt/airflow to sys.path so we can import src modules
sys.path.append('/opt/airflow')

try:
    from src.pipelines.api_request import fetch_data

    # Get configuration from environment
    base_url = os.getenv("WEATHER_API_BASE_URL", "http://api.weatherstack.com/current")
    api_key = os.getenv("WEATHER_API_KEY")
    city = os.getenv("WEATHER_API_CITY", "New York")

    print(f"Base URL: {base_url}")
    print(f"API Key present: {bool(api_key)}")
    print(f"Testing with city: {city}")

    data = fetch_data(city)

    if data:
        print("Keys in response:", list(data.keys()))
        if 'forecast' in data:
            print("Forecast data FOUND")
        else:
            print("Forecast data NOT found")
except Exception as e:
    print(f"Error: {e}")
