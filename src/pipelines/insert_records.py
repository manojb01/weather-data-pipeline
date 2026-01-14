import os
import pprint
import psycopg2
import time
from dotenv import load_dotenv
from src.pipelines.api_request import mock_fetch_data, fetch_data

# Load environment variables
load_dotenv()

# pprint.pprint(mock_fetch_data())

def connect_to_db():

    print("connecting to database")
 
    try:
        conn = psycopg2.connect(
            host = os.getenv("POSTGRES_HOST", "postgres"),
            port = int(os.getenv("POSTGRES_PORT", 5432)),
            dbname = os.getenv("POSTGRES_DB", "db"),
            user = os.getenv("POSTGRES_USER", "postgres"),
            password = os.getenv("POSTGRES_PASSWORD")
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise 

def create_table(conn):
    print("creating table if not exist")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
                id SERIAL PRIMARY KEY,
                -- Location data
                city TEXT,
                country TEXT,
                region TEXT,
                latitude FLOAT,
                longitude FLOAT,
                timezone_id TEXT,
                utc_offset TEXT,
                local_time TIMESTAMP,
                localtime_epoch BIGINT,

                -- Current weather data
                observation_time TEXT,
                temperature FLOAT,
                weather_code INT,
                weather_descriptions TEXT,
                weather_icon_url TEXT,
                is_day TEXT,

                -- Wind data
                wind_speed FLOAT,
                wind_degree INT,
                wind_dir TEXT,

                -- Atmospheric data
                pressure INT,
                precip FLOAT,
                humidity INT,
                cloudcover INT,
                feelslike FLOAT,
                uv_index INT,
                visibility INT,

                -- Astronomical data
                sunrise TEXT,
                sunset TEXT,
                moonrise TEXT,
                moonset TEXT,
                moon_phase TEXT,
                moon_illumination INT,

                -- Air quality data
                co FLOAT,
                no2 FLOAT,
                o3 FLOAT,
                so2 FLOAT,
                pm2_5 FLOAT,
                pm10 FLOAT,
                us_epa_index INT,
                gb_defra_index INT,

                -- Metadata
                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Table was created")
    except psycopg2.Error as e:
        print(f"failed to create table: {e}")
        raise

def insert_records(conn, data):
    print("Inserting weather data to database")
    try:
       weather = data['current']
       location = data['location']
       astro = weather.get('astro', {})
       air_quality = weather.get('air_quality', {})

       cursor = conn.cursor()
       cursor.execute("""
            INSERT INTO dev.raw_weather_data (
                -- Location data
                city,
                country,
                region,
                latitude,
                longitude,
                timezone_id,
                utc_offset,
                local_time,
                localtime_epoch,

                -- Current weather data
                observation_time,
                temperature,
                weather_code,
                weather_descriptions,
                weather_icon_url,
                is_day,

                -- Wind data
                wind_speed,
                wind_degree,
                wind_dir,

                -- Atmospheric data
                pressure,
                precip,
                humidity,
                cloudcover,
                feelslike,
                uv_index,
                visibility,

                -- Astronomical data
                sunrise,
                sunset,
                moonrise,
                moonset,
                moon_phase,
                moon_illumination,

                -- Air quality data
                co,
                no2,
                o3,
                so2,
                pm2_5,
                pm10,
                us_epa_index,
                gb_defra_index,

                -- Metadata
                inserted_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                NOW()
            )
        """,(
            # Location data
            location.get('name'),
            location.get('country'),
            location.get('region'),
            float(location.get('lat', 0)),
            float(location.get('lon', 0)),
            location.get('timezone_id'),
            location.get('utc_offset'),
            location.get('localtime'),
            location.get('localtime_epoch'),

            # Current weather data
            weather.get('observation_time'),
            weather.get('temperature'),
            weather.get('weather_code'),
            weather.get('weather_descriptions', [''])[0] if weather.get('weather_descriptions') else None,
            weather.get('weather_icons', [''])[0] if weather.get('weather_icons') else None,
            weather.get('is_day'),

            # Wind data
            weather.get('wind_speed'),
            weather.get('wind_degree'),
            weather.get('wind_dir'),

            # Atmospheric data
            weather.get('pressure'),
            weather.get('precip'),
            weather.get('humidity'),
            weather.get('cloudcover'),
            weather.get('feelslike'),
            weather.get('uv_index'),
            weather.get('visibility'),

            # Astronomical data
            astro.get('sunrise'),
            astro.get('sunset'),
            astro.get('moonrise'),
            astro.get('moonset'),
            astro.get('moon_phase'),
            astro.get('moon_illumination'),

            # Air quality data
            float(air_quality.get('co', 0)) if air_quality.get('co') else None,
            float(air_quality.get('no2', 0)) if air_quality.get('no2') else None,
            float(air_quality.get('o3', 0)) if air_quality.get('o3') else None,
            float(air_quality.get('so2', 0)) if air_quality.get('so2') else None,
            float(air_quality.get('pm2_5', 0)) if air_quality.get('pm2_5') else None,
            float(air_quality.get('pm10', 0)) if air_quality.get('pm10') else None,
            int(air_quality.get('us-epa-index', 0)) if air_quality.get('us-epa-index') else None,
            int(air_quality.get('gb-defra-index', 0)) if air_quality.get('gb-defra-index') else None
        ))
       conn.commit()
       print("data successfully inserted")
    except psycopg2.Error as e:
        print(f"error inserting data to database: {e}")
        raise

def main():
    conn = None
    try:
        conn = connect_to_db()
        create_table(conn)

        # Fetch and insert data for each city
        # Get city from env var, default to "New York"
        city = os.getenv("WEATHER_API_CITY", "New York")
        cities = [city]  # Support for single city, can be extended to multiple cities
        for idx, city in enumerate(cities):
            try:
                print(f"\n--- Processing {city} ---")
                data = fetch_data(city)
                insert_records(conn, data)
                print(f"Successfully processed {city}")

                # Add delay between API calls to avoid rate limiting (except for last city)
                if idx < len(cities) - 1:
                    print("Waiting 3 seconds before next API call...")
                    time.sleep(3)

            except Exception as e:
                print(f"Error processing {city}: {e}")
                # Continue with next city even if one fails
                continue

    except Exception as e:
        print(f"error occured during execution: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed")
