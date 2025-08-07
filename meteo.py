import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os
import time
import numpy as np

# Set working directory to script's directory
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir == "" else filedir)

import config
csvdir = "meteocsv/"

# Define locations with coordinates, timezones, and CSV paths
LOCATIONS = [
    {
        "name": "Ganges",
        "latitude": 43.94,
        "longitude": 3.71,
        "timezone": "Europe/Paris",
        "csv_file": "ganges_weather.csv"
    },
    {
        "name": "Étel",
        "latitude": 47.66,
        "longitude": -3.20,
        "timezone": "Europe/Paris",
        "csv_file": "etel_weather.csv"
    },
    {
        "name": "Plomari",
        "latitude": 39.01,
        "longitude": 26.37,
        "timezone": "Europe/Athens",
        "csv_file": "plomari_weather.csv"
    },
    {
        "name": "Paris",
        "latitude": 48.8566,
        "longitude": 2.3522,
        "timezone": "Europe/Paris",
        "csv_file": "paris_weather.csv"
    },
    {
        "name": "Atlanta",
        "latitude": 33.7490,
        "longitude": -84.3880,
        "timezone": "America/New_York",
        "csv_file": "atlanta_weather.csv"
    },
    {
        "name": "Dallas",
        "latitude": 32.7767,
        "longitude": -96.7970,
        "timezone": "America/Chicago",
        "csv_file": "dallas_weather.csv"
    },
    {
        "name": "Phoenix",
        "latitude": 33.4484,
        "longitude": -112.0740,
        "timezone": "America/Phoenix",
        "csv_file": "phoenix_weather.csv"
    },
    {
        "name": "Kansas City",
        "latitude": 39.0997,
        "longitude": -94.5786,
        "timezone": "America/Chicago",
        "csv_file": "kansas_city_weather.csv"
    },
    {
        "name": "Charlotte",
        "latitude": 35.2271,
        "longitude": -80.8431,
        "timezone": "America/New_York",
        "csv_file": "charlotte_weather.csv"
    },
    {
        "name": "Fort Lauderdale",
        "latitude": 26.1224,
        "longitude": -80.1373,
        "timezone": "America/New_York",
        "csv_file": "fort_lauderdale_weather.csv"
    },
    {
        "name": "Hong Kong",
        "latitude": 22.3193,
        "longitude": 114.1694,
        "timezone": "Asia/Hong_Kong",
        "csv_file": "hong_kong_weather.csv"
    },
    {
        "name": "Pattaya",
        "latitude": 12.9357,
        "longitude": 100.8890,
        "timezone": "Asia/Bangkok",
        "csv_file": "pattaya_weather.csv"
    },
    {
        "name": "Tokyo",
        "latitude": 35.6762,
        "longitude": 139.6503,
        "timezone": "Asia/Tokyo",
        "csv_file": "tokyo_weather.csv"
    },
    {
        "name": "Asunción",
        "latitude": -25.2637,
        "longitude": -57.5759,
        "timezone": "America/Asuncion",
        "csv_file": "asuncion_weather.csv"
    },
    {
        "name": "Istanbul",
        "latitude": 41.0082,
        "longitude": 28.9784,
        "timezone": "Europe/Istanbul",
        "csv_file": "istanbul_weather.csv"
    },
    {
        "name": "Dubai",
        "latitude": 25.2048,
        "longitude": 55.2708,
        "timezone": "Asia/Dubai",
        "csv_file": "dubai_weather.csv"
    }
]

# Open-Meteo API endpoints
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

def sendTelegram(text):
    """Send a message via Telegram, splitting if over 4000 characters."""
    maxlen = 4000
    if len(text) < maxlen:
        time.sleep(0.1)
        params = {
            'chat_id': config.telegramchatid,
            'text': os.uname()[1] + ":" + __file__ + ":" + text,
            'parse_mode': 'markdown'
        }
        resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
        if resp.status_code == 400:
            print(f"ERR: {len(text)}")
            print(text)
        resp.raise_for_status()
    else:
        idx = text.rfind("\n", 0, maxlen)
        if idx == -1:
            idx = maxlen - 1
        sendTelegram(text[:idx])
        sendTelegram(text[idx:])

def fetch_weather_data(lat, lon, timezone):
    """Fetch weather data from the forecast API."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,pressure_msl_mean,relative_humidity_2m_mean",
        "timezone": timezone,
        "past_days": 1,
        "forecast_days": 0
    }
    response = requests.get(WEATHER_API_URL, params=params)
    response.raise_for_status()
    return response.json()

def fetch_air_quality_data(lat, lon, timezone):
    """Fetch PM10 and PM2.5 data from the air quality API."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5",
        "timezone": timezone,
        "past_days": 1
    }
    response = requests.get(AIR_QUALITY_API_URL, params=params)
    response.raise_for_status()
    return response.json()

def save_to_csv(data, air_quality_data, csv_file, timezone, location_name):
    """Save weather and air quality data to CSV, removing duplicates by Date."""
    # Weather data
    daily = data["daily"]
    dates = daily["time"]
    temp_max = daily["temperature_2m_max"]
    temp_min = daily["temperature_2m_min"]
    precipitation = daily["precipitation_sum"]
    pressure = daily["pressure_msl_mean"]
    humidity = daily["relative_humidity_2m_mean"]

    # Air quality data
    hourly = air_quality_data["hourly"]
    hourly_times = hourly["time"]
    pm10 = hourly["pm10"]
    pm2_5 = hourly["pm2_5"]

    # Calculate yesterday in the location's timezone
    tz = pytz.timezone(timezone)
    now_tz = datetime.now(tz)
    yesterday = (now_tz - timedelta(days=1)).strftime("%Y-%m-%d")

    # Aggregate hourly PM10 and PM2.5 to daily mean
    pm10_daily = []
    pm2_5_daily = []
    for date in dates:
        if date == yesterday:
            # Extract hourly data for the given date
            indices = [i for i, t in enumerate(hourly_times) if t.startswith(date)]
            pm10_values = [pm10[i] for i in indices if pm10[i] is not None]
            pm2_5_values = [pm2_5[i] for i in indices if pm2_5[i] is not None]
            pm10_mean = np.mean(pm10_values) if pm10_values else None
            pm2_5_mean = np.mean(pm2_5_values) if pm2_5_values else None
            pm10_daily.append(pm10_mean)
            pm2_5_daily.append(pm2_5_mean)
        else:
            pm10_daily.append(None)
            pm2_5_daily.append(None)

    print(f"{location_name} - API returned dates: {dates}")
    print(f"{location_name} - Calculated yesterday ({timezone}): {yesterday}")

    # Filter for yesterday's data
    filtered_data = [
        {
            "Date": date,
            "Max_Temperature_C": t_max,
            "Min_Temperature_C": t_min,
            "Precipitation_mm": precip,
            "Mean_Pressure_hPa": press,
            "Mean_Humidity_Pct": hum,
            "Mean_PM10_ugm3": pm10_mean,
            "Mean_PM25_ugm3": pm2_5_mean
        }
        for date, t_max, t_min, precip, press, hum, pm10_mean, pm2_5_mean in zip(
            dates, temp_max, temp_min, precipitation, pressure, humidity, pm10_daily, pm2_5_daily
        )
        if date == yesterday
    ]

    if not filtered_data:
        print(f"{location_name} - No data for yesterday ({yesterday}) found in API response.")
        return None

    # Create DataFrame
    df = pd.DataFrame(filtered_data)

    # Append to CSV, removing duplicates by Date
    if os.path.exists(csv_file):
        df_existing = pd.read_csv(csv_file)
        # Ensure new columns exist in existing CSV
        for col in ["Mean_PM10_ugm3", "Mean_PM25_ugm3"]:
            if col not in df_existing.columns:
                df_existing[col] = None
        df = pd.concat([df_existing, df], ignore_index=True).drop_duplicates(subset="Date", keep="first")
    df.to_csv(csv_file, index=False)
    print(f"{location_name} - Data saved to {csv_file} for {yesterday}")

    # Return the latest data for summary
    return filtered_data[0]

def main():
    try:
        # Collect summary data for Telegram message
        summary_lines = []
        for location in LOCATIONS:
            name = location["name"]
            lat = location["latitude"]
            lon = location["longitude"]
            timezone = location["timezone"]
            csv_file = csvdir+location["csv_file"]

            print(f"\nProcessing {name}...")
            print(f"Using coordinates: Latitude={lat}, Longitude={lon}")

            # Fetch weather and air quality data
            weather_data = fetch_weather_data(lat, lon, timezone)
            air_quality_data = fetch_air_quality_data(lat, lon, timezone)
            combined_data = save_to_csv(weather_data, air_quality_data, csv_file, timezone, name)

            # Add to summary if data was saved
            if combined_data:
                summary_lines.append(
                    f"{name}: Min {combined_data['Min_Temperature_C']}°C, "
                    f"Max {combined_data['Max_Temperature_C']}°C, "
                    f"Precip {combined_data['Precipitation_mm']}mm, "
                    f"Press {combined_data['Mean_Pressure_hPa']}hPa, "
                    f"Hum {combined_data['Mean_Humidity_Pct']}%, "
                    f"PM10 {combined_data['Mean_PM10_ugm3']:.1f}µg/m³, "
                    f"PM2.5 {combined_data['Mean_PM25_ugm3']:.1f}µg/m³"
                )

        # Send Telegram summary
        if summary_lines:
            tz = pytz.timezone("Europe/Paris")
            yesterday = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")
            summary_text = f"Weather & Air Quality Summary for {yesterday}:\n" + "\n".join(summary_lines)
            #sendTelegram(summary_text)
        else:
            sendTelegram("No data available for yesterday.")

    except Exception as e:
        error_msg = f"Error in meteo.py: {str(e)}"
        print(error_msg)
        sendTelegram(error_msg)

if __name__ == "__main__":
    main()
