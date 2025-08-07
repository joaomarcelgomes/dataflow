import requests
import json
import logging
import pandas as pd
from confluent_kafka import Producer
from datetime import datetime

logging.basicConfig(
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    level=logging.INFO
)

# API config
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": -5.1783,
    "longitude": -40.6775,
    "hourly": [
        "temperature_2m", "relative_humidity_2m", "apparent_temperature",
        "precipitation_probability", "rain", "evapotranspiration",
        "soil_temperature_0cm", "soil_moisture_0_to_1cm", "wind_speed_10m"
    ],
    "past_days": 1,
    "timezone": "America/Fortaleza"
}

def transform_weather_data(data):
    df = pd.DataFrame(data)
    df["time"] = pd.to_datetime(df["time"])
    df["date"] = df["time"].dt.date

    results = []
    for full_date, group in df.groupby("date"):
        formatted_date = full_date.strftime("%d-%m")
        weekday = full_date.strftime("%a")

        max_temp = round(group["temperature_2m"].max(), 1)
        min_temp = round(group["temperature_2m"].min(), 1)
        avg_humidity = int(round(group["relative_humidity_2m"].mean()))
        avg_cloudiness = 0.0  # not provided
        total_rain = round(group["rain"].sum(), 1)
        max_rain_prob = int(group["precipitation_probability"].max())
        avg_wind_speed = round(group["wind_speed_10m"].mean(), 2)

        entry = {
            "date": formatted_date,
            "full_date": str(full_date),
            "weekday": weekday,
            "max": max_temp,
            "min": min_temp,
            "humidity": avg_humidity,
            "cloudiness": avg_cloudiness,
            "rain": total_rain,
            "rain_probability": max_rain_prob,
            "wind_speedy": f"{avg_wind_speed} km/h",
            "sunrise": "N/A",
            "sunset": "N/A",
            "moon_phase": "N/A",
            "description": "clear",
            "condition": "clear_day",
        }
        results.append(entry)
        logging.info(entry)

    return results

def fetch_weather_dataframe():
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()["hourly"]

    return transform_weather_data(data)


def transform_to_old_format(df):
    grouped = df.groupby("date").agg({
        "temperature_2m": ["max", "min"],
        "relative_humidity_2m": "mean",
        "precipitation_probability": ["mean", "max"],
        "rain": "sum",
        "wind_speed_10m": "mean"
    })

    grouped.columns = [
        "max", "min", "humidity", "cloudiness", "rain_probability", "rain", "wind"
    ]
    grouped = grouped.reset_index()

    result = []
    for _, row in grouped.iterrows():
        date_obj = row["date"]
        formatted = {
            "date": date_obj.strftime("%d-%m"),
            "full_date": date_obj.isoformat(),
            "weekday": date_obj.strftime("%a"),
            "max": round(row["max"], 1),
            "min": round(row["min"], 1),
            "humidity": int(round(row["humidity"])),
            "cloudiness": round(row["cloudiness"], 1),
            "rain": round(row["rain"], 2),
            "rain_probability": int(round(row["rain_probability"])),
            "wind_speedy": f"{round(row['wind'], 2)} km/h",
            "sunrise": "N/A",
            "sunset": "N/A",
            "moon_phase": "N/A",
            "description": "clear",
            "condition": "clear_day"
        }
        result.append(formatted)
    return result

def publish_to_kafka(producer, data_list):
    for item in data_list:
        producer.produce('ingestion_api', value=json.dumps(item).encode('utf-8'))
    producer.flush()

def main():
    logging.info("Starting weather API producer with open-meteo")
    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    logging.info("Connected to Kafka")

    logging.info("Fetching weather data from open-meteo")
    fetch_weather_dataframe()
    # df = fetch_weather_dataframe()
    # data_list = transform_to_old_format(df)
    # logging.info("Fetched and transformed data")

    # logging.info("Publishing to Kafka topic 'ingestion_api'")
    # publish_to_kafka(producer, data_list)
    # logging.info("Published successfully")

if __name__ == '__main__':
    
    main()
