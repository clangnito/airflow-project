from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import os
import json
import requests

API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 48.8566, 2.3522

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    "weather_full_paris",
    start_date=datetime(2025, 11, 29),
    schedule="0 6 * * *",  # every day at 06:00
    catchup=False,
    default_args=default_args,
    tags=["weather", "full", "paris"]
) as dag:

    @task(execution_timeout=timedelta(seconds=30))
    def fetch_full():
        params = {
            "latitude": LAT,
            "longitude": LON,
            "hourly": "temperature_2m,relativehumidity_2m",
            "past_days": 7
        }
        r = requests.get(API_URL, params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    @task
    def save_full(data):
        out_dir = "/opt/airflow/logs/data/weather"
        os.makedirs(out_dir, exist_ok=True)
        filename = f"{out_dir}/weather_full.json"

        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Saved FULL file: {filename}")

    save_full(fetch_full())
