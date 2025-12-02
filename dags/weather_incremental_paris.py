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
    "weather_incremental_paris",
    start_date=datetime(2025, 11, 29),
    schedule="0 * * * *",  # every hour
    catchup=False,
    default_args=default_args,
    tags=["weather", "incremental", "paris"]
) as dag:

    @task(execution_timeout=timedelta(seconds=30))
    def fetch_incremental(ds=None):
        params = {
            "latitude": LAT,
            "longitude": LON,
            "hourly": "temperature_2m,relativehumidity_2m",
            "forecast_days": 1
        }
        r = requests.get(API_URL, params=params, timeout=15)
        r.raise_for_status()
        return r.json(), ds

    @task
    def save_incremental(payload):
        data, ds = payload   # unpack JSON + Airflow date

        out_dir = "/opt/airflow/logs/data/weather"
        filename = f"{out_dir}/weather_inc_{ds}.json"

        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # ALWAYS overwrite the file
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        print(f"Saved INCREMENTAL file: {filename}")
        return filename

    save_incremental(fetch_incremental())
