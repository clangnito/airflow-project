from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import os
import json
import requests

API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 48.8566, 2.3522

SNOWFLAKE_CONN_ID = "snowflake_conn"   # Défini dans Airflow

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    "weather_to_snowflake",
    start_date=datetime(2025, 11, 29),
    schedule="0 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["weather", "snowflake"]
) as dag:

    @task
    def fetch_weather(ds=None):
        params = {
            "latitude": LAT,
            "longitude": LON,
            "hourly": "temperature_2m,relativehumidity_2m",
            "forecast_days": 1
        }
        r = requests.get(API_URL, params=params, timeout=20)
        r.raise_for_status()
        return {"data": r.json(), "ds": ds}

    @task
    def save_file(payload):
        data = payload["data"]
        ds = payload["ds"]

        out_dir = "/opt/airflow/logs/data/weather"
        os.makedirs(out_dir, exist_ok=True)

        filename = f"{out_dir}/weather_inc_{ds}.json"
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)

        return filename

    @task
    def upload_to_stage(file_path):
        hook = SnowflakeHook(SNOWFLAKE_CONN_ID)

        # PUT = upload vers Snowflake stage
        sql = f"""
        PUT file://{file_path}
        @airflow_stage
        AUTO_COMPRESS=FALSE
        OVERWRITE=TRUE;
        """

        hook.run(sql)
        return os.path.basename(file_path)

    @task
    def copy_into_snowflake(file_name):
        hook = SnowflakeHook(SNOWFLAKE_CONN_ID)

        sql = f"""
        COPY INTO weather_incremental
        FROM @airflow_stage/{file_name}
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'CONTINUE';
        """

        hook.run(sql)

    # Workflow Airflow
    payload = fetch_weather()
    file_path = save_file(payload)
    staged_file = upload_to_stage(file_path)
    copy_into_snowflake(staged_file)
