CREATE STAGE airflow_stage;
USE DATABASE airflow;
USE SCHEMA PUBLIC;

CREATE STAGE AIRFLOW_STAGE;


CREATE TABLE weather_incremental (
  data VARIANT,
  load_date DATE
);

select current_user();
select current_account();
select current_warehouse();


SHOW STAGES;

LIST @AIRFLOW_STAGE;

CREATE OR REPLACE TABLE weather_incremental (
    data VARIANT
);

CREATE OR REPLACE TABLE WEATHER_INCREMENTAL_flatten (
    ts TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    load_date DATE
);


INSERT INTO WEATHER_INCREMENTAL_flatten (
    ts,
    temperature,
    humidity,
    latitude,
    longitude,
    load_date
)
SELECT
    TO_TIMESTAMP(hour.value::string) AS ts,
    temperature.value::float AS temperature,
    humidity.value::float AS humidity,
    data:latitude::float AS latitude,
    data:longitude::float AS longitude,
    CURRENT_DATE() AS load_date
FROM weather_incremental,
     LATERAL FLATTEN(input => data:hourly.time) AS hour,
     LATERAL FLATTEN(input => data:hourly.temperature_2m) AS temperature,
     LATERAL FLATTEN(input => data:hourly.relativehumidity_2m) AS humidity
WHERE hour.index = temperature.index
  AND hour.index = humidity.index
QUALIFY ROW_NUMBER() OVER (PARTITION BY hour.value ORDER BY load_date DESC) = 1;


CREATE OR REPLACE TABLE weather_incremental (
    data VARIANT);

    CREATE OR REPLACE TABLE weather_full (
    ts TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);



