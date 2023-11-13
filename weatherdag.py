from airflow import DAG
from datetime import timedelta, datetime
import json
import pandas as pd
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator



now = datetime.now()
dt_string = now.strftime("%d%m%Y%H%M%S")
city_name="Atlanta"
dt_string = f'current_weather_data_{city_name}_' + dt_string + ".csv"
filename=dt_string


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


def transform_load_data(task_instance, filename):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": "ASIAR4DZVAQOF3VKJGEI", "secret": "Bl+w2C1OuV/KjcC8fdkqO5xSgpgQq8xkRKCLfVXU", "token": "FwoGZXIvYXdzEO3//////////wEaDFpRBu2kujMy/jCxnyKCAYqLrnSUTbKI0KTLuFeF7OsDmkF442v0DBmpdnzNVGZlYYatwhXDYzfWDTqvdoAxNLQN6sdvGbIyO2QRRRJe9Kp2jfo3V0q3PzZJ8fbrZ4L3eILAFq+/9/wwcBglg8GQOxWHdGBeBfHcNwnQr73LalEeWGiALArphCFwl9MjjbZPLv8os4HBqgYyKKa7XArNiW+lRtfmp4WNvfpWPaDmSvMJHuZBPUeBmM70Hz+CH1a3YqM="}

    # now = datetime.now()
    # dt_string = now.strftime("%d%m%Y%H%M%S")
    # city_name="Atlanta"
    # dt_string = f'current_weather_data_{city_name}_' #+ dt_string
    df_data.to_csv(f"s3://weatherbucketdataeng/{filename}", index=False)
    #df_data.to_csv(f"{dt_string}.csv", index=False)



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}



with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = None,
        catchup=False) as dag:


        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=f'/data/2.5/weather?q=Atlanta&appid=693d33aa88cea1f63d263db39278e08f'
        )


        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint=f'/data/2.5/weather?q=Atlanta&appid=693d33aa88cea1f63d263db39278e08f',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        )

        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data,
        op_kwargs={'filename': filename}
        )

        uploadS3_to_postgres = PostgresOperator(
            task_id="tsk_uploadS3_to_postgres",
            postgres_conn_id="postgres_conn",
            sql="SELECT aws_s3.table_import_from_s3('weather_data', '', '(format csv, DELIMITER '', HEADER true)', 'weatherbucketdataeng', %(filename)s, 'us-west-2');",
            parameters={"filename": filename},
        )



        create_table = PostgresOperator(
            task_id='tsk_create_table',
            postgres_conn_id = "postgres_conn",
            sql= ''' 
                CREATE TABLE IF NOT EXISTS weather_data (
                city TEXT,
                description TEXT,
                temperature_farenheit NUMERIC,
                feels_like_farenheit NUMERIC,
                minimun_temp_farenheit NUMERIC,
                maximum_temp_farenheit NUMERIC,
                pressure NUMERIC,
                humidity NUMERIC,
                wind_speed NUMERIC,
                time_of_record TIMESTAMP,
                sunrise_local_time TIMESTAMP,
                sunset_local_time TIMESTAMP                    
            );
            '''
            )



        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data>>create_table>>uploadS3_to_postgres