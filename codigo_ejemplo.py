from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import csv

# Definimos la URL del API del clima (reemplaza por alguna API pública si lo deseas)
WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast?latitude=35.6895&longitude=139.6917&hourly=temperature_2m"

# Función para extraer datos del clima
def extract_weather_data():
    response = requests.get(WEATHER_API_URL)
    data = response.json()
    return data['hourly']['temperature_2m']

# Función para transformar los datos a un formato CSV
def transform_and_save_to_csv(ti):
    temperature_data = ti.xcom_pull(task_ids='extract_weather_data')
    filename = '/home/diegoOSMD-LAP/airflow/weather_data.csv'

    with open(filename, mode='w') as file:
        writer = csv.writer(file)
        writer.writerow(["Hour", "Temperature"])
        for i, temp in enumerate(temperature_data):
            writer.writerow([i, temp])

    print(f"Archivo CSV guardado en: {filename}")

# Configuramos el DAG con sus parámetros
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_etl_dag',
    default_args=default_args,
    description='Un DAG de ejemplo para ETL de datos del clima',
    schedule_interval='@daily',
    start_date=datetime(2023, 10, 12),
    catchup=False,
) as dag:

    extract_weather = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data
    )

    transform_save_csv = PythonOperator(
        task_id='transform_and_save_to_csv',
        python_callable=transform_and_save_to_csv
    )

    # Definimos la secuencia de tareas
    extract_weather >> transform_save_csv