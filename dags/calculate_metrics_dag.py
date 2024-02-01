from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# Параметры подключения к PostgreSQL
POSTGRES_CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@localhost:5432/test"

# Определение DAG для расчета метрик
calculate_metrics_dag = DAG(
    'weather_data_processing',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 2, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Process weather data and create views in PostgreSQL',
    schedule_interval='@hourly',  # Планирование каждый час
)

# Функция для расчета метрик
def calculate_metrics():
    # Подключение к базе данных PostgreSQL
    conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
    cursor = conn.cursor()

    # Расчет часов начала дождя
    rain_start_query = """
        CREATE TEMP TABLE temp_rain_start_times AS
        SELECT
            city,
            date,
            MIN(hour) AS rain_start_hour
        FROM raw_data.weather_data
        WHERE is_rainy
        GROUP BY city, date;

        DELETE FROM aggregated_data.rain_start_times;
        INSERT INTO aggregated_data.rain_start_times (city, date, rain_start_hour)
        SELECT city, date, rain_start_hour FROM temp_rain_start_times;
    """
    cursor.execute(rain_start_query)

    # Расчет скользящего среднего
    sliding_avg_query = """
        CREATE TEMP TABLE temp_avg AS
        SELECT
            city,
            date,
            hour,
            AVG(temperature_c) OVER (PARTITION BY city, date ORDER BY hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS temperature_avg,
            AVG(pressure_mm) OVER (PARTITION BY city, date ORDER BY hour ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS pressure_avg
        FROM raw_data.weather_data;

        DELETE FROM aggregated_data.temperature_pressure_avg;
        INSERT INTO aggregated_data.temperature_pressure_avg (city, date, hour, temperature_avg, pressure_avg)
        SELECT city, date, hour, temperature_avg, pressure_avg FROM temp_avg;
    """
    cursor.execute(sliding_avg_query)

    # Закрытие соединения
    conn.commit()
    cursor.close()
    conn.close()

# Определение задачи для расчета метрик
calculate_metrics_task = PythonOperator(
    task_id='calculate_metrics',
    python_callable=calculate_metrics,
    dag=calculate_metrics_dag,
)