from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from io import StringIO
import psycopg2

# Параметры подключения к PostgreSQL
POSTGRES_CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@localhost:5432/test"

# URL для загрузки CSV файла
CSV_FILE_URL = "https://yadi.sk/d/ZdaGdtyXRL3qZQ"

# Определение DAG для загрузки данных
load_data_dag = DAG(
    'load_weather_data',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 2, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Load weather data into PostgreSQL',
    schedule_interval='@hourly',  # Планирование каждый час
)

# Функция для загрузки данных в PostgreSQL
def load_data_to_postgres():
    # Загрузка данных из URL
    response = requests.get(CSV_FILE_URL)
    content = StringIO(response.text)

    # Подключение к базе данных PostgreSQL
    conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
    cursor = conn.cursor()

    # Создание временной таблицы и загрузка данных
    create_table_query = """
        CREATE TEMP TABLE temp_weather_data (
            city VARCHAR,
            date DATE,
            hour INT,
            temperature_c DECIMAL,
            pressure_mm DECIMAL,
            is_rainy BOOLEAN
        );
    """
    cursor.execute(create_table_query)

    copy_data_query = """
        COPY temp_weather_data FROM stdin WITH CSV HEADER DELIMITER ',' QUOTE '"';
    """
    cursor.copy_expert(sql=copy_data_query, file=content)

    # Заменить текущие данные новыми данными
    replace_data_query = """
        INSERT INTO raw_data.weather_data
        SELECT * FROM temp_weather_data
        ON CONFLICT (city, date, hour) DO UPDATE SET
            temperature_c = EXCLUDED.temperature_c,
            pressure_mm = EXCLUDED.pressure_mm,
            is_rainy = EXCLUDED.is_rainy;
    """
    cursor.execute(replace_data_query)

    # Закрытие соединения
    conn.commit()
    cursor.close()
    conn.close()

# Определение задачи загрузки данных
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=load_data_to_postgres,
    dag=load_data_dag,
)