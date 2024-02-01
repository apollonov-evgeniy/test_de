from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# Параметры подключения к PostgreSQL
POSTGRES_CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@localhost:5432/test"

# Определение DAG для создания бекапа
backup_dag = DAG(
    'create_backup',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 2, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'schedule_interval': '@daily',  # Планирование каждый день
    },
    description='Create a daily backup of the PostgreSQL database',
)

# Определение задачи создания бекапа
create_backup_task = PostgresOperator(
    task_id='create_backup',
    postgres_conn_id='postgres_default',
    sql=f"pg_dump {POSTGRES_CONNECTION_STRING} > /path/to/backup/directory/backup_{{ ds }}.sql",
    dag=backup_dag,
)