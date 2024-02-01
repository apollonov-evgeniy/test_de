from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATE, INT, DECIMAL, BOOLEAN, PrimaryKeyConstraint

# Параметры подключения к PostgreSQL
POSTGRES_CONNECTION_STRING = "postgresql+psycopg2://postgres:postgres@localhost:5432/test"

# Создание метаданных для связи с базой данных
engine = create_engine(POSTGRES_CONNECTION_STRING)
metadata = MetaData(bind=engine)

# Создание таблицы для сырых данных
raw_data_table = Table(
    'weather_data',
    metadata,
    Column('city', VARCHAR),
    Column('date', DATE),
    Column('hour', INT),
    Column('temperature_c', DECIMAL),
    Column('pressure_mm', DECIMAL),
    Column('is_rainy', BOOLEAN),
    schema='raw_data'
)

# Создание таблицы для витрины с часами начала дождя
rain_start_times_table = Table(
    'rain_start_times',
    metadata,
    Column('city', VARCHAR),
    Column('date', DATE),
    Column('rain_start_hour', INT),
    schema='aggregated_data',
    PrimaryKeyConstraint('city', 'date')
)

# Создание таблицы для витрины со скользящим средним
temperature_pressure_avg_table = Table(
    'temperature_pressure_avg',
    metadata,
    Column('city', VARCHAR),
    Column('date', DATE),
    Column('hour', INT),
    Column('temperature_avg', DECIMAL),
    Column('pressure_avg', DECIMAL),
    schema='aggregated_data',
    PrimaryKeyConstraint('city', 'date', 'hour')
)

# Создание схем и таблиц в базе данных
metadata.create_all()