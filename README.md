# Weather Data Project

This project includes Apache Airflow DAGs for loading weather data, calculating metrics, and creating daily backups of a PostgreSQL database.

## Project Structure

weather_data_project/
├── dags/
│ ├── load_weather_data_dag.py
│ ├── calculate_metrics_dag.py
│ └── backup_dag.py
├── scripts/
│ └── create_tables.py
├── README.md
└── requirements.txt
