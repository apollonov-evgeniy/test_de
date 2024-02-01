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

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/weather_data_project.git
   cd weather_data_project

1.    Install dependencies:
pip install -r requirements.txt
2.   Set up your PostgreSQL database and update the connection string in DAGs accordingly.

# Usage
## Load Weather Data
1. Run the following DAG in Apache Airflow:
airflow trigger_dag load_weather_data

## Calculate Metrics
1. Run the following DAG in Apache Airflow:
airflow trigger_dag calculate_metrics

## Create Daily Backup
1. Run the following DAG in Apache Airflow:
airflow trigger_dag create_backup
## Notes
Make sure to set up appropriate PostgreSQL connection strings in DAGs.
Adjust schedule intervals and other configurations in DAGs according to your requirements. 
