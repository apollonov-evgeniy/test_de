1. Выгрузка данных из API Яндекс.Погоды и преобразование их в csv
Используя API Яндекс.Погоды, необходимо выгрузить прогнозные данные за 7 дней для Москвы, Казани, Санкт-Петербурга, Тулы и Новосибирска. В случае, если API отдает пустые значения за день, то их необходимо удалить.
Информация должна быть представлена по часам с расширенным набором полей по осадкам.
Полученный json необходимо преобразовать в csv, формат:
city,date,hour,temperature_c,pressure_mm,is_rainy


𝑀𝑜𝑠𝑐𝑜𝑤,19.08.2023,12,27,750,0


𝑀𝑜𝑠𝑐𝑜𝑤,19.08.2023,13,27,750,0


...
𝐾𝑎𝑧𝑎𝑛,19.08.2023,12,20,770,1


𝐾𝑎𝑧𝑎𝑛,19.08.2023,13,21,770,0


Описание полей:
city - Город
date - Дата события
hour - Часы
temperature_c - Температура в Цельсиях
pressure_mm - Давление в мм ртутного столба
is_rainy - Флаг наличия дождя в конкретный день и час (см. документацию по API - описание полей).
Полученный csv необходимо выгрузить на облачный диск и в конце решения предоставить ссылку.
Ссылка на получение ключа: https://yandex.ru/dev/weather/doc/dg/concepts/about.html#about__onboarding
Дополнительно ответьте на вопросы: какие существуют возможные пути ускорения получения данных по API и их преобразования? Возможно ли эти способы использовать в Airflow?


3. Загрузка данных в БД (Airflow + PostgreSQL).
Используя полученный csv файл, необходимо каждый час загружать данные в PostgreSQL. Предварительно в БД необходимо создать схемы: для приемки сырых данных и для будущих агрегирующих таблиц.

При создании таблиц приветствуется использование партицирования и индексирования (по возможности и необходимости).

В решении необходимо показать код загрузки данных, скрипты создания схем и таблиц для пункта 2 и 2.1.

Подсказка: для решения задачи нужно развернуть БД и Airflow, мы рекомендуем это сделать локально с помощью докера.


2.1 Формирование витрин (PostgreSQL).
Используя таблицу с сырыми данными, необходимо собрать витрину, где для каждого города и дня будут указаны часы начала дождя. Условимся, что дождь может начаться только 1 раз за день в любом из городов. Витрина должна обновляться ежечасно.

Необходимо создать витрину, где для каждого города, дня и часа будет рассчитано скользящее среднее по температуре и по давлению.


2.2 Создание бекапа
Каждый день создавать бекап всей базы данных.
Результаты данного задания организовать в виде git репозитория, содержащего описание проекта, как запустить и всего выше описанного кода.


# Weather Data Project

This project includes Apache Airflow DAGs for loading weather data, calculating metrics, and creating daily backups of a PostgreSQL database.

### Shared link for /weather_data.csv: https://yadi.sk/d/ZdaGdtyXRL3qZQ

## Project Structure
 ```bash
weather_data_project/
├── dags/
│ ├── load_weather_data_dag.py
│ ├── calculate_metrics_dag.py
│ └── backup_dag.py
├── scripts
│ ├─ fetch_and_transform_weather_data.ipynb
│ └── create_tables.py
├── README.md
└── requirements.txt
 ```
## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/your-username/weather_data_project.git
   cd weather_data_project

1.    Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2.   Set up your PostgreSQL database and update the connection string in DAGs accordingly.

# Usage

## Run Jupyter Notebook:
```bash
jupyter notebook fetch_and_transform_weather_data.ipynb
```
Open the notebook and execute each cell to fetch and transform weather data.
## Load Weather Data
1. Run the following DAG in Apache Airflow:

 ```bash
airflow trigger_dag load_weather_data
 ```

## Calculate Metrics
1. Run the following DAG in Apache Airflow:

 ```bash
airflow trigger_dag calculate_metrics
 ```

## Create Daily Backup
1. Run the following DAG in Apache Airflow:

 ```bash
airflow trigger_dag create_backup
 ```
## Notes
Make sure to set up appropriate PostgreSQL connection strings in DAGs.
Adjust schedule intervals and other configurations in DAGs according to your requirements. 
