#import xmltodict
import psycopg2

from sqlalchemy import create_engine

import xmlschema  # type: ignore
from zeep import Client  # type: ignore

from datetime import datetime
from minio import Minio

from zeep import Client
from zeep.transports import Transport
from requests import Session

import psycopg2

import pandas as pd

from dotenv import dotenv_values
import json
import io
import os


import subprocess


def dump_public_schema():
    # Загружаем данные из .env
    config = dotenv_values("close.env")
    dbhost = config['PG_HOST']
    dbpass = config['PG_PASSWORD']
    dbuser = config['PG_USER']
    dbname = 'saprr_dev'  # ваше название базы

    # Устанавливаем пароль в переменную окружения,
    # чтобы pg_dump мог аутентифицироваться
    os.environ['PGPASSWORD'] = dbpass

    # Формируем команду pg_dump
    # --schema-only (-s): только структура (DDL), без данных
    # -n public: дамп только схемы 'public'
    cmd = [
        'pg_dump',
        '-U', dbuser,
        '-h', dbhost,
        '-p', '5432',
        dbname,
        '--schema-only',
        '-n', 'public',
    ]

    # Запускаем pg_dump и пишем вывод в файл (например, schema_public.sql)
    output_file = 'schema_public.sql'
    with open(output_file, 'w', encoding='utf-8') as f:
        subprocess.run(cmd, stdout=f, check=True)

    # (Необязательно) удаляем пароль из переменной окружения
    del os.environ['PGPASSWORD']

    print(f"Схема public сохранена в {output_file}")



# Загрузка конфигурации из файла окружения
config = dotenv_values("close.env")
dbhost = config['PG_HOST']
dbpass = config['PG_PASSWORD']
dbuser = config['PG_USER']
base_name = 'saprr_dev'

# Подключение к базе данных с помощью psycopg2
conn = psycopg2.connect(
    host=dbhost,
    database=base_name,
    user=dbuser,
    password=dbpass
)

# Создание SQLAlchemy engine для удобной работы с базой данных
engine_sp = create_engine(f'postgresql+psycopg2://{dbuser}:{dbpass}@{dbhost}:5432/{base_name}')


def extract_prefixes_from_filenames(directory):
    prefixes = set()

    if not os.path.exists(directory):
        print(f"Папка {directory} не существует.")
        return prefixes

    for filename in os.listdir(directory):
        match = re.match(r'^(\d+)_', filename)
        if match:
            prefixes.add(match.group(1))

    return prefixes


name_cat = 'rep1002_new'
TIMEOUT = 4  # Например, 10 секунд
config = dotenv_values(".env")
session=Session()
#wsdl = config['SPARK_URL']
wsdl='http://webservicefarm.interfax.ru/IfaxWebService/ifaxwebservice.asmx'
#wsdl = 'http://localhost/IfaxWebService/ifaxwebservice.asmx'
session.timeout = TIMEOUT  # Установка таймаута для сессии
client_spark=Client(wsdl,transport=Transport(session=session))
request_data={'Login' : config['SPARK_LOGIN'] ,
              'Password' : config['SPARK_PASSWORD']}
response=client_spark.service.Authmethod(**request_data)


#--------------------------------------------------------------------------------------

# 1. Отфильтруем строки, где id_park и company_id не пустые
filtered_data = first_sheet.dropna(subset=['id_park', 'company_id'])

# 2. Оставляем только уникальные комбинации association_id и company_id
insert_data = filtered_data[['id_park', 'company_id']].drop_duplicates()

# 3. Добавляем столбец association_type со значением 'park_ind'
insert_data['association_type'] = 'cluster'
insert_data.rename(columns={'id_park': 'association_id'}, inplace=True)

# 4. Замена значений NA на None для вставки в PostgreSQL
insert_data = insert_data.astype(object).where(pd.notna(insert_data), None)

# Чтение данных из файла Excel с указанием типа данных строкового столбца
df = pd.read_excel('not_inn_in_base.xlsx',dtype={'inn': str})
#, dtype={'INN': str, 'i2110': str, 'i2400': str, 'pers': str})
inn_set = set(df['inn'])
df = pd.read_excel('not_orgn_in_base.xlsx',dtype={'ogrn': str})
#, dtype={'INN': str, 'i2110': str, 'i2400': str, 'pers': str})
ogrn_set = set(df['ogrn'])
len(inn_set)


df1 = select('select * from cl_companies_types')
df1

cur = conn.cursor()

# Предположим, df содержит уникальные значения ogrn, которые нужно обновить
# Здесь df - ваш DataFrame, и мы итерируем по его уникальным значениям ogrn
for ogrn in df['ogrn'].unique():
    # SQL-запрос для обновления столбца is_rep и добавления текущего времени в столбец added
    query = """
        UPDATE public.companies
        SET is_rep = TRUE, added = now()
        WHERE id IN (
            SELECT company
            FROM public.main_data
            WHERE registration_number = %s
        )
    """
    # Выполнение запроса
    cur.execute(query, (ogrn,))

# Фиксация транзакций
conn.commit()

# Закрытие курсора и соединения
cur.close()
conn.close()




# Итерация по строкам DataFrame
for index, row in df.iterrows():
    ogrn = row['ogrn']  # Получение значения ogrn из df
    company_type = row['id']  # Получение company_type из df
    
    # Получение ID компании, соответствующего ogrn
    cur.execute("""
        SELECT company FROM public.main_data WHERE registration_number = %s;
    """, (ogrn,))
    company_id_result = cur.fetchone()
    
    if company_id_result:
        company_id = company_id_result[0]
        # Вставка новой записи в companies_companies_types
        cur.execute("""
            INSERT INTO public.companies_companies_types (company, company_type, user_id, added, deleted)
            VALUES (%s, %s, NULL, now(), NULL);
        """, (company_id, company_type))
    else:
        print(f"No company found for OGRN: {ogrn}")

# Фиксация транзакций
conn.commit()

# Закрытие курсора и соединения
cur.close()
conn.close()

#---------------------------------------------------------------------------------------------

# Путь к вашей директории с JSON файлами
directory = 'C:/VNIIR/add_new_mpr2/ereps'

# Загрузка датафрейма
df = pd.read_excel('inn_list_n.xlsx', dtype={'OGRN': str, 'INN': str})

# Добавление новых столбцов для SparkID и FoundIn
df['SparkID'] = pd.NA
df['FoundIn'] = None  # Инициализируем новый столбец как None
def find_sparkid_and_label(ogrn=None, inn=None):
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            with open(os.path.join(directory, filename), 'r', encoding='utf-8') as file:
                data = json.load(file)
                
                # Сначала проверяем основную ветку данных на наличие INN или OGRN
                report = data['Data']['Report'][0]
                report_inn = report.get('INN', '').zfill(12)
                report_ogrn = report.get('OGRN', '')
                inn = inn.zfill(12) if inn else None
                
                if (ogrn and report_ogrn == ogrn) or (inn and report_inn == inn):
                    return report.get('SparkID'), 'new'
                
                # Если в основной ветке не нашлось, ищем в ChangesInNameAndLegalForm
                changes = report.get('ChangesInNameAndLegalForm', {}).get('Change', [])
                for change in changes:
                    change_inn = change.get('INN', '').zfill(12)
                    change_ogrn = change.get('OGRN', '')
                    if (ogrn and change_ogrn == ogrn) or (inn and change_inn == inn):
                        return report.get('SparkID'), 'old'
    return None, None

# Применение функции к каждой строке датафрейма
for index, row in df.iterrows():
    sparkid, label = find_sparkid_and_label(ogrn=row.get('OGRN'), inn=row.get('INN'))
    if sparkid:
        df.at[index, 'SparkID'] = sparkid
        df.at[index, 'FoundIn'] = label




#----------------------------------------------------------------------------------------------------


# Предполагаем, что client_spark, inn_set и name_cat уже инициализированы
# ПО SPARKID
sparkid_set = set()
#df1spark_id_set=df1spark_id_set-sparkid_set # временно 
method_schemas = {
    'GetCompanyExtendedReport': 'schemas/ExtendedReport.xsd',  # ereps
    'GetCompanyStructure': 'schemas/CompanyStructure.xsd',  # structure
    'GetCompanyLicenses': 'schemas/CompanyLicenses.xsd',  # licenses
    'GetCompanyExecutionProceedings': 'schemas/CompanyExecutionProceedings.xsd',  # execution
    'GetCompanyArbitrationSummary': 'schemas/CompanyArbitrationSummary.xsd'  # arbitration
}
method_schemas_1 = {
    'GetSupplierStateContracts': 'schemas/SupplierStateContracts.xsd',
    'GetOwnershipHierarchy': 'schemas/OwnershipHierarchy.xsd'
}
schema_date_list = xmlschema.XMLSchema('schemas/CompanyAccountingReportDateList.xsd')
schema_report = xmlschema.XMLSchema('schemas/CompanyAccountingReport.xsd')

for inn in inn_set:
    # Обработка GetCompanyExtendedReport отдельно
    extended_report_schema = xmlschema.XMLSchema(method_schemas['GetCompanyExtendedReport'])
    response = client_spark.service.GetCompanyExtendedReport(sparkId=inn)
    extended_data = extended_report_schema.to_dict(response['xmlData'])
    
    # Проверка наличия SparkID в ответе
    try:
        sparkid = extended_data['Data']['Report'][0]['SparkID']
    except (KeyError, IndexError):
        print(f"Компания с ИНН {inn} не найдена.")
        continue  # Пропускаем этот ИНН, если SparkID не найден

    if sparkid not in sparkid_set:
        sparkid_set.add(sparkid)
        # Путь для сохранения отчетов GetCompanyExtendedReport
        ereps_path = f"{name_cat}/ereps"
        if not os.path.exists(ereps_path):
            os.makedirs(ereps_path)
        with open(f"{ereps_path}/{sparkid}_GetCompanyExtendedReport.json", 'w', encoding='utf-8') as file:
            json.dump(extended_data, file, ensure_ascii=False, indent=4)

        # Выполнение и сохранение результатов остальных методов
        for method, xsd_file in method_schemas.items():
            if method == 'GetCompanyExtendedReport':
                continue  # Этот отчет уже обработан

            schema = xmlschema.XMLSchema(xsd_file)
            response = client_spark.service.__getattr__(method)(sparkId=inn)
            data = schema.to_dict(response['xmlData'])

            # Определение пути на основе метода
            if method == 'GetCompanyStructure':
                path = f"{name_cat}/structure"
            elif method == 'GetCompanyLicenses':
                path = f"{name_cat}/licenses"
            elif method == 'GetCompanyExecutionProceedings':
                path = f"{name_cat}/execution"
            elif method == 'GetCompanyArbitrationSummary':
                path = f"{name_cat}/arbitration"

            if not os.path.exists(path):
                os.makedirs(path)

            with open(f"{path}/{sparkid}_{method}.json", 'w', encoding='utf-8') as file:
                json.dump(data, file, ensure_ascii=False, indent=4)
        

        for method, xsd_file in method_schemas_1.items():
            schema = xmlschema.XMLSchema(xsd_file)
                    
            if method == 'GetSupplierStateContracts':
                    path = f"{name_cat}/contracts"
            elif method == 'GetOwnershipHierarchy':
                    path = f"{name_cat}/ownerhierar"
                    
            if not os.path.exists(f"{path}/{inn}_{method}.json"):
                    if method == 'GetSupplierStateContracts':
                        #print(ogrn)
                        response = client_spark.service.__getattr__(method)(sparkId=inn)
                    elif method == 'GetOwnershipHierarchy':
                        response = client_spark.service.__getattr__(method)(sparkId=inn)
                        
            data = schema.to_dict(response['xmlData'])
                    
            if not os.path.exists(path):
                        os.makedirs(path)
                        
            with open(f"{path}/{sparkid}_{method}.json", 'w', encoding='utf-8') as file:
                        json.dump(data, file, ensure_ascii=False, indent=4)
                

        path_ars = f"{name_cat}/ars"
        if not os.path.exists(path_ars):
               os.makedirs(path_ars)
               
        path = f"{name_cat}/finances"
        if not os.path.exists(path):
               os.makedirs(path)
    
        method = 'GetCompanyAccountingReportDateList'
        response_date_list = client_spark.service.__getattr__(method)(sparkId=inn) #(sparkId=ogrn)
    
        data_date_list = schema_date_list.to_dict(response_date_list['xmlData'])
        with open(f"{path_ars}/{sparkid}_{method}.json", 'w', encoding='utf-8') as file:
                        json.dump(data_date_list, file, ensure_ascii=False, indent=4)
        periods = data_date_list.get('Data', {}).get('Periods', {}).get('Period')
        
        if periods is not None:
            dates = []    # список дат в нужном формате
            closest_year = None  # ближайший год
            
            for period in periods:
                try:
                    # Разбиваем поле на компоненты по пробелу
                    name_parts = period['Name'].split()
                    
                    for part in name_parts:
                        # Если часть - это значение года
                        if part.isdigit() and 2018 <= int(part) <= 2024:
                            # преобразуем дату в нужный формат и добавляем в список
                            old_date_format = datetime.strptime(period['Date'], '%Y-%m-%d')
                            #new_date_format = old_date_format.strftime('%d.%m.%Y')
                            dates.append(old_date_format)
                            break
                        # Если дата состоит только из года и он больше текущего closest_year
                        elif part.isdigit() and len(part) == 4  and (closest_year is None or int(part) > closest_year):
                            if period['Name'] == part:  # проверяем, содержит ли Name только год
                                closest_year = int(part)
                except KeyError:
            # Если ключ 'Name' отсутствует, просто продолжаем с следующим периодом
                    continue
            if not dates and closest_year is not None:
                periods_with_closest_year = [p for p in periods if str(closest_year) == p['Name']]
                if periods_with_closest_year:
                    old_date_format = datetime.strptime(periods_with_closest_year[0]['Date'], '%Y-%m-%d')
                    #new_date_format = old_date_format.strftime('%d.%m.%Y')
                    dates.append(old_date_format)
            
            # Теперь список дат (`dates`) содержит даты в нужном формате для каждого периода с 2018 по 2023 год, 
            # или дату, соответствующую ближайшему году, если таковой имеется.
            # Теперь у вас есть список дат (`dates`) в нужном формате для каждого периода с 2018 по 2023 год, и вы можете использовать его для вызова веб-сервиса
            if dates:
                method = 'GetCompanyAccountingReport'
                for date in dates:
                    response_report = client_spark.service.__getattr__(method)(sparkId=inn, balanceDate=date)
                    try:
                        data_report = schema_report.to_dict(response_report['xmlData'])
                        with open(f"{path}/{sparkid}_{data_report['Data']['Report'][0]['Period']['@PeriodName']}_{date.strftime('%Y-%m-%d')}_{method}.json", 'w', encoding='utf-8') as file:
                            json.dump(data_report, file, ensure_ascii=False, indent=4)
                    except Exception as e:
                        print(f"Error processing data {inn} for date {date} and method {method}: {e}")
                        continue

len(sparkid_set)

# Загрузка датафрейма
df = pd.read_excel('Предприятия РЭП_04.10.xlsx', dtype={'ОГРН': str, 'ИНН': str})
inn_set = set(df['ИНН'])
len(inn_set)

#------------------------------------------------------------------------------------------------------

# Загрузка данных из Excel
df = pd.read_excel('Предприятия РЭП_04.10.xlsx', dtype={'ОГРН': str, 'ИНН': str})


with engine_sp.connect() as connection:
    # Обновление is_rep на True для соответствующих записей
     for ogrn in df['ОГРН'].dropna().unique():
        # Проверка наличия записей, которые могут быть обновлены
        update_query = text("""
            UPDATE public.companies
            SET is_rep = TRUE
            FROM public.main_data
            WHERE public.main_data.registration_number = :ogrn
            AND public.main_data.company = public.companies.id
            """)
        connection.execute(update_query, {'ogrn': ogrn})
        connection.commit()
            #break




# Создание подключения
with engine_sp.connect() as connection:
    # Обновление is_rep на True для соответствующих записей
    for ogrn in df['ОГРН'].dropna().unique():
        # Выполнение обновления через SQL-запрос
        update_query = text("""
        UPDATE public.companies
        SET is_rep = TRUE
        FROM public.main_data
        WHERE public.main_data.registration_number = :ogrn
        AND public.main_data.company = public.companies.id
        """)
        connection.execute(update_query, {'ogrn': ogrn})



# Укажите путь к директории с JSON-файлами
directory_path = r"C:\VNIIR\is_rep_clast_park\ereps"

# Список для хранения значений SparkID
spark_ids = []

# Проход по всем файлам в директории
for filename in os.listdir(directory_path):
    if filename.endswith(".json"):  # Проверка, что файл имеет расширение .json
        file_path = os.path.join(directory_path, filename)
        try:
            # Открытие и чтение JSON-файла
            with open(file_path, "r", encoding="utf-8") as file:
                data = json.load(file)
                
                # Извлечение значения SparkID
                spark_id = data.get("Data", {}).get("Report", [{}])[0].get("SparkID")
                if spark_id:  # Проверка, что SparkID существует
                    spark_ids.append(spark_id)
        except Exception as e:
            print(f"Ошибка обработки файла {filename}: {e}")

# Результат: массив SparkID
print("SparkID values:", spark_ids)


im
# Пример массива SparkID


# Формируем строку для подстановки в SQL-запрос
spark_ids_str = ', '.join(f"'{spark_id}'" for spark_id in spark_ids)

# SQL-запрос для обновления записей
update_query = f"""
UPDATE public.companies
SET is_rep = TRUE, added = NOW()
WHERE spark_id IN ({spark_ids_str});
"""

# Выполнение SQL-запроса
try:
    with conn.cursor() as cursor:
        cursor.execute(update_query)
        conn.commit()
        print("Обновление завершено успешно.")
except Exception as e:
    conn.rollback()
    print(f"Ошибка при обновлении записей: {e}")
finally:
    conn.close()


import psycopg2
from dotenv import dotenv_values

# Загрузка конфигурации из файла окружения
config = dotenv_values("close.env")
dbhost = config['PG_HOST']
dbpass = config['PG_PASSWORD']
dbuser = config['PG_USER']
base_name = 'saprr_dev'

# Подключение к базе данных
conn = psycopg2.connect(
    host=dbhost,
    database=base_name,
    user=dbuser,
    password=dbpass
)

# Словари для хранения комментариев
view_comments = {}
column_comments = {}

try:
    with conn.cursor() as cursor:
        # Шаг 1: Сохранение комментариев представлений
        cursor.execute("""
            SELECT schemaname, matviewname, description
            FROM pg_matviews mv
            LEFT JOIN pg_description d
            ON mv.matviewname::regclass = d.objoid
            WHERE schemaname = 'public';
        """)
        rows = cursor.fetchall()
        for row in rows:
            schemaname, matviewname, description = row
            view_comments[matviewname] = description

        print("Комментарии представлений сохранены.")

        # Шаг 2: Сохранение комментариев колонок
        cursor.execute("""
            SELECT c.relname AS matviewname, a.attname AS column_name, d.description
            FROM pg_class c
            JOIN pg_attribute a ON c.oid = a.attrelid
            LEFT JOIN pg_description d ON c.oid = d.objoid AND a.attnum = d.objsubid
            WHERE c.relkind = 'm' AND c.relnamespace = 'public'::regnamespace AND a.attnum > 0;
        """)
        rows = cursor.fetchall()
        for row in rows:
            matviewname, column_name, description = row
            if matviewname not in column_comments:
                column_comments[matviewname] = {}
            column_comments[matviewname][column_name] = description

        print("Комментарии колонок сохранены.")

        cursor.execute("""
            SELECT matviewname, relowner::regrole::text AS owner
            FROM pg_matviews
            JOIN pg_class ON pg_matviews.matviewname = pg_class.relname
            WHERE schemaname = 'public';
        """)
        ownership_info = cursor.fetchall()

        # Для каждого представления выполнить проверку и обновление
        for matviewname, owner in ownership_info:
            if owner != dbuser:
                print(f"Изменение владельца для {matviewname} с {owner} на {dbuser}")
                cursor.execute(f"ALTER MATERIALIZED VIEW public.{matviewname} OWNER TO {dbuser};")
            # Выполнить обновление
            cursor.execute(f"REFRESH MATERIALIZED VIEW public.{matviewname};")
            # Вернуть владельца обратно
            cursor.execute(f"ALTER MATERIALIZED VIEW public.{matviewname} OWNER TO saprr_dev_power_users;")
        conn.commit()
        print("Обновление завершено успешно.")

# Шаг 4: Восстановление комментариев представлений
        for matviewname, comment in view_comments.items():
            if comment:  # Восстанавливаем только если был комментарий
                cursor.execute(f"""
                    COMMENT ON MATERIALIZED VIEW public.{matviewname} IS %s;
                """, (comment,))
        print("Комментарии представлений восстановлены.")

        # Шаг 5: Восстановление комментариев колонок
        for matviewname, columns in column_comments.items():
            for column_name, comment in columns.items():
                if comment:  # Восстанавливаем только если был комментарий
                    cursor.execute(f"""
                        COMMENT ON COLUMN public.{matviewname}.{column_name} IS %s;
                    """, (comment,))
        print("Комментарии колонок восстановлены.")

        # Шаг 6: Изменение владельца и прав
        for matviewname in view_comments.keys():
            cursor.execute(f"""
                ALTER TABLE public.{matviewname} OWNER TO saprr_dev_power_users;
                GRANT ALL ON TABLE public.{matviewname} TO saprr_dev_power_users;
            """)
        print("Изменение владельца и прав выполнено.")

        # Зафиксировать все изменения
        conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Ошибка: {e}")

finally:
    conn.close()



# Создание таблицы, если её нет
create_table_query = """
CREATE TABLE IF NOT EXISTS coowners_target_table (
    id SERIAL PRIMARY KEY,
    company_spark_id VARCHAR,
    company_id INT,
    is_rep BOOLEAN,
    coowner_name VARCHAR,
    spark_id VARCHAR,
    address VARCHAR,
    inn VARCHAR,
    ogrn VARCHAR,
    okpo VARCHAR,
    okopf_name VARCHAR,
    manager VARCHAR,
    manager_inn VARCHAR,
    full_name VARCHAR,
    share_part VARCHAR,
    management_company_name VARCHAR,
    management_company_spark_id VARCHAR,
    management_company_inn VARCHAR,
    management_company_ogrn VARCHAR,
    trustee_name VARCHAR,
    trustee_spark_id VARCHAR,
    trustee_inn VARCHAR,
    trustee_ogrn VARCHAR,
    trustee_input_date DATE
);
"""
with conn.cursor() as cursor:
    cursor.execute(create_table_query)
    conn.commit()

# Directory with JSON files
directory_path = r"C:\VNIIR\stucture\structure"

# Collecting data from JSON files
json_coowners_data = []
for filename in os.listdir(directory_path):
    if filename.endswith(".json"):
        file_path = os.path.join(directory_path, filename)
        if os.path.getsize(file_path) > 0:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        else:
            print(f"Файл {file_path} пуст. Пропуск...")
            continue
        

        # Extract SparkID and Coowners
        spark_id = data.get("Data", {}).get("Report", [{}])[0].get("SparkID")
        
        # Attempt to get CoownerEGRUL data first
        coowners_egrul = data.get("Data", {}).get("Report", [{}])[0].get("CoownersEGRUL", {})
        coowner_data = coowners_egrul.get("CoownerEGRUL", [])
        actual_date = coowners_egrul.get("@ActualDate")
        
        # Fallback to CoownersFCSM if CoownerEGRUL is missing
        if not coowner_data:
            coowners_fcsm = data.get("Data", {}).get("Report", [{}])[0].get("CoownersFCSM", {})
            coowner_data = coowners_fcsm.get("CoownerFCSM", [])
            actual_date = coowners_fcsm.get("@ActualDate")

        # Collect data
        for coowner in coowner_data:
            management_company = coowner.get("ManagementCompany", {})
            trustee = coowner.get("Trustee", {})
            json_coowners_data.append({
                "CompanySparkID": spark_id,
                "ActualDate": actual_date,
                "CoownerName": coowner.get("Name"),
                "SparkID": coowner.get("SparkID"),
                "Address": coowner.get("Address"),
                "INN": coowner.get("INN"),
                "OGRN": coowner.get("OGRN"),
                "OKPO": coowner.get("OKPO"),
                "OKOPFName": coowner.get("OKOPFName"),
                "Manager": coowner.get("Manager"),
                "ManagerInn": coowner.get("ManagerInn"),
                "FullName": coowner.get("FullName"),
                "SharePart": coowner.get("SharePart"),
                "ManagementCompanyName": management_company.get("Name"),
                "ManagementCompanySparkID": management_company.get("SparkID"),
                "ManagementCompanyINN": management_company.get("INN"),
                "ManagementCompanyOGRN": management_company.get("OGRN"),
                "TrusteeName": trustee.get("Name"),
                "TrusteeSparkID": trustee.get("SparkId"),
                "TrusteeINN": trustee.get("INN"),
                "TrusteeOGRN": trustee.get("OGRN"),
                "TrusteeInputDate": trustee.get("InputDate")
            })

# Convert JSON data into a DataFrame
df_json_coowners = pd.DataFrame(json_coowners_data)

# Enrich data with companies table
companies_query = "SELECT id AS company_id, spark_id, is_rep FROM companies"
companies_df = pd.read_sql(companies_query, con=engine)

# Приведение типов данных
df_json_coowners['CompanySparkID'] = df_json_coowners['CompanySparkID'].astype(str)
companies_df['spark_id'] = companies_df['spark_id'].astype(str)

df_enriched_coowners = df_json_coowners.merge(
    companies_df,
    left_on='CompanySparkID',
    right_on='spark_id',
    how='left'
)

# Insert enriched data into the database
for record in df_enriched_coowners.to_dict(orient="records"):
    values = (
        record["CompanySparkID"],
        record.get("company_id"),
        record.get("is_rep"),
        record.get("CoownerName"),
        record.get("SparkID"),
        record.get("Address"),
        record.get("INN"),
        record.get("OGRN"),
        record.get("OKPO"),
        record.get("OKOPFName"),
        record.get("Manager"),
        record.get("ManagerInn"),
        record.get("FullName"),
        record.get("SharePart"),
        record.get("ManagementCompanyName"),
        record.get("ManagementCompanySparkID"),
        record.get("ManagementCompanyINN"),
        record.get("ManagementCompanyOGRN"),
        record.get("TrusteeName"),
        record.get("TrusteeSparkID"),
        record.get("TrusteeINN"),
        record.get("TrusteeOGRN"),
        record.get("TrusteeInputDate")
    )

    # SQL query for insertion
    insert_query = """
    INSERT INTO target_table (
        company_spark_id, company_id, is_rep, coowner_name, spark_id,
        address, inn, ogrn, okpo, okopf_name, manager, manager_inn,
        full_name, share_part, management_company_name, management_company_spark_id,
        management_company_inn, management_company_ogrn, trustee_name, trustee_spark_id,
        trustee_inn, trustee_ogrn, trustee_input_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, values)
    except Exception as e:
        print(f"Error inserting record: {e}")
        conn.rollback()

# Commit the transaction and close the connection
conn.commit()
conn.close()


df_ovner_table = select('select distinct company_spark_id from target_table')

df_ovner_table

# Убедитесь, что типы данных совпадают
df_structure['spark_id'] = df_structure['spark_id'].astype(str)
df_ovner_table['company_spark_id'] = df_ovner_table['company_spark_id'].astype(str)

# Найти значения, которых нет в df_owner_table
missing_spark_ids = set(df_structure['spark_id']) - set(df_ovner_table['company_spark_id'])

# Преобразовать в DataFrame для удобного отображения
missing_spark_ids_df = pd.DataFrame(missing_spark_ids, columns=['missing_spark_id'])
missing_spark_ids_df



# Insert enriched data into the database
# Удаляем поля, отсутствующие в таблице
for record in df_enriched_rosstat.to_dict(orient="records"):
    values = (
        record["CompanySparkID"],
        record.get("company_id"),
        record.get("is_rep"),
        record.get("CoownerName"),
        record.get("SparkID"),
        record.get("AddressOrComment"),  # Соответствует столбцу `address`
        record.get("INN"),
        record.get("OGRN"),
        record.get("OKPO"),
        record.get("OKOPFName"),
        record.get("Manager"),
        record.get("ManagerInn"),
        record.get("FullName"),
        record.get("SharePart"),
        record.get("ManagementCompanyName"),
        record.get("ManagementCompanySparkID"),
        record.get("ManagementCompanyINN"),
        record.get("ManagementCompanyOGRN"),
        record.get("TrusteeName"),
        record.get("TrusteeSparkID"),
        record.get("TrusteeINN"),
        record.get("TrusteeOGRN"),
        record.get("TrusteeInputDate")
    )

    insert_query = """
    INSERT INTO target_table (
        company_spark_id, company_id, is_rep, coowner_name, spark_id,
        address, inn, ogrn, okpo, okopf_name, manager, manager_inn,
        full_name, share_part, management_company_name, management_company_spark_id,
        management_company_inn, management_company_ogrn, trustee_name, trustee_spark_id,
        trustee_inn, trustee_ogrn, trustee_input_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, values)
    except Exception as e:
        print(f"Error inserting record for SparkID {record['CompanySparkID']}: {e}")
        conn.rollback()

# Commit changes
conn.commit()

conn.close()

# Display collected Rosstat data
tools.display_dataframe_to_user(name="Rosstat Coowners Data (Enriched)", dataframe=df_enriched_rosstat)




