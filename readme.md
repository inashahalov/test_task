## Запуск инфраструктуры
Для запуска необходима команда `docker compose up [-d]`

В случае возникновения ошибки `jupyter-1 'Kernel does not exist` необходимо выполнить следующие команды:
```bash
# Остановка всех контейнеров
docker-compose down

# Перезапуск
docker-compose up -d
```

## Инфраструктура
Инструменты запущены по следующим портам для внешнего использования:
- **Airflow** - 8080:8080 (admin, admin);
- **Jupyter** - 8888 (без аутентификации);
- **PostgreSQL** - 5444 (airflow).

Для подключения внутри инструментов контейнера к БД необходимо использовать порт 5432.

В файле `requirements.txt` описаны `python`-пакеты, которые устанавливаются на все инструменты, для работы спарка на эти инструменты так же скачивается `spark-jars` для PostgreSQL.

Все настройки можно изменять при помощи файла `.env`, откуда `docker-compose.yml` берет основные переменные. `Python`-пакеты так же можно модифицировать, но, возможно, придется бороться с отсутствием совместимости у пакетов.

## Работа с инфраструктурой
Датасеты расположены в папке `data` и доступны в `airflow` и `jupyter`.
Для выкладки дагов и спарк-скриптов для них необходимо использовать директорию `dags`. 
После добавления файлов они в течение пары минут появятся в интерфейсе [airflow ui](http://localhost:8080/).

По ссылке [jupyter](http://localhost:8888/) можно тестировать `pyspark` скрипты.

Для всех инструментов есть свои примеры.
При необходимости можно модифицировать `init-scripts` - список скриптов, которые выполняются в БД при запуске.

## Описание задания
В папке `data` есть `csv` файлы с разделением по годам (raw_2020, ..., raw_2023). Это официальные данные по заболевшим COVID-19, поля следующие:
- `Country`, `Region` - страна, регион;
- `Province`, `State` - провинция, штат;
- столбцы с количество заболевших по датам и по дням. 

Данные по столбцам идут нарастающим итогом

Задача:
1. Необходимо загрузить файлы (данные разбиты по годам) (слой `RAW`)
2. Необходимо транспонировать дата сет. Даты должны быть записаны из столбцов в строки (слой `STAGE`) и записать в одну витрину
3. На слое `MART` сделать витрину (количество смертей по месяцам и годам)

## Что можно сделать
Реализовать атомарность потоков для масштабирования ETL-процессов.
Добавить в сборку образа коннект PostgreSQL для создания в Airflow и передавать через параметры `SparkSubmit` в DAG'е, а не хардкодить, как в примерах.
Добавить `.ipynb` с аналитикой полученных данных (выборки, дашборды, статистика).


---

##  Что реализовано

**RAW** | Загрузка CSV файлов по годам
**STAGE** | Трансформация Wide → Long 
**MART** | Агрегация по месяцам/странам


### Ограничения
 `Deaths = 0` — в исходных данных отсутствует колонка смертности
 Полная загрузка (не инкрементальная)


---

##  Файлы проекта

<details>
<summary><strong>1. covid_etl_dag.py (основной DAG)</strong></summary>
transform_stage.py (альтернатива для масштабирования)
aggregate_mart.py (альтернатива для масштабирования)

Сейчас работает Pandas версия, а Spark — альтернатива для масштабирования.

```python
"""
COVID-19 ETL Pipeline
Архитектура: RAW -> STAGE -> MART
Автор: Илья, 2026
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

DEFAULT_ARGS = {
    'owner': 'ilya',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BASE_PATH = '/usr/local/airflow'
DATA_PATH = f'{BASE_PATH}/data'

DB_CONFIG = {
    'host': os.getenv('POSTGRES_NAME', 'postgres'),
    'port': os.getenv('POSTGRES_PORT_IN', '5432'),
    'database': os.getenv('POSTGRES_DB', 'airflow'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def read_df_from_sql(query):
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()
        conn.close()

def load_raw_files(**context):
    years = [2020, 2021, 2022, 2023]
    all_dfs = []
    for year in years:
        file_path = f'{DATA_PATH}/raw_{year}.csv'
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, sep=';')
            df['Source_Year'] = year
            all_dfs.append(df)
    
    df_combined = pd.concat(all_dfs, ignore_index=True)
    df_combined.columns = df_combined.columns.str.replace('/', '_').str.replace('.', '_')
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS public.raw_covid CASCADE")
        columns_def = [f'"{col}" TEXT' for col in df_combined.columns]
        cursor.execute(f"CREATE TABLE public.raw_covid ({', '.join(columns_def)})")
        insert_sql = "INSERT INTO public.raw_covid VALUES %s"
        values = [tuple(x) for x in df_combined.values]
        execute_values(cursor, insert_sql, values)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    
    return {'total_rows': int(df_combined.shape[0])}

def transform_stage_pandas(**context):
    df = read_df_from_sql("SELECT * FROM raw_covid")
    id_cols = ['Country_Region', 'Province_State', 'Source_Year']
    date_cols = [c for c in df.columns if c[0].isdigit()]
    
    df_long = df.melt(id_vars=id_cols, value_vars=date_cols,
                      var_name='Date_Str', value_name='Confirmed')
    df_long['Date_Str_Fixed'] = df_long['Date_Str'].str.replace('_', '/')
    df_long['Date'] = pd.to_datetime(df_long['Date_Str_Fixed'], format='%m/%d/%y', errors='coerce')
    df_long['Confirmed'] = pd.to_numeric(df_long['Confirmed'], errors='coerce').fillna(0).astype(int)
    df_long['Deaths'] = 0
    df_long = df_long.dropna(subset=['Date'])
    
    output_cols = ['Country_Region', 'Province_State', 'Source_Year', 'Date', 'Confirmed', 'Deaths']
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS public.stage_covid CASCADE")
        cursor.execute("""CREATE TABLE public.stage_covid (
            Country_Region TEXT, Province_State TEXT, Source_Year INTEGER,
            Date TIMESTAMP, Confirmed INTEGER, Deaths INTEGER)""")
        insert_sql = """INSERT INTO public.stage_covid 
            (Country_Region, Province_State, Source_Year, Date, Confirmed, Deaths) VALUES %s"""
        values = [tuple(x) for x in df_long[output_cols].values]
        execute_values(cursor, insert_sql, values)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    
    return {'stage_rows': len(df_long)}

def aggregate_mart_pandas(**context):
    df = read_df_from_sql("SELECT * FROM stage_covid")
    col_mapping = {col.lower(): col for col in df.columns}
    
    df['Date'] = pd.to_datetime(df[col_mapping.get('date')])
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    
    df_mart = df.groupby([col_mapping.get('country_region'), 'Year', 'Month']).agg(
        Total_Confirmed_Monthly=(col_mapping.get('confirmed'), 'sum'),
        Peak_Confirmed_Monthly=(col_mapping.get('confirmed'), 'max'),
        Total_Deaths_Monthly=(col_mapping.get('deaths'), 'sum')
    ).reset_index().rename(columns={col_mapping.get('country_region'): 'Country_Region'})
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS public.mart_deaths_monthly CASCADE")
        cursor.execute("""CREATE TABLE public.mart_deaths_monthly (
            Country_Region TEXT, Year INTEGER, Month INTEGER,
            Total_Confirmed_Monthly BIGINT, Peak_Confirmed_Monthly INTEGER,
            Total_Deaths_Monthly BIGINT)""")
        insert_sql = """INSERT INTO public.mart_deaths_monthly 
            (Country_Region, Year, Month, Total_Confirmed_Monthly, Peak_Confirmed_Monthly, Total_Deaths_Monthly) VALUES %s"""
        values = [tuple(x) for x in df_mart.values]
        execute_values(cursor, insert_sql, values)
        conn.commit()
    finally:
        cursor.close()
        conn.close()
    
    return {'mart_rows': len(df_mart)}

with DAG(dag_id='covid_etl_pipeline', default_args=DEFAULT_ARGS,
         schedule_interval=None, catchup=False, tags=['covid', 'etl', 'test_task']) as dag:
    raw_load = PythonOperator(task_id='raw_load_csv_to_postgres', python_callable=load_raw_files)
    stage_transform = PythonOperator(task_id='stage_wide_to_long', python_callable=transform_stage_pandas)
    mart_aggregate = PythonOperator(task_id='mart_aggregate_monthly', python_callable=aggregate_mart_pandas)
    raw_load >> stage_transform >> mart_aggregate
```

</details>

<details>
<summary><strong>2. transform_stage.py (Spark трансформация)</strong></summary>

```python
"""
Spark скрипт для трансформации Wide -> Long
Безопасность: пароли не логируются
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, lit
from pyspark.sql.types import IntegerType
import os

DB_HOST = os.getenv('POSTGRES_NAME', 'postgres')
DB_PORT = os.getenv('POSTGRES_PORT_IN', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'airflow')
DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'airflow')
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Host: {DB_HOST}, Port: {DB_PORT}, Database: {DB_NAME}")
print("Password: ***скрыто***")

spark = SparkSession.builder.appName("COVID_Stage_Transform") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    df_raw = spark.read.format("jdbc").option("url", JDBC_URL) \
        .option("dbtable", "public.raw_covid").option("user", DB_USER) \
        .option("password", DB_PASS).load()
    
    id_cols = ['Country_Region', 'Province_State', 'Source_Year']
    date_cols = [c for c in df_raw.columns if c[0].isdigit()]
    columns_expr = ", ".join([f"'{c}', {c}" for c in date_cols])
    
    df_long = df_raw.selectExpr(*id_cols, f"stack({len(date_cols)}, {columns_expr}) as (Date_Str, Confirmed)")
    
    df_clean = df_long \
        .withColumn("Date", when(col("Date_Str").rlike("^\\d{1,2}/\\d{1,2}/\\d{2}$"),
                    to_timestamp(col("Date_Str"), "M/d/yy"))
                    .when(col("Date_Str").rlike("^\\d{2}\\.\\d{2}\\.\\d{4}$"),
                    to_timestamp(col("Date_Str"), "dd.MM.yyyy")).otherwise(None)) \
        .withColumn("Confirmed", col("Confirmed").cast(IntegerType())) \
        .withColumn("Deaths", lit(0)) \
        .filter(col("Date").isNotNull())
    
    df_clean.write.format("jdbc").option("url", JDBC_URL).option("dbtable", "public.stage_covid") \
        .option("user", DB_USER).option("password", DB_PASS).mode("overwrite").save()
    
    print(f"Записано в stage_covid: {df_clean.count()} строк")
finally:
    spark.stop()
```

</details>

<details>
<summary><strong>3. aggregate_mart.py (Spark агрегация)</strong></summary>

```python
"""
Spark скрипт для агрегации витрины
Безопасность: пароли не логируются
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max, year, month
import os

DB_HOST = os.getenv('POSTGRES_NAME', 'postgres')
DB_PORT = os.getenv('POSTGRES_PORT_IN', '5432')
DB_NAME = os.getenv('POSTGRES_DB', 'airflow')
DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'airflow')
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Host: {DB_HOST}, Port: {DB_PORT}, Database: {DB_NAME}")
print("Password: ***скрыто***")

spark = SparkSession.builder.appName("COVID_Mart_Aggregate") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

try:
    df_stage = spark.read.format("jdbc").option("url", JDBC_URL) \
        .option("dbtable", "public.stage_covid").option("user", DB_USER) \
        .option("password", DB_PASS).load()
    
    df_mart = df_stage \
        .withColumn("Year", year(col("Date"))) \
        .withColumn("Month", month(col("Date"))) \
        .groupBy("Country_Region", "Year", "Month") \
        .agg(sum("Confirmed").alias("Total_Confirmed_Monthly"),
             max("Confirmed").alias("Peak_Confirmed_Monthly"),
             sum("Deaths").alias("Total_Deaths_Monthly")) \
        .orderBy("Year", "Month", col("Total_Confirmed_Monthly").desc())
    
    df_mart.write.format("jdbc").option("url", JDBC_URL).option("dbtable", "public.mart_deaths_monthly") \
        .option("user", DB_USER).option("password", DB_PASS).mode("overwrite").save()
    
    print(f"Записано в mart_deaths_monthly: {df_mart.count()} записей")
finally:
    spark.stop()
```

</details>

---

##  Быстрый старт

```bash
git clone https://github.com/inashahalov/covid-etl-pipeline.git
cd covid-etl-pipeline
cp .env.example .env
docker-compose up -d
# Airflow UI: http://localhost:8080 (admin/admin)
```

---

## Автор

**Илья** | [GitHub](https://github.com/inashahalov) telegram @NSIIya