"""
COVID-19 ETL Pipeline

Архитектура:
    RAW (CSV файлы) -> STAGE (нормализованные данные) -> MART (агрегированная витрина)

Слои данных:
    - RAW: Загрузка исходных CSV файлов в PostgreSQL без изменений
    - STAGE: Трансформация Wide формата в Long (unpivot дат)
    - MART: Агрегация показателей по месяцам и странам

Примечание:
    В исходных данных отсутствует колонка Deaths. В витрине это поле
    заполняется нулями. Это ограничение источника данных должно быть
    отражено в документации проекта.

Технический стек:
    - Airflow 2.x (оркестрация)
    - Python 3.10 + Pandas (обработка данных)
    - PostgreSQL 16 (хранение)
    - psycopg2 (подключение к БД без SQLAlchemy)

Автор: Илья
Дата: 2026
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os


# =============================================================================
# КОНФИГУРАЦИЯ DAG
# =============================================================================

DEFAULT_ARGS = {
    'owner': 'ilya',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BASE_PATH = '/usr/local/airflow'
DATA_PATH = f'{BASE_PATH}/data'
SPARK_SCRIPTS_PATH = f'{BASE_PATH}/dags/spark_scripts'

# =============================================================================
#  КОНФИГУРАЦИЯ БД (соответствует .env файлу)
# =============================================================================

# # ПРАВИЛЬНО для продакшена (убери дефолты):
# DB_CONFIG = {
#     'host': os.getenv('POSTGRES_NAME'),
#     'port': os.getenv('POSTGRES_PORT_IN'),
#     'database': os.getenv('POSTGRES_DB'),
#     'user': os.getenv('POSTGRES_USER'),
#     'password': os.getenv('POSTGRES_PASSWORD'),
# }

# Для тестового задания :
DB_CONFIG = {
    'host': os.getenv('POSTGRES_NAME', 'postgres'),
    'port': os.getenv('POSTGRES_PORT_IN', '5432'),
    'database': os.getenv('POSTGRES_DB', 'airflow'),
    'user': os.getenv('POSTGRES_USER', 'airflow'),
    'password': os.getenv('POSTGRES_PASSWORD', 'airflow'),
}


def get_db_connection():
    """
    Создаёт подключение к PostgreSQL через psycopg2.

    Returns:
        psycopg2.connection: Объект подключения к базе данных PostgreSQL
    """
    return psycopg2.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password']
    )


def read_df_from_sql(query):
    """
    Читает данные из PostgreSQL и возвращает результат в виде DataFrame.

    Args:
        query (str): SQL запрос для выполнения

    Returns:
        pandas.DataFrame: Результат запроса с именами колонок
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)

        # Получаем имена колонок из метаданных курсора
        columns = [desc[0] for desc in cursor.description]

        # Получаем все строки результата
        rows = cursor.fetchall()

        # Создаём DataFrame с правильными именами колонок
        df = pd.DataFrame(rows, columns=columns)

        print(f"  Прочитано строк: {len(df)}")
        print(f"  Прочитано колонок: {len(df.columns)}")
        print(f"  Имена колонок: {df.columns.tolist()}")

        return df

    finally:
        cursor.close()
        conn.close()


# =============================================================================
# RAW LAYER: Загрузка CSV файлов в PostgreSQL
# =============================================================================

def load_raw_files(**context):
    """
    Загружает все CSV файлы (raw_2020.csv - raw_2023.csv) в таблицу raw_covid.
    """
    years = [2020, 2021, 2022, 2023]
    all_dfs = []

    print("=" * 60)
    print("RAW LAYER: Загрузка CSV файлов")
    print("=" * 60)

    for year in years:
        file_path = f'{DATA_PATH}/raw_{year}.csv'
        print(f"Проверка файла: {file_path}")

        if os.path.exists(file_path):
            df = pd.read_csv(file_path, sep=';')
            df['Source_Year'] = year
            all_dfs.append(df)
            print(f"  Успешно загружен: raw_{year}.csv ({df.shape[0]} строк, {df.shape[1]} колонок)")
        else:
            print(f"  Файл не найден: raw_{year}.csv")

    if all_dfs:
        df_combined = pd.concat(all_dfs, ignore_index=True)
        df_combined.columns = df_combined.columns.str.replace('/', '_').str.replace('.', '_')

        print(f"\nОбъединённый DataFrame: {df_combined.shape[0]} строк, {df_combined.shape[1]} колонок")

        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS public.raw_covid CASCADE")

            columns_def = []
            for col in df_combined.columns:
                columns_def.append(f'"{col}" TEXT')

            create_table_sql = f"CREATE TABLE public.raw_covid ({', '.join(columns_def)})"
            cursor.execute(create_table_sql)
            print(f"Таблица raw_covid создана с {len(df_combined.columns)} колонками")

            insert_sql = "INSERT INTO public.raw_covid VALUES %s"
            values = [tuple(x) for x in df_combined.values]
            execute_values(cursor, insert_sql, values)
            conn.commit()

            print(f"Успешно записано в таблицу raw_covid: {df_combined.shape[0]} строк")

        finally:
            cursor.close()
            conn.close()

        return {
            'total_rows': int(df_combined.shape[0]),
            'years_processed': len(all_dfs)
        }
    else:
        raise Exception("Ни один файл не найден! Проверьте пути и наличие файлов.")


# =============================================================================
# STAGE LAYER: Трансформация Wide -> Long формат
# =============================================================================

def transform_stage_pandas(**context):
    """
    Преобразует данные из широкого формата (Wide) в длинный (Long).
    """
    print("=" * 60)
    print("STAGE LAYER: Трансформация Wide -> Long")
    print("=" * 60)

    df = read_df_from_sql("SELECT * FROM raw_covid")

    print(f"Всего прочитано из raw_covid: {len(df)} строк")

    if len(df) == 0:
        raise Exception("Таблица raw_covid пуста! Проверьте RAW слой.")

    id_cols = ['Country_Region', 'Province_State', 'Source_Year']

    date_cols = [c for c in df.columns if c[0].isdigit()]
    print(f"\nНайдено колонок с датами: {len(date_cols)}")

    if len(date_cols) == 0:
        raise Exception("Не найдены колонки с датами для трансформации!")

    df_long = df.melt(
        id_vars=id_cols,
        value_vars=date_cols,
        var_name='Date_Str',
        value_name='Confirmed'
    )

    print(f"\nПосле операции melt(): {len(df_long)} строк")

    if len(df_long) == 0:
        raise Exception("После melt() получилось 0 строк!")

    # Конвертация форматов дат: 1_22_20 -> 1/22/20
    print("\nКонвертация форматов дат...")
    df_long['Date_Str_Fixed'] = df_long['Date_Str'].str.replace('_', '/')

    # Парсинг дат
    df_long['Date'] = pd.to_datetime(df_long['Date_Str_Fixed'], format='%m/%d/%y', errors='coerce')

    null_dates = df_long['Date'].isnull().sum()
    print(f"Строк с некорректными датами (NaT): {null_dates}")

    if null_dates == len(df_long):
        print("\nВсе даты не распарсились! Пробуем альтернативный формат...")
        df_long['Date'] = pd.to_datetime(df_long['Date_Str_Fixed'], errors='coerce')
        null_dates = df_long['Date'].isnull().sum()
        print(f"После альтернативного парсинга NaT: {null_dates}")

    # Преобразование Confirmed в числовой тип
    df_long['Confirmed'] = pd.to_numeric(
        df_long['Confirmed'],
        errors='coerce'
    ).fillna(0).astype(int)

    df_long['Deaths'] = 0

    # Удаляем строки с некорректными датами
    df_before_drop = len(df_long)
    df_long = df_long.dropna(subset=['Date'])
    print(f"\nУдалено строк с некорректными датами: {df_before_drop - len(df_long)}")

    print(f"\nПеред записью в БД: {len(df_long)} строк")

    if len(df_long) == 0:
        raise Exception("После очистки осталось 0 строк! Нельзя записать пустую таблицу.")

    output_cols = [
        'Country_Region',
        'Province_State',
        'Source_Year',
        'Date',
        'Confirmed',
        'Deaths'
    ]

    print(f"\nЗапись в таблицу stage_covid...")

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS public.stage_covid CASCADE")

        create_table_sql = """
            CREATE TABLE public.stage_covid (
                Country_Region TEXT,
                Province_State TEXT,
                Source_Year INTEGER,
                Date TIMESTAMP,
                Confirmed INTEGER,
                Deaths INTEGER
            )
        """
        cursor.execute(create_table_sql)
        print("Таблица stage_covid создана")

        insert_sql = """
            INSERT INTO public.stage_covid 
            (Country_Region, Province_State, Source_Year, Date, Confirmed, Deaths)
            VALUES %s
        """

        values = [tuple(x) for x in df_long[output_cols].values]
        print(f"Подготовлено записей для вставки: {len(values)}")

        execute_values(cursor, insert_sql, values)
        conn.commit()

        print(f"Успешно записано в stage_covid: {len(df_long)} строк")

    except Exception as e:
        print(f"Ошибка при записи в stage_covid: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    return {'stage_rows': len(df_long)}


# =============================================================================
# MART LAYER: Агрегация витрины по месяцам
# =============================================================================

def aggregate_mart_pandas(**context):
    """
    Создаёт агрегированную витрину данных по месяцам и странам.
    """
    print("=" * 60)
    print("MART LAYER: Агрегация витрины")
    print("=" * 60)

    df = read_df_from_sql("SELECT * FROM stage_covid")

    print(f"Всего прочитано из stage_covid: {len(df)} строк")
    print(f"Имена колонок: {df.columns.tolist()}")

    if len(df) == 0:
        raise Exception("Таблица stage_covid пуста! Проверьте STAGE слой.")

    # ==========================================================================
    # КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Приводим имена колонок к ожидаемому виду
    # PostgreSQL может хранить имена в нижнем регистре
    # ==========================================================================

    # Создаём маппинг имён колонок (lowercase -> оригинальное имя)
    col_mapping = {col.lower(): col for col in df.columns}
    print(f"\nМаппинг колонок: {col_mapping}")

    # Определяем фактические имена колонок
    date_col = col_mapping.get('date')
    confirmed_col = col_mapping.get('confirmed')
    deaths_col = col_mapping.get('deaths')
    country_col = col_mapping.get('country_region')

    print(f"\nФактические имена колонок:")
    print(f"  Date: {date_col}")
    print(f"  Confirmed: {confirmed_col}")
    print(f"  Deaths: {deaths_col}")
    print(f"  Country_Region: {country_col}")

    # Преобразуем дату в datetime
    df['Date'] = pd.to_datetime(df[date_col])

    # Извлекаем год и месяц для группировки
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month

    # Группировка по стране, году, месяцу
    df_mart = df.groupby([country_col, 'Year', 'Month']).agg(
        Total_Confirmed_Monthly=(confirmed_col, 'sum'),
        Peak_Confirmed_Monthly=(confirmed_col, 'max'),
        Total_Deaths_Monthly=(deaths_col, 'sum')
    ).reset_index()

    # Переименовываем колонку страны для консистентности
    df_mart = df_mart.rename(columns={country_col: 'Country_Region'})

    # Сортировка
    df_mart = df_mart.sort_values(
        ['Year', 'Month', 'Total_Confirmed_Monthly'],
        ascending=[True, True, False]
    )

    print(f"\nАгрегировано записей: {len(df_mart)}")

    if len(df_mart) == 0:
        raise Exception("После агрегации осталось 0 записей!")

    # Запись в таблицу mart_deaths_monthly
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS public.mart_deaths_monthly CASCADE")

        create_table_sql = """
            CREATE TABLE public.mart_deaths_monthly (
                Country_Region TEXT,
                Year INTEGER,
                Month INTEGER,
                Total_Confirmed_Monthly BIGINT,
                Peak_Confirmed_Monthly INTEGER,
                Total_Deaths_Monthly BIGINT
            )
        """
        cursor.execute(create_table_sql)
        print("Таблица mart_deaths_monthly создана")

        insert_sql = """
            INSERT INTO public.mart_deaths_monthly 
            (Country_Region, Year, Month, Total_Confirmed_Monthly, Peak_Confirmed_Monthly, Total_Deaths_Monthly)
            VALUES %s
        """

        values = [tuple(x) for x in df_mart.values]
        print(f"Подготовлено записей для вставки: {len(values)}")

        execute_values(cursor, insert_sql, values)
        conn.commit()

        print(f"Успешно записано в mart_deaths_monthly: {len(df_mart)} записей")

        # Проверка после записи
        check_df = read_df_from_sql("SELECT COUNT(*) as cnt FROM mart_deaths_monthly")
        print(f"Проверка: в таблице {check_df.iloc[0]['cnt']} записей")

    except Exception as e:
        print(f"Ошибка при записи в mart_deaths_monthly: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

    return {'mart_rows': len(df_mart)}


# =============================================================================
# ОПРЕДЕЛЕНИЕ DAG
# =============================================================================

with DAG(
    dag_id='covid_etl_pipeline',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=['covid', 'etl', 'test_task'],
    description='ETL пайплайн для обработки данных COVID-19',
) as dag:

    raw_load = PythonOperator(
        task_id='raw_load_csv_to_postgres',
        python_callable=load_raw_files,
        provide_context=True,
    )

    stage_transform = PythonOperator(
        task_id='stage_wide_to_long',
        python_callable=transform_stage_pandas,
    )

    mart_aggregate = PythonOperator(
        task_id='mart_aggregate_monthly',
        python_callable=aggregate_mart_pandas,
    )

    raw_load >> stage_transform >> mart_aggregate