"""
Spark скрипт для трансформации данных из Wide формата в Long.

Назначение:
    Читает данные из таблицы raw_covid и преобразует колонки с датами
    в строки (unpivot операция). Результат записывается в stage_covid.

Архитектура:
    PostgreSQL (raw_covid) -> Spark (transform) -> PostgreSQL (stage_covid)

Примечание:
    Используется JDBC коннектор для чтения/записи в PostgreSQL.
    Для работы требуется пакет org.postgresql:postgresql:42.6.0

Безопасность:
    - Пароли не логируются
    - Конфигурация через переменные окружения
    - Чувствительные данные не выводятся в stdout

Автор: Илья
Дата: 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_timestamp, lit
from pyspark.sql.types import IntegerType
import os

# =============================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ
# =============================================================================

# Переменные окружения из docker-compose.yml
# Примечание: имена должны соответствовать .env файлу
DB_HOST = os.getenv('POSTGRES_NAME', 'postgres')      # ← Исправлено на POSTGRES_NAME
DB_PORT = os.getenv('POSTGRES_PORT_IN', '5432')       # ← Исправлено на POSTGRES_PORT_IN
DB_NAME = os.getenv('POSTGRES_DB', 'airflow')
DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'airflow')

# JDBC URL для подключения Spark к PostgreSQL
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =============================================================================
#  БЕЗОПАСНОЕ ЛОГИРОВАНИЕ (без паролей!)
# =============================================================================

print("Конфигурация подключения:")
print(f"  Host: {DB_HOST}")
print(f"  Port: {DB_PORT}")
print(f"  Database: {DB_NAME}")
print(f"  User: {DB_USER}")
#  НЕ печатаем пароль и полный JDBC URL!
print("  Password: ***скрыто***")

# =============================================================================
# ИНИЦИАЛИЗАЦИЯ SPARK СЕССИИ
# =============================================================================

spark = SparkSession.builder \
    .appName("COVID_Stage_Transform") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Устанавливаем уровень логирования для чистоты вывода
spark.sparkContext.setLogLevel("WARN")

try:
    # ==========================================================================
    # ЧТЕНИЕ ДАННЫХ ИЗ RAW СЛОЯ
    # ==========================================================================

    print("\n" + "=" * 60)
    print("ШАГ 1: Чтение данных из raw_covid")
    print("=" * 60)

    df_raw = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "public.raw_covid") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .load()

    print(f"Количество строк: {df_raw.count()}")
    print(f"Количество колонок: {len(df_raw.columns)}")
    print(f"Имена колонок: {df_raw.columns[:10]}...")

    # ==========================================================================
    # ОПРЕДЕЛЕНИЕ КОЛОНОК ДЛЯ ТРАНСФОРМАЦИИ
    # ==========================================================================

    id_cols = ['Country_Region', 'Province_State', 'Source_Year']
    date_cols = [c for c in df_raw.columns if c[0].isdigit()]

    print(f"\nКолонки-идентификаторы: {id_cols}")
    print(f"Количество колонок с датами: {len(date_cols)}")

    # ==========================================================================
    # TRANSFORM: UNPIVOT ЧЕРЕЗ STACK
    # ==========================================================================

    print("\n" + "=" * 60)
    print("ШАГ 2: Трансформация Wide -> Long")
    print("=" * 60)

    if date_cols:
        columns_expr = ", ".join([f"'{c}', {c}" for c in date_cols])

        df_long = df_raw.selectExpr(
            *id_cols,
            f"stack({len(date_cols)}, {columns_expr}) as (Date_Str, Confirmed)"
        )

        print(f"Строк после stack(): {df_long.count()}")

        # ==========================================================================
        # ОЧИСТКА И ПРЕОБРАЗОВАНИЕ ДАННЫХ
        # ==========================================================================

        print("\n" + "=" * 60)
        print("ШАГ 3: Очистка и преобразование типов")
        print("=" * 60)

        df_clean = df_long \
            .withColumn("Date",
                        when(
                            col("Date_Str").rlike("^\\d{1,2}/\\d{1,2}/\\d{2}$"),
                            to_timestamp(col("Date_Str"), "M/d/yy")
                        )
                        .when(
                            col("Date_Str").rlike("^\\d{2}\\.\\d{2}\\.\\d{4}$"),
                            to_timestamp(col("Date_Str"), "dd.MM.yyyy")
                        )
                        .otherwise(None)
                        ) \
            .withColumn("Confirmed", col("Confirmed").cast(IntegerType())) \
            .withColumn("Deaths", lit(0)) \
            .withColumn("Source_Year", col("Source_Year").cast(IntegerType())) \
            .filter(col("Date").isNotNull()) \
            .filter(col("Confirmed").isNotNull())

        print(f"Строк после очистки: {df_clean.count()}")

        # ==========================================================================
        # ЗАПИСЬ В STAGE СЛОЙ
        # ==========================================================================

        print("\n" + "=" * 60)
        print("ШАГ 4: Запись в stage_covid")
        print("=" * 60)

        df_clean.write \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", "public.stage_covid") \
            .option("user", DB_USER) \
            .option("password", DB_PASS) \
            .mode("overwrite") \
            .save()

        print(f"Успешно записано в stage_covid: {df_clean.count()} строк")

    else:
        raise Exception("Не найдены колонки с датами для трансформации!")

except Exception as e:
    print(f"\nОШИБКА: {e}")
    raise

finally:
    spark.stop()
    print("\nSpark сессия закрыта")