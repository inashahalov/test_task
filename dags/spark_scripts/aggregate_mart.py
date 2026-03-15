"""
Spark скрипт для агрегации витрины данных.

Назначение:
    Создаёт итоговую витрину с месячными агрегатами по странам.
    Читает из stage_covid, записывает в mart_deaths_monthly.

Агрегаты:
    - Total_Confirmed_Monthly: Сумма подтверждённых случаев за месяц
    - Peak_Confirmed_Monthly: Максимальное значение за день в месяце
    - Total_Deaths_Monthly: Сумма смертей за месяц

Примечание:
    Поле Deaths заполняется нулями, так как в исходных данных
    эта информация отсутствует.

Безопасность:
    - Пароли не логируются
    - Конфигурация через переменные окружения
    - Чувствительные данные не выводятся в stdout

Автор: Илья
Дата: 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max, year, month
import os

# =============================================================================
# КОНФИГУРАЦИЯ ПОДКЛЮЧЕНИЯ К БАЗЕ ДАННЫХ
# =============================================================================

# Переменные окружения из docker-compose.yml
# Примечание: имена должны соответствовать .env файлу
DB_HOST = os.getenv('POSTGRES_NAME', 'postgres')        # ← Исправлено
DB_PORT = os.getenv('POSTGRES_PORT_IN', '5432')         # ← Исправлено
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
print(" Password: ***скрыто***")

# =============================================================================
# ИНИЦИАЛИЗАЦИЯ SPARK СЕССИИ
# =============================================================================

spark = SparkSession.builder \
    .appName("COVID_Mart_Aggregate") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Устанавливаем уровень логирования для чистоты вывода
spark.sparkContext.setLogLevel("WARN")

try:
    # ==========================================================================
    # ЧТЕНИЕ ДАННЫХ ИЗ STAGE СЛОЯ
    # ==========================================================================

    print("\n" + "=" * 60)
    print("ШАГ 1: Чтение данных из stage_covid")
    print("=" * 60)

    df_stage = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "public.stage_covid") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .load()

    print(f"Количество строк: {df_stage.count()}")
    print(f"Имена колонок: {df_stage.columns}")

    # ==========================================================================
    # ДОБАВЛЕНИЕ КОЛОНОК ГОДА И МЕСЯЦА
    # ==========================================================================

    print("\n" + "=" * 60)
    print("ШАГ 2: Добавление колонок Year и Month")
    print("=" * 60)

    df_with_periods = df_stage \
        .withColumn("Year", year(col("Date"))) \
        .withColumn("Month", month(col("Date")))

    # ==========================================================================
    # АГРЕГАЦИЯ ПО СТРАНАМ И ПЕРИОДАМ
    # ==========================================================================

    print("\n" + "=" * 60)
    print("ШАГ 3: Агрегация данных")
    print("=" * 60)

    df_mart = df_with_periods \
        .groupBy("Country_Region", "Year", "Month") \
        .agg(
            sum("Confirmed").alias("Total_Confirmed_Monthly"),
            max("Confirmed").alias("Peak_Confirmed_Monthly"),
            sum("Deaths").alias("Total_Deaths_Monthly"),
        ) \
        .orderBy(
            "Year",
            "Month",
            col("Total_Confirmed_Monthly").desc()
        )

    print(f"Количество агрегированных записей: {df_mart.count()}")

    # ==========================================================================
    # ЗАПИСЬ В MART СЛОЙ
    # ==========================================================================

    print("\n" + "=" * 60)
    print("ШАГ 4: Запись в mart_deaths_monthly")
    print("=" * 60)

    df_mart.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "public.mart_deaths_monthly") \
        .option("user", DB_USER) \
        .option("password", DB_PASS) \
        .mode("overwrite") \
        .save()

    print(f"Успешно записано в mart_deaths_monthly: {df_mart.count()} записей")

    # ==========================================================================
    # ПРЕДПРОСМОТР РЕЗУЛЬТАТА
    # ==========================================================================

    print("\n" + "=" * 60)
    print("ПРЕДПРОСМОТР: Первые 10 записей")
    print("=" * 60)
    df_mart.show(10, truncate=False)

except Exception as e:
    print(f"\nОШИБКА: {e}")
    raise

finally:
    # Завершение Spark сессии
    spark.stop()
    print("\nSpark сессия закрыта")