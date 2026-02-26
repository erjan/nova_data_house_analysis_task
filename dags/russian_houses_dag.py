"""
DAG для анализа данных о российских домах с использованием PySpark и ClickHouse
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, floor, count, max as spark_max, min as spark_min, avg, expr
from pyspark.sql.types import DoubleType, IntegerType
import clickhouse_connect
import os
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt

# Конфигурация ClickHouse из переменных окружения
CH_HOST = os.getenv('CH_HOST', 'clickhouse')
CH_PORT = int(os.getenv('CH_PORT', 8123))
CH_USER = os.getenv('CH_USER', 'default')
CH_PASSWORD = os.getenv('CH_PASSWORD', 'clickhouse123')
CH_DATABASE = os.getenv('CH_DATABASE', 'russian_houses_db')

# Пути к файлам
CSV_FILE_PATH = '/opt/airflow/data/russian_houses.csv'
PARQUET_FILE_PATH = '/opt/airflow/data/russian_houses.parquet'

# Глобальная переменная для SparkSession
spark = None


def get_spark_session():
    """Создание или получение SparkSession"""
    global spark
    if spark is None:
        spark = SparkSession.builder \
            .appName("RussianHousesAnalysis") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
    return spark


def read_parquet_data(spark):
    """Чтение предварительно обработанных данных из Parquet"""
    df = spark.read.parquet(PARQUET_FILE_PATH)
    print(f"✅ Parquet прочитан. Колонки: {df.columns}")
    return df


def read_csv_with_bom_fix(spark):
    """Чтение CSV с правильной кодировкой (UTF-16 LE) и полными параметрами"""
    # CSV файл имеет UTF-16 LE BOM (0xFF 0xFE), поэтому читаем с правильной кодировкой
    df = spark.read.csv(
        CSV_FILE_PATH,
        header=True,
        encoding='UTF-16LE',
        quote='"',
        escape='"',
        multiLine=True,
        mode="PERMISSIVE",
        columnNameOfCorruptRecord="_corrupt_record"
    )
    
    print(f"✅ CSV прочитан. Колонки: {df.columns}")
    
    return df


def stop_spark_session():
    """Остановка SparkSession"""
    global spark
    if spark is not None:
        spark.stop()
        spark = None


def prepare_data_to_parquet(**context):
    """
    Задача 0: Подготовка данных - чтение CSV один раз, очистка и сохранение в Parquet
    Это гарантирует, что все последующие задачи работают с одними и теми же очищенными данными
    """
    print("=" * 80)
    print("ЗАДАЧА 0: Подготовка данных - Чтение CSV и сохранение в Parquet")
    print("=" * 80)
    
    spark = get_spark_session()
    
    # Читаем CSV с правильной кодировкой
    df = read_csv_with_bom_fix(spark)
    
    # Подсчет количества строк
    row_count = df.count()
    print(f"\n✅ Количество строк в исходном CSV: {row_count}")
    
    # Показываем схему данных
    print("\nИсходная схема данных:")
    df.printSchema()
    
    # Преобразование типов данных
    print("\nПреобразование типов данных...")
    df_transformed = df \
        .withColumn("house_id", col("house_id").cast(IntegerType())) \
        .withColumn("latitude", col("latitude").cast(DoubleType())) \
        .withColumn("longitude", col("longitude").cast(DoubleType())) \
        .withColumn("maintenance_year", col("maintenance_year").cast(IntegerType())) \
        .withColumn("square", col("square").cast(DoubleType())) \
        .withColumn("population", col("population").cast(IntegerType())) \
        .withColumnRenamed("house_id", "id") \
        .withColumnRenamed("maintenance_year", "year") \
        .withColumnRenamed("square", "area") \
        .withColumnRenamed("population", "floors") \
        .withColumnRenamed("locality_name", "city")
    
    # Удаляем строки с null значениями в критичных полях
    df_clean = df_transformed.filter(
        col("id").isNotNull() & 
        col("year").isNotNull() & 
        col("area").isNotNull()
    ).select("id", "latitude", "longitude", "year", "area", "floors", "region", "city", "address", "description")
    
    clean_count = df_clean.count()
    print(f"✅ Количество строк после очистки: {clean_count}")
    
    print("\nПреобразованная схема данных:")
    df_clean.printSchema()
    
    # Сохраняем очищенные данные в Parquet (перезаписываем, если уже существует)
    print(f"\nСохранение очищенных данных в {PARQUET_FILE_PATH}...")
    df_clean.coalesce(1).write.mode("overwrite").parquet(PARQUET_FILE_PATH)
    print(f"✅ Данные успешно сохранены в Parquet")
    
    # Сохраняем статистику в XCom
    context['ti'].xcom_push(key='total_rows', value=row_count)
    context['ti'].xcom_push(key='clean_rows', value=clean_count)
    
    return clean_count


def load_csv_to_spark(**context):
    """
    Задача 1: Информация о загруженных данных (данные уже подготовлены в задаче 0)
    """
    print("=" * 80)
    print("ЗАДАЧА 1: Информация о загруженных данных")
    print("=" * 80)
    
    spark = get_spark_session()
    df = read_parquet_data(spark)
    
    # Подсчет количества строк
    row_count = df.count()
    print(f"\n✅ Количество строк в датасете: {row_count}")
    
    # Показываем схему данных
    print("\nСхема данных:")
    df.printSchema()
    
    # Показываем первые несколько строк
    print("\nПервые 5 строк данных:")
    df.show(5, truncate=False)
    
    # Сохраняем количество строк в XCom
    context['ti'].xcom_push(key='row_count', value=row_count)
    
    return row_count


def validate_data(**context):
    """
    Задача 2: Проверка корректности данных (данные уже очищены в задаче 0)
    """
    print("=" * 80)
    print("ЗАДАЧА 2: Проверка корректности данных")
    print("=" * 80)
    
    spark = get_spark_session()
    df = read_parquet_data(spark)
    
    # Проверка на пустые строки
    total_rows = df.count()
    print(f"\nОбщее количество строк: {total_rows}")
    
    # Проверка на null значения в каждой колонке
    print("\nПроверка на NULL значения:")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0
        print(f"  {column}: {null_count} NULL ({null_percentage:.2f}%)")
    
    # Проверка, что данные корректно прочитаны
    print("\n✅ Данные успешно прочитаны")
    print(f"✅ Количество колонок: {len(df.columns)}")
    print(f"✅ Названия колонок: {df.columns}")
    
    return True


def transform_data(**context):
    """
    Задача 3: Проверка преобразованных типов данных (преобразование уже выполнено в задаче 0)
    """
    print("=" * 80)
    print("ЗАДАЧА 3: Проверка преобразованных типов данных")
    print("=" * 80)
    
    spark = get_spark_session()
    df = read_parquet_data(spark)
    
    print("\nСхема данных:")
    df.printSchema()
    
    print("\nТипы данных для каждого столбца:")
    for column_name, data_type in df.dtypes:
        print(f"  {column_name}: {data_type}")
    
    # Проверка количества строк после очистки
    clean_count = df.count()
    print(f"\n✅ Количество строк после очистки: {clean_count}")
    
    # Показываем примеры данных
    print("\nПримеры данных:")
    df.show(10, truncate=False)
    
    context['ti'].xcom_push(key='clean_count', value=clean_count)
    
    return clean_count


def calculate_year_statistics(**context):
    """
    Задача 4: Вычисление среднего и медианного года постройки зданий
    """
    print("=" * 80)
    print("ЗАДАЧА 4: Статистика по годам постройки")
    print("=" * 80)
    
    spark = get_spark_session()
    df = read_parquet_data(spark)
    
    # Фильтруем данные с не-null значениями года
    df_filtered = df.filter(col("year").isNotNull())
    
    # Проверка: сколько строк осталось после фильтрации
    filtered_count = df_filtered.count()
    print(f"\n📊 Строк после фильтрации NULL: {filtered_count}")
    
    if filtered_count == 0:
        print("❌ ERROR: Все строки были отфильтрованы!")
        return None
    
    # Вычисление среднего года
    avg_year_result = df_filtered.select(avg("year")).collect()
    print(f"Debug: avg_year_result = {avg_year_result}")
    avg_year = avg_year_result[0][0] if avg_year_result and avg_year_result[0][0] is not None else None
    
    if avg_year is None:
        print("❌ ERROR: avg_year is None!")
        return None
        
    print(f"\n📊 Средний год постройки: {avg_year:.2f}")
    
    # Вычисление медианного года
    median_year = df_filtered.stat.approxQuantile("year", [0.5], 0.01)[0]
    print(f"📊 Медианный год постройки: {median_year}")
    
    context['ti'].xcom_push(key='avg_year', value=avg_year)
    context['ti'].xcom_push(key='median_year', value=median_year)
    
    return {"avg_year": avg_year, "median_year": median_year}


def top_regions_and_cities(**context):
    """
    Задача 5: Определение топ-10 областей и городов с наибольшим количеством объектов
    """
    print("=" * 80)
    print("ЗАДАЧА 5: Топ-10 регионов и городов")
    print("=" * 80)
    
    spark = get_spark_session()
    df = read_parquet_data(spark)
    
    # Топ-10 регионов
    print("\n📊 Топ-10 регионов по количеству объектов:")
    top_regions = df.groupBy("region") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(10)
    
    top_regions.show(truncate=False)
    
    # Топ-10 городов
    print("\n📊 Топ-10 городов по количеству объектов:")
    top_cities = df.groupBy("city") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc()) \
        .limit(10)
    
    top_cities.show(truncate=False)
    
    # Создание графиков
    try:
        # График для регионов
        regions_data = top_regions.collect()
        regions_names = [row['region'] for row in regions_data]
        regions_counts = [row['count'] for row in regions_data]
        
        plt.figure(figsize=(12, 6))
        plt.barh(regions_names, regions_counts)
        plt.xlabel('Количество объектов')
        plt.ylabel('Регион')
        plt.title('Топ-10 регионов по количеству объектов')
        plt.tight_layout()
        plt.savefig('/opt/airflow/data/top_regions.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("\n✅ График топ-10 регионов сохранен: /opt/airflow/data/top_regions.png")
        
        # График для городов
        cities_data = top_cities.collect()
        cities_names = [row['city'] for row in cities_data]
        cities_counts = [row['count'] for row in cities_data]
        
        plt.figure(figsize=(12, 6))
        plt.barh(cities_names, cities_counts)
        plt.xlabel('Количество объектов')
        plt.ylabel('Город')
        plt.title('Топ-10 городов по количеству объектов')
        plt.tight_layout()
        plt.savefig('/opt/airflow/data/top_cities.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("✅ График топ-10 городов сохранен: /opt/airflow/data/top_cities.png")
        
    except Exception as e:
        print(f"⚠️  Ошибка при создании графиков: {e}")
    
    return True


def buildings_area_by_region(**context):
    """
    Задача 6: Найти здания с максимальной и минимальной площадью в рамках каждой области
    """
    print("=" * 80)
    print("ЗАДАЧА 6: Здания с макс/мин площадью в каждом регионе")
    print("=" * 80)
    
    spark = get_spark_session()
    df = read_parquet_data(spark)
    
    df_filtered = df.filter(col("area").isNotNull())
    
    # Агрегация по регионам
    area_stats = df_filtered.groupBy("region") \
        .agg(
            spark_max("area").alias("max_area"),
            spark_min("area").alias("min_area"),
            count("*").alias("count")
        ) \
        .orderBy(col("max_area").desc())
    
    print("\nСтатистика площадей по регионам (топ-20):")
    area_stats.show(20, truncate=False)
    
    # Создание графика
    try:
        stats_data = area_stats.limit(15).collect()
        regions = [row['region'] for row in stats_data]
        max_areas = [row['max_area'] for row in stats_data]
        min_areas = [row['min_area'] for row in stats_data]
        
        x = range(len(regions))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.bar([i - width/2 for i in x], max_areas, width, label='Макс. площадь', alpha=0.8)
        ax.bar([i + width/2 for i in x], min_areas, width, label='Мин. площадь', alpha=0.8)
        
        ax.set_xlabel('Регион')
        ax.set_ylabel('Площадь (кв.м)')
        ax.set_title('Максимальная и минимальная площадь зданий по регионам')
        ax.set_xticks(x)
        ax.set_xticklabels(regions, rotation=45, ha='right')
        ax.legend()
        
        plt.tight_layout()
        plt.savefig('/opt/airflow/data/area_by_region.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("\n✅ График площадей по регионам сохранен: /opt/airflow/data/area_by_region.png")
        
    except Exception as e:
        print(f"⚠️  Ошибка при создании графика: {e}")
    
    return True


def buildings_by_decade(**context):
    """
    Задача 7: Определить количество зданий по десятилетиям
    """
    print("=" * 80)
    print("ЗАДАЧА 7: Количество зданий по десятилетиям")
    print("=" * 80)
    
    spark = get_spark_session()
    df = read_parquet_data(spark)
    
    df_filtered = df.filter(col("year").isNotNull())
    
    # Группировка по десятилетиям
    df_decades = df_filtered \
        .withColumn("decade", (floor(col("year") / 10) * 10).cast(IntegerType())) \
        .groupBy("decade") \
        .agg(count("*").alias("count")) \
        .orderBy("decade")
    
    print("\nКоличество зданий по десятилетиям:")
    df_decades.show(50, truncate=False)
    
    # Создание графика
    try:
        decades_data = df_decades.collect()
        decades = [row['decade'] for row in decades_data]
        counts = [row['count'] for row in decades_data]
        
        plt.figure(figsize=(14, 7))
        plt.bar(decades, counts, width=8, edgecolor='black', alpha=0.7)
        plt.xlabel('Десятилетие')
        plt.ylabel('Количество зданий')
        plt.title('Распределение зданий по десятилетиям постройки')
        plt.xticks(decades, [f"{d}s" for d in decades], rotation=45)
        plt.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('/opt/airflow/data/buildings_by_decade.png', dpi=300, bbox_inches='tight')
        plt.close()
        print("\n✅ График зданий по десятилетиям сохранен: /opt/airflow/data/buildings_by_decade.png")
        
    except Exception as e:
        print(f"⚠️  Ошибка при создании графика: {e}")
    
    return True


def create_clickhouse_table(**context):
    """
    Задача 8: Создать схему таблицы в ClickHouse
    """
    print("=" * 80)
    print("ЗАДАЧА 8: Создание таблицы в ClickHouse")
    print("=" * 80)
    
    try:
        # Подключение к ClickHouse
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD
        )
        
        # Создание базы данных
        print(f"\nСоздание базы данных {CH_DATABASE}...")
        client.command(f"CREATE DATABASE IF NOT EXISTS {CH_DATABASE}")
        print(f"✅ База данных {CH_DATABASE} создана")
        
        # Создание таблицы
        print("\nСоздание таблицы houses...")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {CH_DATABASE}.houses
        (
            id Int32,
            latitude Float64,
            longitude Float64,
            year Nullable(Int32),
            area Nullable(Float64),
            floors Nullable(Int32),
            region Nullable(String),
            city Nullable(String),
            address Nullable(String),
            description Nullable(String)
        )
        ENGINE = MergeTree()
        ORDER BY id
        """
        
        client.command(create_table_query)
        print("✅ Таблица houses создана успешно")
        
        # Проверка таблицы
        result = client.command(f"SHOW TABLES FROM {CH_DATABASE}")
        print(f"\nТаблицы в базе {CH_DATABASE}: {result}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Ошибка при создании таблицы: {e}")
        raise
    
    return True


def load_data_to_clickhouse(**context):
    """
    Задача 10: Загрузка обработанных данных из Parquet в таблицу в ClickHouse
    """
    print("=" * 80)
    print("ЗАДАЧА 10: Загрузка данных в ClickHouse")
    print("=" * 80)
    
    spark = get_spark_session()
    
    # Читаем предварительно обработанные данные
    df = read_parquet_data(spark)
    
    print(f"\nКоличество строк для загрузки: {df.count()}")
    
    try:
        # Подключение к ClickHouse
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE
        )
        
        # Конвертируем Spark DataFrame в Pandas для загрузки
        print("\nКонвертация DataFrame в Pandas...")
        pandas_df = df.toPandas()
        
        print(f"Размер данных: {len(pandas_df)} строк")
        
        # Загрузка данных батчами
        print("\nЗагрузка данных в ClickHouse...")
        batch_size = 50000
        total_rows = len(pandas_df)
        
        for i in range(0, total_rows, batch_size):
            batch = pandas_df.iloc[i:i+batch_size]
            client.insert_df(
                'houses',
                batch
            )
            print(f"  Загружено {min(i+batch_size, total_rows)}/{total_rows} строк")
        
        print("✅ Данные успешно загружены в ClickHouse")
        
        # Проверка количества строк в таблице
        count_result = client.query("SELECT count() as cnt FROM houses")
        db_count = count_result.result_rows[0][0]
        print(f"\n📊 Количество строк в таблице ClickHouse: {db_count}")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Ошибка при загрузке данных: {e}")
        raise
    
    return True


def query_top_houses(**context):
    """
    Задача 11: Выполнить SQL скрипт, который выведет топ 25 домов с площадью больше 60 кв.м
    """
    print("=" * 80)
    print("ЗАДАЧА 11: Запрос топ-25 домов с площадью > 60 кв.м")
    print("=" * 80)
    
    try:
        # Подключение к ClickHouse
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE
        )
        
        # SQL запрос
        query = """
        SELECT 
            id,
            region,
            city,
            address,
            area,
            year,
            floors
        FROM houses
        WHERE area > 60
        ORDER BY area DESC
        LIMIT 25
        """
        
        print("\nВыполнение SQL запроса...")
        result = client.query(query)
        
        print("\n📊 Топ-25 домов с площадью больше 60 кв.м:")
        print("=" * 150)
        print(f"{'ID':<10} {'Регион':<30} {'Город':<25} {'Площадь':<12} {'Год':<10} {'Этажей':<10}")
        print("=" * 150)
        
        for row in result.result_rows:
            id_val, region, city, address, area, year, floors = row
            print(f"{id_val:<10} {region[:28]:<30} {city[:23]:<25} {area:<12.2f} {year if year else 'N/A':<10} {floors if floors else 'N/A':<10}")
        
        print("=" * 150)
        print(f"\n✅ Найдено {len(result.result_rows)} записей")
        
        client.close()
        
    except Exception as e:
        print(f"❌ Ошибка при выполнении запроса: {e}")
        raise
    
    return True


# Определение DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'russian_houses_analysis',
    default_args=default_args,
    description='Анализ данных о российских домах с использованием PySpark и ClickHouse',
    schedule_interval=None,
    catchup=False,
    tags=['pyspark', 'clickhouse', 'data-analysis'],
)

# Определение задач
task_0_prepare = PythonOperator(
    task_id='prepare_data_to_parquet',
    python_callable=prepare_data_to_parquet,
    dag=dag,
)

task_1_load = PythonOperator(
    task_id='load_csv_to_spark',
    python_callable=load_csv_to_spark,
    dag=dag,
)

task_2_validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

task_3_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

task_4_year_stats = PythonOperator(
    task_id='calculate_year_statistics',
    python_callable=calculate_year_statistics,
    dag=dag,
)

task_5_top_regions = PythonOperator(
    task_id='top_regions_and_cities',
    python_callable=top_regions_and_cities,
    dag=dag,
)

task_6_area_stats = PythonOperator(
    task_id='buildings_area_by_region',
    python_callable=buildings_area_by_region,
    dag=dag,
)

task_7_decades = PythonOperator(
    task_id='buildings_by_decade',
    python_callable=buildings_by_decade,
    dag=dag,
)

task_8_create_table = PythonOperator(
    task_id='create_clickhouse_table',
    python_callable=create_clickhouse_table,
    dag=dag,
)

task_10_load_to_ch = PythonOperator(
    task_id='load_data_to_clickhouse',
    python_callable=load_data_to_clickhouse,
    dag=dag,
)

task_11_query = PythonOperator(
    task_id='query_top_houses',
    python_callable=query_top_houses,
    dag=dag,
)

task_cleanup = PythonOperator(
    task_id='cleanup_spark',
    python_callable=stop_spark_session,
    dag=dag,
)

# Определение зависимостей между задачами
task_0_prepare >> task_1_load >> task_2_validate >> task_3_transform
# Задачи 4-7 выполняются последовательно, чтобы избежать нехватки памяти
task_3_transform >> task_4_year_stats >> task_5_top_regions >> task_6_area_stats >> task_7_decades
task_7_decades >> task_8_create_table
task_8_create_table >> task_10_load_to_ch >> task_11_query >> task_cleanup
