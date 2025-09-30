# utils/spark_singleton.py

import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from django.conf import settings

def get_spark_session() -> SparkSession:
    """
    Creates a Spark session with proper environment variables.
    This should be called inside each Celery task to ensure environment is ready.
    """
    os.environ["JAVA_HOME"] = os.getenv("JAVA_HOME", r"C:\java\jdk-17.0.2")
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", r"C:\hadoop")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")
    os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
    spark = SparkSession.builder \
        .appName("WebServerLogAnalyzer") \
        .config("spark.driver.extraJavaOptions", "--enable-native-access=ALL-UNNAMED") \
        .config("spark.executor.extraJavaOptions", "--enable-native-access=ALL-UNNAMED") \
        .getOrCreate()

    return spark

def get_product_dataframe() -> DataFrame:
    spark = get_spark_session()

    # ✅ Use absolute path
    csv_path = os.path.join(settings.BASE_DIR, "front", "utils", "shein-products.csv")
    print(f"📂 Using CSV path: {csv_path}")

    # ✅ Read CSV safely
    df = spark.read.option("header", True) \
        .option("inferSchema", True) \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .csv(csv_path)

    # ✅ Handle corrupt records
    if "_corrupt_record" in df.columns:
        corrupt_count = df.filter(df["_corrupt_record"].isNotNull()).count()
        print(f"❌ Found {corrupt_count} corrupt rows")
        df = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")

    if df.rdd.isEmpty():
        raise ValueError("CSV file is empty or contains only invalid rows.")

    df = df.cache()
    print("✅ Product DataFrame loaded successfully with columns:", df.columns)
    return df
