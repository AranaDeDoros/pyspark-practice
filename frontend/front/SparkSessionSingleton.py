# utils/spark_singleton.py

import os
from pyspark.sql import SparkSession as PySparkSession, DataFrame

# Internal storage for singleton instances
_spark: PySparkSession = None
_df: DataFrame = None

def get_spark_session() -> PySparkSession:
    global _spark
    if _spark is None:
        # Java JDK path
        os.environ["JAVA_HOME"] = r"C:\java\jdk-17.0.2"
        os.environ["PATH"] += ";" + os.path.join(os.environ["JAVA_HOME"], "bin")

        # Hadoop path
        os.environ['HADOOP_HOME'] = r'C:\hadoop'
        os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

        _spark = PySparkSession.builder \
            .appName("WebServerLogAnalyzer") \
            .getOrCreate()
    return _spark


def get_product_dataframe() -> DataFrame:
    global _df
    if _df is None:
        spark = get_spark_session()
        _df = spark.read.option("header", True) \
            .option("inferSchema", True) \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("quote", '"') \
            .option("escape", '"') \
            .csv("front/shein-products.csv", header=True, inferSchema=True)

    if "_corrupt_record" in _df.columns:
        df_invalid = _df.filter(_df["_corrupt_record"].isNotNull())
        df_invalid.write.csv("bad_rows.csv")
    else:
        print("No corrupt records found. Proceeding with full dataset.")
    return _df
