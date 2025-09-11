from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, expr,desc, asc, max
import os

# Java JDK path
os.environ["JAVA_HOME"] = r"C:\java\jdk-17.0.2"
os.environ["PATH"] += ";" + os.path.join(os.environ["JAVA_HOME"], "bin")

# Hadoop path
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')

spark = SparkSession.builder.appName("WebServerLogAnalyzer").getOrCreate()

df = spark.read.option("header", True) \
               .option("inferSchema", True) \
               .option("mode", "PERMISSIVE") \
               .option("columnNameOfCorruptRecord", "_corrupt_record") \
               .csv("shein-products.csv", header=True, inferSchema=True)

if "_corrupt_record" in df.columns:
    df_clean = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
    df_invalid = df.filter(df["_corrupt_record"].isNotNull())
    df_invalid.write.csv("bad_rows.csv")
else:
    df_clean = df
    print("No corrupt records found. Proceeding with full dataset.")

df_clean = df.withColumn("final_price", expr("try_cast(final_price as double)"))
df_clean = df_clean.fillna({
    "color": "unknown",
    "final_price": 0.0
})
df_clean = df_clean.filter((col("final_price") >= 0) & (col("final_price").isNotNull()))

print("#no stock#")
df_clean = df_clean.withColumn("in_stock_bool", expr("try_cast(in_stock as boolean)"))
df_clean.filter(col("in_stock_bool") == False).select("product_name", "final_price", "in_stock", "color", "size", "root_category").show(10)


print("#over 10usd#")
df_clean = df_clean.withColumn("final_price_int", expr("try_cast(final_price as double)"))
df_clean.filter(col("final_price_int") >= 10).select("product_name", "final_price", "in_stock", "color", "size", "root_category").show(10)


print("#most popular color#")
df_clean.select("product_name", "final_price", "in_stock", "color", "size", "root_category").groupBy("color").count().orderBy(desc("count")).limit(1).show()

print("#least popular colors#")
df_clean.select("product_name", "final_price", "in_stock", "color", "size", "root_category").groupBy("color").count().orderBy(asc("count")).limit(10).show()

print("#most expensive product#")
df_clean.orderBy(desc("final_price")).select("product_name", "final_price", "in_stock", "color", "size", "root_category").show(1, truncate=True)

