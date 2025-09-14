# my_tennis_club/members/views.py
from django.shortcuts import render
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from pyspark.sql import SparkSession


from pyspark.sql.functions import regexp_extract, col, count, expr, desc, asc, max
import os
import json

from .classes.ReportType import ReportType


def index(request):
    combo = [
            {"key": ReportType.NO_STOCK , "label":"No stock"},
            {"key": ReportType.OVER_10 , "label":"Over 10 usd" },
            {"key": ReportType.MOST_POPULAR_COLOR, "label":"Most popular color"},
            {"key": ReportType.MOST_EXPENSIVE, "label":"Most expensive"}
        ]
    return render(request,"index.html", {"combo":combo})

@csrf_exempt
def ml(request):
    # Java JDK path
    os.environ["JAVA_HOME"] = r"C:\java\jdk-17.0.2"
    os.environ["PATH"] += ";" + os.path.join(os.environ["JAVA_HOME"], "bin")
    print(request.body)
    # Hadoop path
    os.environ['HADOOP_HOME'] = r'C:\hadoop'
    os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')
    df_clean = None
    if request.method == 'POST':
        try:
            data = json.loads(request.body)

            option = data.get('option')

            spark = SparkSession.builder.appName("WebServerLogAnalyzer").getOrCreate()

            df = spark.read.option("header", True) \
                .option("inferSchema", True) \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .option("quote", '"') \
                .option("escape", '"') \
                .csv("front/shein-products.csv", header=True, inferSchema=True)

            if "_corrupt_record" in df.columns:
                df_clean = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
                df_invalid = df.filter(df["_corrupt_record"].isNotNull())
                df_invalid.write.csv("bad_rows.csv")
            else:
                df_clean = df
                print("No corrupt records found. Proceeding with full dataset.")

            df_clean = df.withColumn("final_price", expr("try_cast(final_price as double)"))
            df_clean = df_clean.dropna(subset=["product_name", "final_price"])
            df_clean = df_clean.fillna({
                "color": "unknown",
                "final_price": 0.0
            })
            df_clean = df_clean.filter((col("final_price") >= 0) & (col("final_price").isNotNull()))
            match option:
                case ReportType.NO_STOCK:
                    print("no stockkkk")
                    df_clean = df_clean.withColumn("in_stock_bool", expr("try_cast(in_stock as boolean)"))
                    df_clean = df_clean.filter(col("in_stock_bool") == False).select("product_name", "final_price", "in_stock", "color",   "size", "root_category").limit(10)
                case  ReportType.OVER_10:
                    df_clean = df_clean.withColumn("final_price_int", expr("try_cast(final_price as double)"))
                    df_clean = df_clean.filter(col("final_price_int") >= 10).select("product_name", "final_price", "in_stock", "color", "size",   "root_category").limit(10)
                case ReportType.MOST_POPULAR_COLOR:
                    df_clean = df_clean.select("product_name", "final_price", "in_stock", "color", "size", "root_category").groupBy("color").count().orderBy(desc("count")).limit(10)
                case _:
                    df_clean = df_clean.orderBy(desc("final_price")).select("product_name", "final_price", "in_stock", "color", "size", "root_category")


            return JsonResponse(df_clean.toJSON().collect(), safe=False)
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON format'}, status=400)

    else:
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)


