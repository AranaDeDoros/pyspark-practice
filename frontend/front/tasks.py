# tasks.py
import json
import os

from celery import shared_task
from pyspark.sql.functions import col, desc, count, first, lower, trim

from . import settings
from .utils.spark_singleton import get_product_dataframe
from .classes.ReportType import ReportType

@shared_task
def process_products(option: str):
    try:
        print("ctm||", option)
        df = get_product_dataframe()

        df = df.withColumn("final_price", col("final_price").cast("double"))
        df = df.dropna(subset=["product_name", "final_price"])
        df = df.fillna({"color": "unknown", "final_price": 0.0})

        if option == ReportType.ON_STOCK.value:
            df = df.withColumn("in_stock_clean", lower(trim(col("in_stock"))))
            df = df.filter(col("in_stock_clean") == "true")
            df = df.select("product_name", "final_price", "in_stock", "color", "size", "root_category").limit(10)

        elif option == ReportType.OVER_10.value:
            df = df.filter(col("final_price") >= 10)
            df = df.select("product_name", "final_price", "in_stock", "color", "size", "root_category").limit(10)

        elif option == ReportType.MOST_POPULAR_COLOR.value:
            df = df.groupBy("color") \
                .agg(count("*").alias("count"), first("product_name").alias("product_name")) \
                .orderBy(desc("count")) \
                .limit(10)

        elif option == ReportType.MOST_EXPENSIVE.value:
            df = df.orderBy(desc("final_price")) \
                .select("product_name", "final_price", "in_stock", "color", "size", "root_category") \
                .limit(10)

        else:
            return {"error": "Invalid report option provided"}

        json_result = df.toJSON().collect()
        return [json.loads(row) for row in json_result]

    except Exception as e:
        print(f"❌ Error in process_products: {e}")
        return {"status": "failed", "error": str(e)}

@shared_task
def test_spark_csv():
    try:
        df = get_product_dataframe()
        sample = df.limit(5).toJSON().collect()
        print("✅ Successfully read CSV and converted to JSON")
        return sample
    except Exception as e:
        print(f"❌ Error in test_spark_csv: {e}")
        return {"error": str(e)}

@shared_task
def test_file_access():
    csv_path = os.path.join(settings.BASE_DIR, "front", "utils", "shein-products.csv")
    exists = os.path.isfile(csv_path)
    size = os.path.getsize(csv_path) if exists else -1
    return {"exists": exists, "size": size, "path": csv_path}


@shared_task
def add(x, y):
    return x + y