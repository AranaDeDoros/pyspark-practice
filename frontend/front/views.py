# my_tennis_club/members/views.py
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from pyspark.sql.functions import col, expr, desc
import json

from .classes.ReportType import ReportType
from front.SparkSessionSingleton import get_product_dataframe


def index(request):
    combo = [
            {"key": ReportType.NO_STOCK.value , "label":"No stock"},
            {"key": ReportType.OVER_10.value , "label":"Over 10 usd" },
            {"key": ReportType.MOST_POPULAR_COLOR.value, "label":"Most popular color"},
            {"key": ReportType.MOST_EXPENSIVE.value, "label":"Most expensive"}
        ]
    return render(request,"index.html", {"combo":combo})

@csrf_exempt
def ml(request):

    df_clean = None
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            option = data.get('option')
            df_clean = get_product_dataframe()
            df_clean = df_clean.withColumn("final_price", expr("try_cast(final_price as double)"))
            df_clean = df_clean.dropna(subset=["product_name", "final_price"])
            df_clean = df_clean.fillna({
                "color": "unknown",
                "final_price": 0.0
            })
            #df_clean = df_clean.filter((col("final_price") >= 0) & (col("final_price").isNotNull()))
            print("OPTION ", option)
            match option:
                case ReportType.NO_STOCK.value:
                    print("no stockkkk")
                    df_clean = df_clean.withColumn("in_stock_bool", expr("try_cast(in_stock as boolean)"))
                    df_clean = df_clean.filter(col("in_stock_bool") == True).select("product_name", "final_price", "in_stock", "color",   "size", "root_category").limit(10)
                case  ReportType.OVER_10.value:
                    print("over 10")
                    df_clean = df_clean.withColumn("final_price_int", expr("try_cast(final_price as double)"))
                    df_clean = df_clean.filter(col("final_price_int") >= 10).select("product_name", "final_price", "in_stock", "color", "size",   "root_category").limit(10)
                case ReportType.MOST_POPULAR_COLOR.value:
                    df_clean = df_clean.select("product_name", "final_price", "in_stock", "color", "size", "root_category").groupBy("color").count().orderBy(desc("count")).limit(10)
                case _:
                    df_clean = df_clean.orderBy(desc("final_price")).select("product_name", "final_price", "in_stock", "color", "size", "root_category").limit(0)

            json_result = df_clean.toJSON().collect()
            data = [json.loads(row) for row in json_result]
            return JsonResponse(data, safe=False)
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON format'}, status=400)

    else:
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)


