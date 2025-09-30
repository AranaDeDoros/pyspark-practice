# views.py
import json
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from .classes.ReportType import ReportType
from front.tasks import process_products
from celery.result import AsyncResult

def index(request):
    combo = [
        {"key": ReportType.NO_STOCK.value , "label":"No stock"},
        {"key": ReportType.OVER_10.value , "label":"Over 10 usd" },
        {"key": ReportType.MOST_POPULAR_COLOR.value, "label":"Most popular color"},
        {"key": ReportType.MOST_EXPENSIVE.value, "label":"Most expensive"}
    ]
    return render(request,"index.html", {"combo": combo})

@csrf_exempt
def ml(request):
    if request.method != "POST":
        return JsonResponse({"error": "Only POST requests are allowed"}, status=405)

    try:
        data = json.loads(request.body)
        option = data.get("option")

        valid_options = {r.value for r in ReportType}
        if option not in valid_options:
            return JsonResponse({"error": "Invalid report option selected."}, status=400)

        task = process_products.delay(option)
        return JsonResponse({"task_id": task.id})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

@csrf_exempt
def ml_result(request, task_id):
    res = AsyncResult(task_id)

    if res.failed():
        return JsonResponse({
            "status": "failed",
            "error": str(res.result) if res.result else "Unknown error"
        }, status=500)

    if res.ready():
        return JsonResponse(res.result, safe=False)

    return JsonResponse({"status": "pending"})
