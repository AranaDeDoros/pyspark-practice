import os
from celery import Celery

os.environ["PYSPARK_PYTHON"] = r"C:\Users\dontb\AppData\Local\Programs\Python\Python310\python.exe"
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "front.settings")

app = Celery("front")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()

