from django.apps import AppConfig

from front.SparkSessionSingleton import get_product_dataframe


class FrontendConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'front'

    def ready(self):

        try:
            print("🔥 Loading product DataFrame at startup...")
            get_product_dataframe()
            print("✅ Product DataFrame ready.")
        except Exception as e:
            print(f"❌ Failed to load product DataFrame: {e}")
