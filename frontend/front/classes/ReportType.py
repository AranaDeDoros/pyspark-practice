from enum import Enum
class ReportType(Enum):
    NO_STOCK = "no_stock"
    OVER_10 ="over_10_usd"
    MOST_POPULAR_COLOR = "most_popular_color"
    MOST_EXPENSIVE = "most_expensive"