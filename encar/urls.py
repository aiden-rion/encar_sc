# encar/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path("combine/", views.combine_page),
    path("api/combine/list", views.combine_list_api),
    path("api/combine/export.xlsx", views.combine_export_xlsx),
    path("api/debug/table", views.debug_table_api),
    path("api/combine/summary", views.combine_summary_api),
    path("api/combine/price-analysis", views.combine_price_analysis_api),
]
