from django.contrib import admin
from .models import CarQueue, VehicleRaw, InspectionRaw, RecordRaw, OptionsChoiceRaw
print("✅ encar.admin loaded")

@admin.register(CarQueue)
class CarQueueAdmin(admin.ModelAdmin):
    list_display = ("car_id", "status", "retry_count", "updated_at")
    list_filter = ("status",)
    search_fields = ("car_id",)
    ordering = ("-updated_at",)


@admin.register(VehicleRaw)
class VehicleRawAdmin(admin.ModelAdmin):
    list_display = ("car_id", "title", "year", "mileage", "price", "fetched_at")
    search_fields = ("car_id",)
    ordering = ("-fetched_at",)
    readonly_fields = ("car_id", "payload", "fetched_at")


@admin.register(InspectionRaw)
class InspectionRawAdmin(admin.ModelAdmin):
    list_display = ("car_id", "vehicle_id", "is_not_found", "fetched_at")
    search_fields = ("car_id",)
    ordering = ("-fetched_at",)
    readonly_fields = ("car_id", "payload", "fetched_at")


@admin.register(RecordRaw)
class RecordRawAdmin(admin.ModelAdmin):
    list_display = ("car_id", "vehicle_no", "accident_cnt", "owner_change_cnt", "fetched_at")
    search_fields = ("car_id", "vehicle_no")
    ordering = ("-fetched_at",)
    readonly_fields = ("car_id", "vehicle_no", "payload", "fetched_at")


@admin.register(OptionsChoiceRaw)
class OptionsChoiceRawAdmin(admin.ModelAdmin):
    list_display = ("car_id", "options_count", "options_top", "fetched_at")
    search_fields = ("car_id",)
    ordering = ("-fetched_at",)
    readonly_fields = ("car_id", "payload", "fetched_at")
