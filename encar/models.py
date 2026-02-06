from django.db import models

# Create your models here.

from django.db import models
import json

class CarQueue(models.Model):
    car_id = models.TextField(primary_key=True)
    status = models.TextField()
    retry_count = models.IntegerField()
    last_error = models.TextField(null=True)
    updated_at = models.TextField(null=True)

    class Meta:
        db_table = "car_queue"
        managed = False


class VehicleRaw(models.Model):
    car_id = models.TextField(primary_key=True)
    payload = models.TextField(null=True)
    fetched_at = models.TextField(null=True)

    class Meta:
        db_table = "vehicle_raw"
        managed = False

    def _json(self):
        try:
            return json.loads(self.payload) if self.payload else {}
        except Exception:
            return {}

    # ✅ 목록 표시용 파싱 필드들 (응답 구조 바뀌어도 안전하게)
    def title(self):
        j = self._json()
        return j.get("title") or j.get("Title") or ""

    def year(self):
        j = self._json()
        return j.get("year") or j.get("Year") or j.get("modelYear") or ""

    def price(self):
        j = self._json()
        return j.get("price") or j.get("Price") or j.get("salePrice") or ""

    def mileage(self):
        j = self._json()
        return j.get("mileage") or j.get("Mileage") or j.get("km") or ""


class InspectionRaw(models.Model):
    car_id = models.TextField(primary_key=True)
    payload = models.TextField(null=True)
    fetched_at = models.TextField(null=True)

    class Meta:
        db_table = "inspection_raw"
        managed = False

    def _json(self):
        try:
            return json.loads(self.payload) if self.payload else {}
        except Exception:
            return {}

    def is_not_found(self):
        j = self._json()
        return j.get("_meta") == "NOT_FOUND"

    def vehicle_id(self):
        j = self._json()
        return j.get("vehicleId") or ""


class RecordRaw(models.Model):
    car_id = models.TextField(primary_key=True)
    vehicle_no = models.TextField(null=True)
    payload = models.TextField(null=True)
    fetched_at = models.TextField(null=True)

    class Meta:
        db_table = "record_raw"
        managed = False

    def _json(self):
        try:
            return json.loads(self.payload) if self.payload else {}
        except Exception:
            return {}

    def accident_cnt(self):
        j = self._json()
        return j.get("accidentCnt")

    def owner_change_cnt(self):
        j = self._json()
        return j.get("ownerChangeCnt")


class OptionsChoiceRaw(models.Model):
    car_id = models.TextField(primary_key=True)
    payload = models.TextField(null=True)
    fetched_at = models.TextField(null=True)

    class Meta:
        db_table = "options_choice_raw"
        managed = False

    def _json(self):
        try:
            return json.loads(self.payload) if self.payload else []
        except Exception:
            return []

    def options_count(self):
        j = self._json()
        return len([x for x in j if isinstance(x, dict)]) if isinstance(j, list) else 0

    def options_top(self, max_items=8):
        j = self._json()
        if not isinstance(j, list):
            return ""
        names = []
        for it in j[:max_items]:
            if isinstance(it, dict):
                n = it.get("name") or it.get("title")
                if n:
                    names.append(str(n))
        return " | ".join(names)

