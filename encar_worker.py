# encar_worker.py
import json
import time
import random
from typing import Any, Dict, Optional, List, Tuple

import requests

from encar_db import connect, init_db, set_status

# -------------------------
# 설정
# -------------------------
INSPECTION_URL = "https://api.encar.com/v1/readside/inspection/vehicle/{carId}"
VEHICLE_URL = "https://api.encar.com/v1/readside/vehicle/{carId}"
USER_URL = "https://api.encar.com/v1/readside/user/{userId}"
RECORD_OPEN_URL = "https://api.encar.com/v1/readside/record/vehicle/{carId}/open?vehicleNo={vehicleNo}"
OPTIONS_CHOICE_URL = "https://api.encar.com/v1/readside/vehicles/car/{carId}/options/choice"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Referer": "https://www.encar.com/",
    "Origin": "https://www.encar.com",
}

TIMEOUT_SEC = (3.0, 12.0)
MAX_RETRIES = 3
SLEEP_BETWEEN_CALLS_SEC = (0.0, 0.15)

# 한 번 실행 시 몇 대 처리할지
BATCH_LIMIT = 100000  # 처음엔 200~500 권장
MAX_RETRY_PER_CAR = 5


# -------------------------
# 유틸
# -------------------------
def sleepy():
    time.sleep(random.uniform(*SLEEP_BETWEEN_CALLS_SEC))


def safe_get(d: Any, path: List[Any], default=None):
    cur = d
    for p in path:
        if cur is None:
            return default
        try:
            if isinstance(p, int):
                if isinstance(cur, list) and 0 <= p < len(cur):
                    cur = cur[p]
                else:
                    return default
            else:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    return default
        except Exception:
            return default
    return cur if cur is not None else default


def first_not_none(*vals):
    for v in vals:
        if v is not None and v != "":
            return v
    return None


def pick_userid_from_vehicle(vehicle_json: Dict[str, Any]) -> Optional[str]:
    uid = (
        safe_get(vehicle_json, ["contact", "userId"])
        or safe_get(vehicle_json, ["contact", "UserId"])
        or safe_get(vehicle_json, ["partnership", "dealer", "userId"])
        or safe_get(vehicle_json, ["partnership", "dealer", "UserId"])
        or vehicle_json.get("userId")
        or vehicle_json.get("UserId")
        or vehicle_json.get("sellerUserId")
        or vehicle_json.get("SellerUserId")
        or vehicle_json.get("dealerUserId")
        or vehicle_json.get("DealerUserId")
        or safe_get(vehicle_json, ["Seller", "userId"])
        or safe_get(vehicle_json, ["Seller", "UserId"])
        or safe_get(vehicle_json, ["Dealer", "userId"])
        or safe_get(vehicle_json, ["Dealer", "UserId"])
        or safe_get(vehicle_json, ["seller", "userId"])
        or safe_get(vehicle_json, ["dealer", "userId"])
    )
    return str(uid) if uid is not None else None


def pick_vehicle_no_from_vehicle(v: Dict[str, Any]) -> Optional[str]:
    vn = first_not_none(
        v.get("vehicleNo"),
        v.get("VehicleNo"),
        v.get("carNo"),
        v.get("CarNo"),
        safe_get(v, ["vehicleNo"]),
        safe_get(v, ["VehicleNo"]),
        safe_get(v, ["spec", "vehicleNo"]),
        safe_get(v, ["Spec", "vehicleNo"]),
        safe_get(v, ["registration", "carNo"]),
        safe_get(v, ["Registration", "carNo"]),
    )
    return str(vn) if vn is not None else None


class EncarClient:
    def __init__(self, headers: Dict[str, str] = None):
        self.s = requests.Session()
        self.s.headers.update(headers or DEFAULT_HEADERS)

    def get_json(self, url: str) -> Any:
        last_err = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.s.get(url, timeout=TIMEOUT_SEC)

                # ✅ 성공
                if resp.status_code == 200:
                    return resp.json()

                # ✅ 여기부터: "재시도하면 안 되는" 케이스
                if resp.status_code in (404, 410):
                    # 성능점검/옵션/기록이 없는 차량이 꽤 있음 → 즉시 스킵
                    raise FileNotFoundError(f"HTTP {resp.status_code} Not Found: {url}")

                if resp.status_code == 400:
                    # 보통 파라미터/요청 문제 → 재시도 의미 거의 없음
                    raise ValueError(f"HTTP 400 Bad Request: {resp.text[:200]}")

                # ✅ 재시도 가치 있는 케이스
                if resp.status_code in (429, 502, 503, 504):
                    last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                    backoff = min(30, (2 ** attempt)) + random.uniform(0, 1.5)
                    time.sleep(backoff)
                    continue

                if resp.status_code == 403:
                    # 차단/보호 장치 가능 → 짧게 몇 번만 시도 후 더 길게 쉬는 게 낫다
                    last_err = RuntimeError(f"HTTP 403: {resp.text[:200]}")
                    backoff = min(60, 10 * attempt) + random.uniform(0, 2.0)
                    time.sleep(backoff)
                    continue

                # 나머지는 에러로 처리
                resp.raise_for_status()

            except (FileNotFoundError, ValueError):
                # ✅ 404/410/400은 즉시 위로 올려서 호출부에서 "스킵" 처리
                raise
            except Exception as e:
                last_err = e
                backoff = min(30, (2 ** attempt)) + random.uniform(0, 1.5)
                time.sleep(backoff)

        raise RuntimeError(f"GET failed: {url}\nlast_err={last_err}")



# -------------------------
# DB upsert
# -------------------------
def upsert_raw(con, table: str, key_col: str, key_val: str, payload_obj: Any, extra_cols: Dict[str, Any] | None = None):
    extra_cols = extra_cols or {}
    payload = json.dumps(payload_obj, ensure_ascii=False)

    cols = [key_col, "payload"] + list(extra_cols.keys())
    vals = [key_val, payload] + list(extra_cols.values())

    placeholders = ",".join(["?"] * len(cols))
    updates = ",".join([f"{c}=excluded.{c}" for c in cols if c != key_col])

    sql = f"""
    INSERT INTO {table} ({",".join(cols)})
    VALUES ({placeholders})
    ON CONFLICT({key_col}) DO UPDATE SET
      {updates},
      fetched_at=datetime('now')
    """
    con.execute(sql, vals)


def fetch_pending_batch(con, limit: int) -> List[str]:
    rows = con.execute(
        """
        SELECT car_id
        FROM car_queue
        WHERE status='PENDING'
        ORDER BY updated_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [r["car_id"] for r in rows]


def main():
    con = connect()
    init_db(con)
    client = EncarClient()

    # user 캐시 (같은 seller 반복 호출 줄임)
    user_cache: Dict[str, Any] = {}

    car_ids = fetch_pending_batch(con, BATCH_LIMIT)
    if not car_ids:
        print("✅ No PENDING cars. done.")
        return

    print(f"✅ Worker start: batch={len(car_ids)}")

    done = 0
    err = 0

    for i, car_id in enumerate(car_ids, start=1):
        print(f"\n[{i}/{len(car_ids)}] carId={car_id}")
        set_status(con, car_id, "RUNNING")

        try:
            # vehicle
            sleepy()
            v = client.get_json(VEHICLE_URL.format(carId=car_id))
            upsert_raw(con, "vehicle_raw", "car_id", car_id, v)

            user_id = pick_userid_from_vehicle(v) if isinstance(v, dict) else None
            vehicle_no = pick_vehicle_no_from_vehicle(v) if isinstance(v, dict) else None

            # inspection
            try:
                sleepy()
                ins = client.get_json(INSPECTION_URL.format(carId=car_id))
                upsert_raw(con, "inspection_raw", "car_id", car_id, ins)
            except FileNotFoundError:
                # ✅ 성능점검 없음: 스킵
                upsert_raw(con, "inspection_raw", "car_id", car_id, {"_meta": "NOT_FOUND"})
            except Exception as e:
                # ✅ 진짜 장애(429/5xx 등)만 여기로
                raise

            # record/open
            if vehicle_no:
                sleepy()
                rec = client.get_json(RECORD_OPEN_URL.format(carId=car_id, vehicleNo=vehicle_no))
                upsert_raw(con, "record_raw", "car_id", car_id, rec, extra_cols={"vehicle_no": vehicle_no})
            else:
                # vehicle_no 없으면 record_raw는 건너뜀
                pass

            # options/choice
            sleepy()
            opt = client.get_json(OPTIONS_CHOICE_URL.format(carId=car_id))
            upsert_raw(con, "options_choice_raw", "car_id", car_id, opt)

            # user (캐시)
            if user_id:
                if user_id not in user_cache:
                    sleepy()
                    u = client.get_json(USER_URL.format(userId=user_id))
                    user_cache[user_id] = u
                    # user_raw는 car_id가 아니라 user_id가 PK
                    upsert_raw(con, "user_raw", "user_id", user_id, u)
                # 캐시된 경우는 DB upsert 생략 가능(원하면 주석 해제)
                # else:
                #     upsert_raw(con, "user_raw", "user_id", user_id, user_cache[user_id])

            con.commit()
            set_status(con, car_id, "DONE")
            done += 1
            print("✅ DONE")

        except Exception as e:
            con.rollback()
            err += 1
            msg = str(e)[:500]
            print(f"❌ ERROR: {msg}")
            # retry_count 증가 + ERROR
            set_status(con, car_id, "ERROR", err=msg, inc_retry=True)

            # 너무 많은 재시도 방지: retry가 너무 많으면 PENDING으로 돌리지 않고 ERROR 유지
            # (재시도는 별도 스크립트/쿼리로 관리)
            continue

    print(f"\n✅ Worker finished. DONE={done}, ERROR={err}")


if __name__ == "__main__":
    main()

