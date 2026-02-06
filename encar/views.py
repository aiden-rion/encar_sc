import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from django.db import connections
from django.http import JsonResponse, HttpRequest, HttpResponse
from django.shortcuts import render

from openpyxl import Workbook


# =========================================================
# 설정
# =========================================================
DB_ALIAS = "encar"

ACCIDENT_CODES = {"X", "W", "C"}

OPTION_CODE_MAP: Dict[str, str] = {
    "10": "선루프",
    "1": "헤드램프(HID, LED)",
    "59": "파워 전동 트렁크",
    "80": "고스트 도어 클로징",
    "24": "전동접이 사이드 미러",
    "17": "알루미늄 휠",
    "62": "루프랙",
    "82": "열선 스티어링 휠",
    "83": "전동 조절 스티어링 휠",
    "84": "패들 시프트",
    "31": "스티어링 휠 리모컨",
    "30": "ECM 룸미러",
    "74": "하이패스",
    "6": "파워 도어록",
    "8": "파워 스티어링 휠",
    "7": "파워 윈도우",

    "2": "에어백(운전석, 동승석)",
    "20": "에어백(사이드)",
    "56": "에어백(커튼)",
    "19": "미끄럼 방지(TCS)",
    "55": "차체자세 제어장치(ESC)",
    "33": "타이어 공기압센서(TPMS)",
    "88": "차선이탈 경보 시스템(LDWS)",
    "86": "후측방 경보 시스템",
    "58": "후방 카메라",
    "87": "360도 어라운드 뷰",

    "4": "크루즈 컨트롤(일반, 어댑티브)",
    "95": "헤드업 디스플레이(HUD)",
    "94": "전자식 주차브레이크(EPB)",
    "23": "자동 에어컨",
    "57": "스마트키",
    "15": "무선도어 잠금장치",
    "81": "레인센서",
    "97": "오토 라이트",
    "96": "블루투스",
    "72": "USB 단자",
    "71": "AUX 단자",

    "14": "가죽시트",
    "89": "전동시트(뒷좌석)",
    "90": "통풍시트(뒷좌석)",
    "91": "마사지 시트",
}


# =========================================================
# 유틸
# =========================================================

def split_keyword_tokens(keyword: str) -> List[str]:
    # 따옴표로 묶인 문장은 하나로 취급하고 싶으면 확장 가능
    tokens = re.split(r"\s+", keyword.strip())
    return [t for t in tokens if t]

def is_numeric_token(t: str) -> bool:
    # 5.0 / 3.3 / 3800 등
    return bool(re.fullmatch(r"\d+(\.\d+)?", t))

def row_matches_tokens(vraw: Dict[str, Any], tokens: List[str]) -> bool:
    """
    2차 정밀 필터:
    - 문자열 토큰은 제조사/모델/트림/세부트림/차량번호에서 AND 매칭
    - 숫자 토큰은 트림/세부트림/모델명 위주로 AND 매칭(옵션/이력에서 매칭 방지)
    """
    maker = str(safe_get(vraw, ["category", "manufacturerName"]) or "").lower()
    model = str(safe_get(vraw, ["category", "modelName"]) or "").lower()
    trim  = str(safe_get(vraw, ["category", "gradeName"]) or "").lower()
    subtrim = str(safe_get(vraw, ["category", "gradeDetailName"]) or "").lower()
    carno = str(safe_get(vraw, ["vehicleNo"]) or "").lower()

    # 넓은 텍스트(문자 토큰은 여기도 허용)
    broad_text = " ".join([maker, model, trim, subtrim, carno])

    # 숫자 토큰은 여기만 보자(잡매칭 방지)
    numeric_text = " ".join([model, trim, subtrim])

    for t in tokens:
        tt = t.lower()
        if is_numeric_token(tt):
            if tt not in numeric_text:
                return False
        else:
            if tt not in broad_text:
                return False
    return True


def yn(v: Any) -> str:
    return "Y" if bool(v) else "N"


def normalize_opt_code(code: Any) -> str:
    s = str(code).strip()
    s2 = s.lstrip("0")
    return s2 if s2 else "0"


def parse_json_maybe(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="ignore")
    if isinstance(v, str):
        t = v.strip()
        if not t:
            return None
        try:
            return json.loads(t)
        except Exception:
            return None
    return None


def safe_get(d: Any, path: List[Any], default=None):
    cur = d
    for p in path:
        if cur is None:
            return default
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
    return default if cur is None else cur


def yyyymmdd_to_iso(s: Optional[str]) -> str:
    if not s:
        return ""
    s = str(s).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s


def to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip().replace(",", "")
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def percentile(sorted_vals: List[int], p: float) -> int:
    if not sorted_vals:
        return 0
    # p: 0~1
    n = len(sorted_vals)
    idx = int(round((n - 1) * p))
    idx = max(0, min(n - 1, idx))
    return sorted_vals[idx]

def _parse_options_choice_payload(payload: Any) -> List[Dict[str, Any]]:
    p = parse_json_maybe(payload)
    if isinstance(p, list):
        return [x for x in p if isinstance(x, dict)]
    if isinstance(p, dict):
        return [p]
    return []


def make_hist(values: List[int], bins: int = 12) -> Dict[str, Any]:
    """
    가격 히스토그램: bins개 구간으로 쪼개서 {labels, counts, min, max}
    """
    values = [v for v in values if v > 0]
    if not values:
        return {"labels": [], "counts": [], "min": 0, "max": 0, "bin_size": 0}

    mn, mx = min(values), max(values)
    if mn == mx:
        return {"labels": [f"{mn:,}"], "counts": [len(values)], "min": mn, "max": mx, "bin_size": 0}

    span = mx - mn
    bin_size = max(1, int(span / bins))
    # bin_size 너무 촘촘하면 보기 별로라 살짝 올림(옵션)
    # bin_size = ((bin_size + 9999) // 10000) * 10000  # 1만원 단위로
    counts = [0] * bins

    for v in values:
        i = int((v - mn) / bin_size)
        if i >= bins:
            i = bins - 1
        counts[i] += 1

    labels = []
    for i in range(bins):
        a = mn + i * bin_size
        b = mn + (i + 1) * bin_size - 1
        labels.append(f"{a:,}~{b:,}")

    return {"labels": labels, "counts": counts, "min": mn, "max": mx, "bin_size": bin_size}



# --------------------------
# LATEST JOIN (빠른 버전)
# --------------------------
LATEST_JOIN_SQL = """
SELECT
  v.car_id,
  v.payload AS vehicle_payload,
  i.payload AS inspection_payload,
  r.payload AS record_payload,
  o.payload AS options_choice_payload
FROM vehicle_raw_latest v
LEFT JOIN inspection_raw_latest i ON i.car_id = v.car_id
LEFT JOIN record_raw_latest r ON r.car_id = v.car_id
LEFT JOIN options_choice_raw_latest o ON o.car_id = v.car_id
"""

def _parse_options_choice_payload(payload: Any) -> List[Dict[str, Any]]:
    p = parse_json_maybe(payload)
    if isinstance(p, list):
        return [x for x in p if isinstance(x, dict)]
    if isinstance(p, dict):
        return [p]
    return []

# --------------------------
# summary API
# --------------------------
def combine_price_analysis_api(request: HttpRequest):
    """
    /encar/api/combine/price-analysis?keyword=K7
    - 연식별, 주행거리별 시세 분석
    - 연식과 주행거리 구간별로 평균 가격을 제공
    """
    try:
        keyword = (request.GET.get("keyword") or "").strip()
        sample = int(request.GET.get("sample", "5000"))  # 0이면 전체
        sample = max(0, sample)

        conn = connections[DB_ALIAS]

        where_sql = ""
        params: List[Any] = []

        if keyword:
            where_sql = " WHERE v.payload LIKE %s"
            params.append(f"%{keyword}%")

        # 집계 대상 select
        sql = LATEST_JOIN_SQL + (where_sql if where_sql else "") + " ORDER BY v.car_id"
        if sample > 0:
            sql += " LIMIT %s"
            params2 = params + [sample]
        else:
            params2 = params

        # 연식별, 주행거리별 데이터 수집
        year_mileage_prices = {}  # {year: {mileage_range: [prices]}}

        # 주행거리 구간 정의 (km)
        mileage_ranges = [
            (0, 30000, "0-3만km"),
            (30000, 60000, "3-6만km"), 
            (60000, 100000, "6-10만km"),
            (100000, 150000, "10-15만km"),
            (150000, 200000, "15-20만km"),
            (200000, float('inf'), "20만km+")
        ]

        with conn.cursor() as cur:
            cur.execute(sql, params2)
            while True:
                rows = cur.fetchmany(800)
                if not rows:
                    break
                for car_id, v_payload, i_payload, r_payload, o_payload in rows:
                    vraw = parse_json_maybe(v_payload) or {}
                    if not isinstance(vraw, dict):
                        continue

                    # 연식, 주행거리, 가격 추출
                    year = safe_get(vraw, ["category", "formYear"]) or safe_get(vraw, ["category", "yearMonth"], "")[:4] or ""
                    price = to_int(safe_get(vraw, ["advertisement", "price"]), 0)
                    mileage = to_int(safe_get(vraw, ["spec", "mileage"]), 0)

                    if not year or price <= 0 or mileage < 0:
                        continue

                    year = str(year).strip()
                    if not year.isdigit() or len(year) != 4:
                        continue

                    # 주행거리 구간 찾기
                    mileage_range_name = None
                    for min_km, max_km, range_name in mileage_ranges:
                        if min_km <= mileage < max_km:
                            mileage_range_name = range_name
                            break

                    if not mileage_range_name:
                        continue

                    # 데이터 저장
                    if year not in year_mileage_prices:
                        year_mileage_prices[year] = {}
                    if mileage_range_name not in year_mileage_prices[year]:
                        year_mileage_prices[year][mileage_range_name] = []

                    year_mileage_prices[year][mileage_range_name].append(price)

        # 결과 정리
        analysis_data = []
        for year in sorted(year_mileage_prices.keys(), reverse=True):  # 최신 연식부터
            year_data = {"year": year, "mileage_ranges": []}

            for min_km, max_km, range_name in mileage_ranges:
                if range_name in year_mileage_prices[year]:
                    prices = year_mileage_prices[year][range_name]
                    if prices:
                        avg_price = int(sum(prices) / len(prices))
                        min_price = min(prices)
                        max_price = max(prices)
                        count = len(prices)

                        year_data["mileage_ranges"].append({
                            "range": range_name,
                            "avg_price": avg_price,
                            "min_price": min_price,
                            "max_price": max_price,
                            "count": count
                        })
                else:
                    year_data["mileage_ranges"].append({
                        "range": range_name,
                        "avg_price": 0,
                        "min_price": 0,
                        "max_price": 0,
                        "count": 0
                    })

            analysis_data.append(year_data)

        return JsonResponse(
            {
                "ok": True,
                "meta": {
                    "keyword": keyword,
                    "sample_size": sample if sample > 0 else "전체",
                },
                "analysis": analysis_data,
                "mileage_ranges": [range_name for _, _, range_name in mileage_ranges]
            },
            json_dumps_params={"ensure_ascii": False},
        )

    except Exception as e:
        import traceback
        return JsonResponse(
            {
                "ok": False,
                "error": str(e),
                "trace": traceback.format_exc(),
            },
            status=500,
            json_dumps_params={"ensure_ascii": False},
        )


def combine_summary_api(request: HttpRequest):
    """
    /encar/api/combine/summary?keyword=K7
    - 현재 검색조건에 대한 요약 (가격 범위/평균/중앙값/분포 등)
    - SQLite가 스크래핑 중이면 전체 스캔이 부담될 수 있으니, sample 파라미터 지원
      예) sample=5000 (기본 5000) => 처음 N개만 집계(빠름)
    """
    try:
        keyword = (request.GET.get("keyword") or "").strip()
        sample = int(request.GET.get("sample", "5000"))  # 0이면 전체
        sample = max(0, sample)

        conn = connections[DB_ALIAS]

        where_sql = ""
        params: List[Any] = []

        # ⚠️ payload LIKE는 인덱스가 안타서 비용 있음. 그래도 latest만이라 훨씬 낫고,
        # sample로 현실 타협 가능.
        if keyword:
            where_sql = " WHERE v.payload LIKE %s"
            params.append(f"%{keyword}%")

        # total (정확한 전체 매물 수)
        total = None
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM vehicle_raw_latest v" + (where_sql if where_sql else ""),
                params,
            )
            total = int(cur.fetchone()[0])

        # 집계 대상 select
        sql = LATEST_JOIN_SQL + (where_sql if where_sql else "") + " ORDER BY v.car_id"
        if sample > 0:
            sql += " LIMIT %s"
            params2 = params + [sample]
        else:
            params2 = params

        prices: List[int] = []
        mileages: List[int] = []
        accident_y = 0
        simple_y = 0
        n = 0

        # 스트리밍 집계 (메모리 절약)
        with conn.cursor() as cur:
            cur.execute(sql, params2)
            while True:
                rows = cur.fetchmany(800)
                if not rows:
                    break
                for car_id, v_payload, i_payload, r_payload, o_payload in rows:
                    vraw = parse_json_maybe(v_payload) or {}
                    if not isinstance(vraw, dict):
                        continue

                    iraw = parse_json_maybe(i_payload) if i_payload else None
                    iraw = iraw if isinstance(iraw, dict) else None

                    # 가격/주행거리 (네 payload 구조 기준)
                    price = to_int(safe_get(vraw, ["advertisement", "price"]), 0)
                    mileage = to_int(safe_get(vraw, ["spec", "mileage"]), 0)

                    if price > 0:
                        prices.append(price)
                    if mileage > 0:
                        mileages.append(mileage)

                    if iraw:
                        if bool(safe_get(iraw, ["master", "accdient"])):  # 원본 오타 accdient
                            accident_y += 1
                        if bool(safe_get(iraw, ["master", "simpleRepair"])):
                            simple_y += 1

                    n += 1

        # 통계 계산
        prices_sorted = sorted(prices)
        mile_sorted = sorted(mileages)

        price_min = min(prices_sorted) if prices_sorted else 0
        price_max = max(prices_sorted) if prices_sorted else 0
        price_avg = int(sum(prices_sorted) / len(prices_sorted)) if prices_sorted else 0
        price_med = percentile(prices_sorted, 0.5) if prices_sorted else 0

        mile_avg = int(sum(mile_sorted) / len(mile_sorted)) if mile_sorted else 0
        mile_med = percentile(mile_sorted, 0.5) if mile_sorted else 0

        hist = make_hist(prices_sorted, bins=12)

        return JsonResponse(
            {
                "ok": True,
                "meta": {
                    "keyword": keyword,
                    "total": total,              # 전체 매물 수(정확)
                    "sampled": (sample > 0),     # 샘플 집계 여부
                    "sample_size": sample if sample > 0 else total,
                    "count_used": n,             # 실제 집계에 사용된 row 수
                },
                "price": {
                    "min": price_min,
                    "max": price_max,
                    "avg": price_avg,
                    "median": price_med,
                    "hist": hist,  # labels, counts
                },
                "mileage": {
                    "avg": mile_avg,
                    "median": mile_med,
                },
                "rates": {
                    "accident_rate": round((accident_y / n) * 100, 2) if n else 0,
                    "simple_repair_rate": round((simple_y / n) * 100, 2) if n else 0,
                },
            },
            json_dumps_params={"ensure_ascii": False},
        )

    except Exception as e:
        import traceback
        return JsonResponse(
            {
                "ok": False,
                "error": str(e),
                "trace": traceback.format_exc(),
            },
            status=500,
            json_dumps_params={"ensure_ascii": False},
        )


# =========================================================
# 사고/보험/옵션 요약
# =========================================================
def accident_easy_summary(inspection_raw: Optional[Dict[str, Any]]) -> str:
    if not inspection_raw:
        return ""
    outers = inspection_raw.get("outers") or []
    if not outers:
        return "무사고"

    items: List[str] = []
    for o in outers:
        part = safe_get(o, ["type", "title"]) or safe_get(o, ["type", "code"]) or ""
        sts = o.get("statusTypes") or []
        for s in sts:
            cd = str(s.get("code") or "").strip()
            title = str(s.get("title") or "").strip()
            if cd in ACCIDENT_CODES:
                items.append(f"{part}-{title or cd}")
                break

    return "무사고" if not items else " / ".join(items)


def insurance_summary(record_raw: Optional[Dict[str, Any]]) -> str:
    if not record_raw:
        return ""

    r = record_raw
    if isinstance(record_raw, dict) and isinstance(record_raw.get("record"), dict):
        r = record_raw["record"]

    def pick(*keys):
        for k in keys:
            if isinstance(r, dict) and k in r and r[k] is not None:
                return r[k]
        return None

    acc_cnt = pick("accidentCnt", "acc_cnt", "accCnt", "totalAccidentCnt")
    my_cnt = pick("myAccidentCnt", "my_cnt", "myAccCnt")
    other_cnt = pick("otherAccidentCnt", "other_cnt", "otherAccCnt")
    my_cost = pick("myAccidentCost", "my_cost", "myAccCost")
    other_cost = pick("otherAccidentCost", "other_cost", "otherAccCost")

    parts = []
    try:
        if acc_cnt is not None:
            parts.append(f"보험이력 {int(acc_cnt)}건")
        if my_cnt is not None:
            parts.append(f"내차 {int(my_cnt)}건")
        if my_cost is not None:
            parts.append(f"내차 {int(my_cost):,}원")
        if other_cnt is not None:
            parts.append(f"타차 {int(other_cnt)}건")
        if other_cost is not None:
            parts.append(f"타차 {int(other_cost):,}원")
    except Exception:
        return str(record_raw)[:200]

    return " / ".join(parts)


def standard_options_kr(vehicle_raw: Dict[str, Any]) -> str:
    std_codes = safe_get(vehicle_raw, ["options", "standard"], default=[]) or []
    names: List[str] = []
    for c in std_codes:
        k = normalize_opt_code(c)
        nm = OPTION_CODE_MAP.get(k)
        names.append(nm or f"({c})")
    return ", ".join(names)


def paid_options_kr_and_sum(
    vehicle_raw: Dict[str, Any],
    options_choice_list: List[Dict[str, Any]]
) -> Tuple[str, str, int]:
    choice_codes = safe_get(vehicle_raw, ["options", "choice"], default=[]) or []
    choice_codes = [str(x).strip() for x in choice_codes if str(x).strip()]

    # optionCd -> {name, price}
    m: Dict[str, Dict[str, Any]] = {}
    for it in options_choice_list or []:
        cd = str(it.get("optionCd") or "").strip()
        if not cd:
            continue
        m[cd] = {"name": it.get("optionName"), "price": it.get("price")}

    all_names: List[str] = []
    paid_names: List[str] = []
    paid_sum = 0

    for cd in choice_codes:
        info = m.get(cd)
        if not info:
            all_names.append(f"({cd})")
            continue

        nm = info.get("name") or f"({cd})"
        all_names.append(nm)

        price_int = to_int(info.get("price"), 0)
        if price_int > 0:
            paid_names.append(nm)
            paid_sum += price_int

    return ", ".join(all_names), ", ".join(paid_names), paid_sum


# =========================================================
# SQL (latest 테이블 기반: 초고속)
# =========================================================
JOIN_LATEST_BASE = """
SELECT
  v.car_id,
  v.payload AS vehicle_payload,
  i.payload AS inspection_payload,
  r.payload AS record_payload,
  o.payload AS options_choice_payload
FROM vehicle_raw_latest v
LEFT JOIN inspection_raw_latest i ON i.car_id = v.car_id
LEFT JOIN record_raw_latest r     ON r.car_id = v.car_id
LEFT JOIN options_choice_raw_latest o ON o.car_id = v.car_id
"""


def build_combined_row(
    vehicle_raw: Dict[str, Any],
    inspection_raw: Optional[Dict[str, Any]],
    record_raw: Optional[Dict[str, Any]],
    options_choice_list: List[Dict[str, Any]],
) -> Dict[str, Any]:
    carid = safe_get(vehicle_raw, ["vehicleId"]) or safe_get(vehicle_raw, ["manage", "dummyVehicleId"]) or ""

    car_no = safe_get(vehicle_raw, ["vehicleNo"]) or ""
    maker = safe_get(vehicle_raw, ["category", "manufacturerName"]) or ""
    model_detail = safe_get(vehicle_raw, ["category", "modelName"]) or ""
    trim = safe_get(vehicle_raw, ["category", "gradeName"]) or ""
    sub_trim = safe_get(vehicle_raw, ["category", "gradeDetailName"]) or ""

    model_year = (
        safe_get(vehicle_raw, ["category", "formYear"])
        or (safe_get(vehicle_raw, ["category", "yearMonth"], "")[:4] or "")
        or ""
    )

    fuel = safe_get(vehicle_raw, ["spec", "fuelName"]) or ""
    body_type = safe_get(vehicle_raw, ["spec", "bodyName"]) or ""
    mileage_km = safe_get(vehicle_raw, ["spec", "mileage"]) or ""
    color = safe_get(vehicle_raw, ["spec", "colorName"]) or ""
    price = safe_get(vehicle_raw, ["advertisement", "price"]) or ""

    first_reg = ""
    if inspection_raw:
        first_reg = yyyymmdd_to_iso(safe_get(inspection_raw, ["master", "detail", "firstRegistrationDate"]))

    accident_yn = yn(inspection_raw and safe_get(inspection_raw, ["master", "accdient"]))
    simple_repair_yn = yn(inspection_raw and safe_get(inspection_raw, ["master", "simpleRepair"]))

    accident_easy = accident_easy_summary(inspection_raw)
    insurance = insurance_summary(record_raw)

    opt_std_kr = standard_options_kr(vehicle_raw)
    _, opt_paid_kr, opt_paid_sum = paid_options_kr_and_sum(vehicle_raw, options_choice_list)

    # ✅ UI에서 바로 쓰는 키로 내려준다
    return {
        "carid": carid,
        "차량번호": car_no,
        "색상": color,
        "제조사": maker,
        "세부모델": model_detail,
        "트림": trim,
        "세부트림": sub_trim,
        "연식": str(model_year).strip() if model_year is not None else "",
        "최초등록일": first_reg,
        "유종": fuel,
        "차형": body_type,
        "주행거리": mileage_km,
        "판매가": price,
        "단순수리 Y/N": simple_repair_yn,
        "사고여부 Y/N": accident_yn,

        "사고이력": accident_easy,
        "보험이력": insurance,
        "옵션": opt_std_kr,
        "유상옵션": opt_paid_kr,
        "옵션 합계금액": opt_paid_sum,
    }


# =========================================================
# Views
# =========================================================
def combine_page(request: HttpRequest):
    return render(request, "encar/combine.html")


def combine_list_api(request: HttpRequest):
    """
    /encar/api/combine/list?keyword=G90 5.0&page=1&size=100&withTotal=1
    - page/size 지원(프론트 Tabulator remote pagination 대응)
    - keyword 토큰 AND 검색 + 2차 정밀필터로 잡매칭 감소
    """
    try:
        keyword = (request.GET.get("keyword") or "").strip()
        with_total = (request.GET.get("withTotal") or "0") == "1"

        # ✅ page/size 우선 지원 (없으면 limit/offset fallback)
        page = request.GET.get("page")
        size = request.GET.get("size")

        if page is not None or size is not None:
            page = int(page or "1")
            size = int(size or "100")
            page = max(1, page)
            size = max(1, min(size, 500))
            limit = size
            offset = (page - 1) * size
        else:
            limit = int(request.GET.get("limit", "100"))
            offset = int(request.GET.get("offset", "0"))
            limit = max(1, min(limit, 500))
            offset = max(0, offset)
            page = (offset // limit) + 1
            size = limit

        tokens = split_keyword_tokens(keyword) if keyword else []

        where_sql = ""
        params: List[Any] = []

        # ✅ 1차 후보군: payload LIKE를 토큰 AND로
        # (tokens가 2개 이상이면 정확도 확 올라감)
        if tokens:
            conds = []
            for t in tokens:
                conds.append("v.payload LIKE %s")
                params.append(f"%{t}%")
            where_sql = " WHERE " + " AND ".join(conds)

        # ✅ SQL 실행
        sql = JOIN_LATEST_BASE + where_sql + " ORDER BY v.car_id LIMIT %s OFFSET %s"
        params_sql = params + [limit, offset]

        conn = connections[DB_ALIAS]
        rows: List[Dict[str, Any]] = []

        with conn.cursor() as cur:
            cur.execute(sql, params_sql)
            fetched = cur.fetchall()

            for car_id, v_payload, i_payload, r_payload, o_payload in fetched:
                vraw = parse_json_maybe(v_payload) or {}
                if not isinstance(vraw, dict):
                    continue

                # ✅ 2차 정밀 필터: 숫자 토큰 잡매칭 방지
                if tokens and (not row_matches_tokens(vraw, tokens)):
                    continue

                iraw = parse_json_maybe(i_payload) if i_payload else None
                iraw = iraw if isinstance(iraw, dict) else None

                rraw = parse_json_maybe(r_payload) if r_payload else None
                rraw = rraw if isinstance(rraw, dict) else None

                olist = _parse_options_choice_payload(o_payload)

                row = build_combined_row(vraw, iraw, rraw, olist)
                if not row.get("carid"):
                    row["carid"] = str(car_id)

                rows.append(row)

        total = None
        last_page = None
        if with_total:
            # total은 1차(where_sql) 기준으로 세되, 2차 필터가 있으면 정확 total은 어려움.
            # 현실적으로는 1차 total을 쓰고, 2차 필터는 "표본 정확도"로 보자.
            cnt_sql = "SELECT COUNT(*) FROM vehicle_raw_latest v" + (where_sql if tokens else "")
            with conn.cursor() as cur:
                cur.execute(cnt_sql, params)
                total = int(cur.fetchone()[0])

            last_page = max(1, (total + size - 1) // size) if total is not None else None

        return JsonResponse(
            {
                "ok": True,
                "meta": {
                    "db_alias": DB_ALIAS,
                    "count": len(rows),
                    "page": page,
                    "size": size,
                    "limit": limit,
                    "offset": offset,
                    "keyword": keyword,
                    "tokens": tokens,
                    "total": total,
                    "last_page": last_page,
                },
                "rows": rows,
            },
            json_dumps_params={"ensure_ascii": False},
        )

    except Exception as e:
        import traceback
        return JsonResponse(
            {
                "ok": False,
                "error": str(e),
                "trace": traceback.format_exc(),
                "db_alias": DB_ALIAS,
            },
            status=500,
            json_dumps_params={"ensure_ascii": False},
        )


def combine_export_xlsx(request: HttpRequest):
    """
    /encar/api/combine/export.xlsx?keyword=... (선택)
    - latest 기반 + fetchmany로 메모리 안정
    """
    try:
        keyword = (request.GET.get("keyword") or "").strip()

        headers = [
            "carid",
            "차량번호",
            "색상",
            "제조사",
            "세부모델",
            "트림",
            "세부트림",
            "연식",
            "최초등록일",
            "유종",
            "차형",
            "주행거리",
            "판매가",
            "단순수리 Y/N",
            "사고여부 Y/N",
            "사고이력",
            "보험이력",
            "옵션",
            "유상옵션",
            "옵션 합계금액",
        ]

        where_sql = ""
        params: List[Any] = []
        if keyword:
            where_sql = " WHERE v.payload LIKE %s "
            params.append(f"%{keyword}%")

        sql = JOIN_LATEST_BASE + where_sql + " ORDER BY v.car_id"

        wb = Workbook(write_only=True)
        ws = wb.create_sheet(title="Encar Combine")
        ws.append(headers)

        conn = connections[DB_ALIAS]
        with conn.cursor() as cur:
            cur.execute(sql, params)
            while True:
                batch = cur.fetchmany(500)
                if not batch:
                    break

                for car_id, v_payload, i_payload, r_payload, o_payload in batch:
                    vraw = parse_json_maybe(v_payload) or {}
                    if not isinstance(vraw, dict):
                        vraw = {"_payload": vraw}

                    iraw = parse_json_maybe(i_payload) if i_payload else None
                    iraw = iraw if isinstance(iraw, dict) else None

                    rraw = parse_json_maybe(r_payload) if r_payload else None
                    rraw = rraw if isinstance(rraw, dict) else None

                    olist = _parse_options_choice_payload(o_payload)

                    row = build_combined_row(vraw, iraw, rraw, olist)
                    if not row.get("carid"):
                        row["carid"] = str(car_id)

                    ws.append([row.get(h, "") for h in headers])

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"encar_combine_{ts}.xlsx"

        resp = HttpResponse(
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        resp["Content-Disposition"] = f'attachment; filename="{filename}"'
        wb.save(resp)
        return resp

    except Exception as e:
        import traceback
        return JsonResponse(
            {
                "ok": False,
                "error": str(e),
                "trace": traceback.format_exc(),
                "db_alias": DB_ALIAS,
            },
            status=500,
            json_dumps_params={"ensure_ascii": False},
        )


def debug_table_api(request: HttpRequest):
    table = (request.GET.get("table") or "").strip()
    if not table:
        return JsonResponse({"ok": False, "error": "table is required"}, status=400)

    conn = connections[DB_ALIAS]
    try:
        with conn.cursor() as cur:
            cur.execute(f"PRAGMA table_info({table})")
            cols = [r[1] for r in cur.fetchall()]

            cur.execute(f"SELECT * FROM {table} ORDER BY fetched_at DESC LIMIT 3")
            rows = cur.fetchall()

            sample = []
            for r in rows:
                d = {}
                for i, c in enumerate(cols):
                    d[c] = r[i] if i < len(r) else None
                sample.append(d)

        return JsonResponse(
            {"ok": True, "table": table, "columns": cols, "sample": sample},
            json_dumps_params={"ensure_ascii": False},
        )

    except Exception as e:
        import traceback
        return JsonResponse(
            {
                "ok": False,
                "error": str(e),
                "trace": traceback.format_exc(),
                "db_alias": DB_ALIAS,
            },
            status=500,
            json_dumps_params={"ensure_ascii": False},
        )
