# encar_seed_queue.py
import time
import random
from typing import Any, Dict, List, Optional, Set, Tuple

import requests
from encar_db import connect, init_db

API_URL = "https://api.encar.com/search/car/list/pricesupply"

MAKERS: Dict[str, str] = {
    "hyundai": "현대",
    "kia": "기아",
    "genesis": "제네시스",
    "chevrolet": "쉐보레(GM대우)",
    "renault": "르노코리아(삼성)",
    "kgm": "KG모빌리티(쌍용)",
}

SORT_FIELD = "PriceAsc"
LIMIT = 500

SLEEP_BETWEEN_CALLS_SEC = (0.2, 0.55)
TIMEOUT_SEC = 20
MAX_RETRIES = 5

EXISTING_TABLE = "car_queue"
EXISTING_ID_COL = "car_id"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://www.encar.com/",
    "Origin": "https://www.encar.com",
}


def sleepy():
    time.sleep(random.uniform(*SLEEP_BETWEEN_CALLS_SEC))


def safe_get(d: Any, path: List[Any], default=None):
    cur = d
    for p in path:
        try:
            if isinstance(p, int):
                cur = cur[p]
            else:
                cur = cur.get(p)
        except Exception:
            return default
        if cur is None:
            return default
    return cur


def extract_items(j: Dict[str, Any]) -> List[Dict[str, Any]]:
    paths = [
        ["SearchResults"],
        ["searchResults"],
        ["items"],
        ["cars"],
        ["Data", "SearchResults"],
        ["data", "SearchResults"],
        ["data", "items"],
    ]
    for p in paths:
        v = safe_get(j, p)
        if isinstance(v, list):
            return v
    return []


def pick_car_id(item: Dict[str, Any]) -> Optional[str]:
    for k in ("Id", "id", "CarId", "carId"):
        if k in item and item[k] is not None:
            return str(item[k])
    v = safe_get(item, ["Vehicle", "Id"]) or safe_get(item, ["Vehicle", "id"])
    return str(v) if v is not None else None


def get_total_count(j: Dict[str, Any]) -> Optional[int]:
    for p in (
        ["common", "totalCount"],
        ["Common", "TotalCount"],
        ["totalCount"],
        ["TotalCount"],
        ["count"],
        ["Count"],
    ):
        v = safe_get(j, p)
        if isinstance(v, int):
            return v
    return None


def table_exists(con, name: str) -> bool:
    r = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return r is not None


def preload_existing_queue_ids(con) -> Set[str]:
    if not table_exists(con, EXISTING_TABLE):
        return set()
    rows = con.execute(f"SELECT {EXISTING_ID_COL} FROM {EXISTING_TABLE}").fetchall()
    return {str(r[0]) for r in rows if r and r[0] is not None}


def build_q(maker_kr: str) -> str:
    return (
        "(And.Hidden.N._.ServiceCopyCar.Original._."
        f"(C.CarType.A._.Manufacturer.{maker_kr}.)_.Mileage.range(0..200000).)"
    )


def build_sr(offset: int, limit: int) -> str:
    return f"|{SORT_FIELD}|{offset}|{limit}"


def chunk_list(xs: List[str], n: int) -> List[List[str]]:
    return [xs[i:i+n] for i in range(0, len(xs), n)]


# -------------------------
# DB
# -------------------------
def ensure_tables(con) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS car_snapshot_today (
        car_id TEXT PRIMARY KEY,
        seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """)
    con.execute("""
    CREATE TABLE IF NOT EXISTS car_state (
        car_id TEXT PRIMARY KEY,
        first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_seen_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        status        TEXT NOT NULL DEFAULT 'ACTIVE',
        last_change_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_car_state_status ON car_state(status)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_car_state_last_seen ON car_state(last_seen_at)")
    con.commit()


def reset_today_snapshot(con) -> None:
    con.execute("DELETE FROM car_snapshot_today")
    con.commit()


def select_existing_states(con, ids: List[str]) -> Dict[str, str]:
    if not ids:
        return {}
    result: Dict[str, str] = {}
    for part in chunk_list(ids, 900):
        placeholders = ",".join(["?"] * len(part))
        rows = con.execute(
            f"SELECT car_id, status FROM car_state WHERE car_id IN ({placeholders})",
            part,
        ).fetchall()
        for r in rows:
            result[str(r[0])] = str(r[1])
    return result


def filter_out_existing_raw(con, ids: List[str]) -> List[str]:
    """
    vehicle_raw에 이미 있는 car_id는 제외
    """
    if not ids:
        return []
    keep: List[str] = []
    for part in chunk_list(ids, 900):
        placeholders = ",".join(["?"] * len(part))
        rows = con.execute(
            f"SELECT car_id FROM vehicle_raw WHERE car_id IN ({placeholders})",
            part
        ).fetchall()
        exists = {str(r[0]) for r in rows}
        keep.extend([cid for cid in part if cid not in exists])
    return keep


def upsert_snapshot_today(con, ids: List[str]) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO car_snapshot_today(car_id) VALUES(?)",
        [(cid,) for cid in ids],
    )


def insert_new_states(con, new_ids: List[str]) -> None:
    con.executemany(
        "INSERT OR IGNORE INTO car_state(car_id, status) VALUES(?, 'ACTIVE')",
        [(cid,) for cid in new_ids],
    )


def touch_active_states(con, ids: List[str]) -> None:
    if not ids:
        return
    for part in chunk_list(ids, 900):
        placeholders = ",".join(["?"] * len(part))
        con.execute(
            f"""
            UPDATE car_state
               SET last_seen_at = CURRENT_TIMESTAMP,
                   status = 'ACTIVE',
                   last_change_at = CASE WHEN status != 'ACTIVE' THEN CURRENT_TIMESTAMP ELSE last_change_at END
             WHERE car_id IN ({placeholders})
            """,
            part
        )


def enqueue_ids(con, queue_existing_ids: Set[str], ids: List[str]) -> int:
    """
    ✅ 큐에 없는 것만 추가 (기존 큐 상태 절대 건드리지 않음)
    """
    if not ids:
        return 0

    new_to_queue = [cid for cid in ids if cid not in queue_existing_ids]
    con.executemany(
        "INSERT OR IGNORE INTO car_queue(car_id, status) VALUES(?, 'PENDING')",
        [(cid,) for cid in new_to_queue],
    )

    for cid in new_to_queue:
        queue_existing_ids.add(cid)

    return len(new_to_queue)


def finalize_inactive(con) -> int:
    before = con.execute("SELECT COUNT(*) FROM car_state WHERE status='INACTIVE'").fetchone()[0]
    con.execute("""
        UPDATE car_state
           SET status='INACTIVE',
               last_change_at=CURRENT_TIMESTAMP
         WHERE status='ACTIVE'
           AND car_id NOT IN (SELECT car_id FROM car_snapshot_today)
    """)
    after = con.execute("SELECT COUNT(*) FROM car_state WHERE status='INACTIVE'").fetchone()[0]
    return int(after - before)


# -------------------------
# HTTP
# -------------------------
class EncarClient:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update(DEFAULT_HEADERS)
        self.s.trust_env = False   # ✅ 환경변수 프록시/인증 무시


    def get_json(self, params: Dict[str, str]) -> Dict[str, Any]:
        last_err = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.s.get(API_URL, params=params, timeout=TIMEOUT_SEC)
                if r.status_code == 200:
                    return r.json()

                if r.status_code == 400:
                    raise RuntimeError(f"HTTP 400 Bad Request: {r.url}")

                if r.status_code in (429, 403, 502, 503, 504):
                    last_err = r.text
                    time.sleep(min(30, 2 ** attempt) + random.uniform(0, 1.5))
                    continue

                r.raise_for_status()
            except Exception as e:
                last_err = e
                time.sleep(min(30, 2 ** attempt) + random.uniform(0, 1.5))

        raise RuntimeError(f"GET failed: {last_err}")


# -------------------------
# main
# -------------------------
def seed_one_maker(
    con,
    client: EncarClient,
    queue_existing_ids: Set[str],
    maker_key: str,
    maker_kr: str,
) -> Tuple[int, int, int]:
    print("\n==============================")
    print(f"🚗 MAKER: {maker_key} ({maker_kr})")
    print("==============================")

    first_params = {"count": "true", "q": build_q(maker_kr), "sr": build_sr(0, LIMIT)}
    j0 = client.get_json(first_params)
    total = get_total_count(j0)
    if total is None:
        raise RuntimeError("totalCount not found (count=true)")

    print(f"✅ totalCount={total} limit={LIMIT}")

    offset = 0
    new_state_total = 0
    reappear_total = 0
    queued_new_total = 0

    while offset < total:
        params = {"count": "true", "q": build_q(maker_kr), "sr": build_sr(offset, LIMIT)}
        data = j0 if offset == 0 else client.get_json(params)

        items = extract_items(data)

        page_ids: List[str] = []
        seen_page: Set[str] = set()
        for it in items:
            if isinstance(it, dict):
                cid = pick_car_id(it)
                if cid and cid not in seen_page:
                    seen_page.add(cid)
                    page_ids.append(cid)

        with con:
            upsert_snapshot_today(con, page_ids)

            existing_map = select_existing_states(con, page_ids)

            # 신규(상태에 없던 것)
            new_ids = [cid for cid in page_ids if cid not in existing_map]
            # 재등장(INACTIVE -> ACTIVE)
            reappear_ids = [cid for cid, st in existing_map.items() if st != "ACTIVE" and cid in seen_page]

            # ✅ raw에 이미 있으면 신규/재등장 후보에서 제거
            new_ids = filter_out_existing_raw(con, new_ids)
            reappear_ids = filter_out_existing_raw(con, reappear_ids)

            if new_ids:
                insert_new_states(con, new_ids)

            touch_active_states(con, page_ids)

            # ✅ 큐에는 신규 + 재등장만 "추가" (기존 큐 상태 변경 X)
            queued_new_total += enqueue_ids(con, queue_existing_ids, new_ids + reappear_ids)

            new_state_total += len(new_ids)
            reappear_total += len(reappear_ids)

        print(
            f"offset={offset}/{total} "
            f"page_ids={len(page_ids)} "
            f"new_state+={len(new_ids)} reappear+={len(reappear_ids)} "
            f"queued_new_total~={queued_new_total}"
        )

        offset += LIMIT
        sleepy()

    print(
        f"🏁 DONE maker={maker_key} "
        f"new_state={new_state_total} reappear={reappear_total} queued_new~={queued_new_total}"
    )
    return new_state_total, reappear_total, queued_new_total


def run_all_makers():
    con = connect()
    init_db(con)

    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass

    ensure_tables(con)
    reset_today_snapshot(con)

    client = EncarClient()
    queue_existing_ids = preload_existing_queue_ids(con)

    print("✅ Start FULL crawl (OFFSET) + snapshot/state + inactive detection")
    print(f"   makers={list(MAKERS.keys())}")
    print(f"   limit={LIMIT}")
    print(f"   existing_queue_skip={EXISTING_TABLE}.{EXISTING_ID_COL} preloaded={len(queue_existing_ids)}")

    total_new = 0
    total_reappear = 0
    total_queued_new = 0

    for maker_key, maker_kr in MAKERS.items():
        n, r, q = seed_one_maker(con, client, queue_existing_ids, maker_key, maker_kr)
        total_new += n
        total_reappear += r
        total_queued_new += q

    with con:
        became_inactive = finalize_inactive(con)

    active_cnt = con.execute("SELECT COUNT(*) FROM car_state WHERE status='ACTIVE'").fetchone()[0]
    inactive_cnt = con.execute("SELECT COUNT(*) FROM car_state WHERE status='INACTIVE'").fetchone()[0]
    snapshot_cnt = con.execute("SELECT COUNT(*) FROM car_snapshot_today").fetchone()[0]

    print("\n✅ ALL DONE")
    print(f"   new_state_total={total_new}")
    print(f"   reappear_total={total_reappear}")
    print(f"   queued_new_total~={total_queued_new}")
    print(f"   snapshot_today_count={snapshot_cnt}")
    print(f"   became_inactive_this_run~={became_inactive}")
    print(f"   car_state ACTIVE={active_cnt} INACTIVE={inactive_cnt}")


if __name__ == "__main__":
    run_all_makers()
