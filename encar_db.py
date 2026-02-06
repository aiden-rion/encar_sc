# encar_db.py
import sqlite3
from pathlib import Path

DB_PATH = Path("encar_dump.db")

DDL = """
CREATE TABLE IF NOT EXISTS car_queue (
  car_id TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'PENDING',   -- PENDING | RUNNING | DONE | ERROR
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  updated_at TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS ix_car_queue_status ON car_queue(status);

CREATE TABLE IF NOT EXISTS vehicle_raw (
  car_id TEXT PRIMARY KEY,
  payload TEXT,
  fetched_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS inspection_raw (
  car_id TEXT PRIMARY KEY,
  payload TEXT,
  fetched_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS record_raw (
  car_id TEXT PRIMARY KEY,
  vehicle_no TEXT,
  payload TEXT,
  fetched_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS options_choice_raw (
  car_id TEXT PRIMARY KEY,
  payload TEXT,
  fetched_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS user_raw (
  user_id TEXT PRIMARY KEY,
  payload TEXT,
  fetched_at TEXT DEFAULT (datetime('now'))
);
"""

PRAGMAS = [
    "PRAGMA journal_mode=WAL;",
    "PRAGMA synchronous=NORMAL;",
    "PRAGMA temp_store=MEMORY;",
    "PRAGMA cache_size=-200000;",  # ~200MB (환경에 맞게 조절)
    "PRAGMA busy_timeout=5000;",
]


def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    for p in PRAGMAS:
        con.execute(p)
    return con


def init_db(con: sqlite3.Connection) -> None:
    con.executescript(DDL)
    con.commit()


def seed_one(con: sqlite3.Connection, car_id: str) -> None:
    con.execute(
        "INSERT OR IGNORE INTO car_queue(car_id, status) VALUES(?, 'PENDING')",
        (car_id,),
    )
    con.commit()


def set_status(con: sqlite3.Connection, car_id: str, status: str, err: str | None = None, inc_retry: bool = False) -> None:
    if inc_retry:
        con.execute(
            """
            UPDATE car_queue
            SET status=?, last_error=?, retry_count=retry_count+1, updated_at=datetime('now')
            WHERE car_id=?
            """,
            (status, err, car_id),
        )
    else:
        con.execute(
            """
            UPDATE car_queue
            SET status=?, last_error=?, updated_at=datetime('now')
            WHERE car_id=?
            """,
            (status, err, car_id),
        )
    con.commit()


def main():
    con = connect()
    init_db(con)

    # ✅ 간단 테스트
    test_car_id = "TEST_CAR_123"
    seed_one(con, test_car_id)
    set_status(con, test_car_id, "RUNNING")
    set_status(con, test_car_id, "DONE")

    row = con.execute("SELECT * FROM car_queue WHERE car_id=?", (test_car_id,)).fetchone()
    print(dict(row))
    print(f"✅ DB OK: {DB_PATH.resolve()}")


if __name__ == "__main__":
    main()

