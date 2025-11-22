# storage.py (gtrans 컬럼 포함 + 자동 마이그레이션)
import sqlite3
import json
from pathlib import Path

DB_PATH = Path("ews.db")

def _ensure_column(cur, table: str, col: str, col_type: str = "TEXT"):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if col not in cols:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ews_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pub TEXT,
        title TEXT,
        link TEXT,
        countries TEXT,
        summary TEXT,
        gtrans TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS risk_events(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pub TEXT,
        title TEXT,
        link TEXT,
        key TEXT,
        score REAL,
        band TEXT,
        signals TEXT,
        gtrans TEXT
    )
    """)

    # 기존 DB 마이그레이션(컬럼 없으면 자동 추가)
    _ensure_column(cur, "ews_events", "gtrans", "TEXT")
    _ensure_column(cur, "risk_events", "gtrans", "TEXT")

    # 링크 인덱스(중복 체크/조회 속도 개선)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ews_link ON ews_events(link)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_link ON risk_events(link)")

    con.commit()
    con.close()

def _exists(cur, table: str, link: str) -> bool:
    if not link:
        return False
    cur.execute(f"SELECT 1 FROM {table} WHERE link=? LIMIT 1", (link,))
    return cur.fetchone() is not None

def insert_ews(e: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    link = e.get("link", "")
    if _exists(cur, "ews_events", link):
        con.close()
        return False

    countries_list = e.get("countries", []) or []
    countries_json = json.dumps(countries_list, ensure_ascii=False)

    cur.execute(
        "INSERT INTO ews_events(pub,title,link,countries,summary,gtrans) VALUES(?,?,?,?,?,?)",
        (
            e.get("pub",""),
            e.get("title",""),
            link,
            countries_json,
            e.get("summary",""),
            e.get("gtrans",""),
        )
    )
    con.commit()
    con.close()
    return True

def insert_risk(r: dict):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    link = r.get("link", "")
    if _exists(cur, "risk_events", link):
        con.close()
        return False

    cur.execute(
        "INSERT INTO risk_events(pub,title,link,key,score,band,signals,gtrans) VALUES(?,?,?,?,?,?,?,?)",
        (
            r.get("pub",""),
            r.get("title",""),
            link,
            r.get("key","미상"),
            float(r.get("score",0.0)),
            r.get("band","낮음"),
            r.get("signals","{}"),
            r.get("gtrans",""),
        )
    )
    con.commit()
    con.close()
    return True
