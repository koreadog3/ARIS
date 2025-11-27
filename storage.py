# storage.py
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
import json

DB_PATH = Path("ews.db")
KST = timezone(timedelta(hours=9))


def _now_iso():
    return datetime.now(KST).isoformat()


def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    # EWS: 주한 외국공관 철수/대피/폐쇄 이벤트
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ews_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pub TEXT,
        title TEXT,
        link TEXT UNIQUE,
        countries TEXT,
        summary TEXT,
        created_at TEXT
    )
    """)

    # RISK: 군사/외교/민간/경제 위험도 이벤트
    cur.execute("""
    CREATE TABLE IF NOT EXISTS risk_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        pub TEXT,
        title TEXT,
        link TEXT UNIQUE,
        key TEXT,
        score REAL,
        band TEXT,
        signals TEXT,
        created_at TEXT
    )
    """)

    con.commit()
    con.close()


def insert_ews(event: dict) -> bool:
    """
    EWS 이벤트 한 건을 삽입.
    성공적으로 새 행이 들어갔으면 True, 아니면 False 반환.
    """
    if not event:
        return False

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        countries = event.get("countries", ["미상"])
        if isinstance(countries, (list, dict)):
            countries = json.dumps(countries, ensure_ascii=False)

        cur.execute(
            """
            INSERT OR IGNORE INTO ews_events
            (pub, title, link, countries, summary, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                event.get("pub"),
                event.get("title"),
                event.get("link"),
                countries,
                event.get("summary", ""),
                _now_iso(),
            ),
        )
        con.commit()
        return cur.rowcount > 0
    except Exception:
        # 필요하면 여기서 print로 로깅해도 됨
        return False
    finally:
        con.close()


def insert_risk(event: dict) -> bool:
    """
    RISK 이벤트 한 건을 삽입.
    성공적으로 새 행이 들어갔으면 True, 아니면 False 반환.
    """
    if not event:
        return False

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    try:
        cur.execute(
            """
            INSERT OR IGNORE INTO risk_events
            (pub, title, link, key, score, band, signals, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.get("pub"),
                event.get("title"),
                event.get("link"),
                event.get("key", "미상"),
                float(event.get("score", 0.0)),
                event.get("band", "낮음"),
                event.get("signals", "{}"),
                _now_iso(),
            ),
        )
        con.commit()
        return cur.rowcount > 0
    except Exception:
        return False
    finally:
        con.close()


def get_ews(limit: int = 100):
    """
    (혹시 나중에 쓸 수도 있으니 남겨둔 함수)
    최신 pub 순으로 최대 limit개 반환.
    """
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        """
        SELECT pub, title, link, countries, summary
        FROM ews_events
        ORDER BY datetime(pub) DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def get_risk(limit: int = 200):
    """
    (혹시 나중에 쓸 수도 있으니 남겨둔 함수)
    최신 pub 순으로 최대 limit개 반환.
    """
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        """
        SELECT pub, title, link, key, score, band, signals
        FROM risk_events
        ORDER BY datetime(pub) DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def clear_all():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM ews_events")
    cur.execute("DELETE FROM risk_events")
    con.commit()
    con.close()


