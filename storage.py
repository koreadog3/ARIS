# storage.py
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
import json

DB_PATH = Path("ews.db")
KST = timezone(timedelta(hours=9))

def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

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

def _now_iso():
    return datetime.now(KST).isoformat()

def insert_ews(events):
    if not events:
        return 0
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    inserted = 0

    for e in events:
        try:
            countries = e.get("countries", ["미상"])
            if isinstance(countries, (list, dict)):
                countries = json.dumps(countries, ensure_ascii=False)

            cur.execute("""
            INSERT OR IGNORE INTO ews_events
            (pub, title, link, countries, summary, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                e.get("pub"),
                e.get("title"),
                e.get("link"),
                countries,
                e.get("summary", ""),
                _now_iso()
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            continue

    con.commit()
    con.close()
    return inserted

def insert_risk(events):
    if not events:
        return 0
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    inserted = 0

    for e in events:
        try:
            cur.execute("""
            INSERT OR IGNORE INTO risk_events
            (pub, title, link, key, score, band, signals, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                e.get("pub"),
                e.get("title"),
                e.get("link"),
                e.get("key", "미상"),
                float(e.get("score", 0.0)),
                e.get("band", "낮음"),
                e.get("signals", "{}"),
                _now_iso()
            ))
            if cur.rowcount > 0:
                inserted += 1
        except Exception:
            continue

    con.commit()
    con.close()
    return inserted

def get_ews(limit=100):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
    SELECT pub, title, link, countries, summary
    FROM ews_events
    ORDER BY pub DESC
    LIMIT ?
    """, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

def get_risk(limit=200):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("""
    SELECT pub, title, link, key, score, band, signals
    FROM risk_events
    ORDER BY pub DESC
    LIMIT ?
    """, (limit,))
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
