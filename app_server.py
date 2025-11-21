# app_server.py
import asyncio
import sqlite3
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from storage import init_db, DB_PATH, insert_ews, insert_risk
from ews_core import run_once

app = FastAPI()

# CORS: 프론트(5173)에서 백엔드(8000) 호출 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fetch_all(table: str, limit: int = 200):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

@app.get("/ews")
def get_ews():
    return fetch_all("ews_events")

@app.get("/risk")
def get_risk():
    return fetch_all("risk_events")

@app.post("/run")
async def run_manual():
    # 수동 1회 수집/분석
    ews_events, risk_events = await run_once()
    for e in ews_events:
        insert_ews(e)
    for r in risk_events:
        insert_risk(r)
    return {"inserted_ews": len(ews_events), "inserted_risk": len(risk_events)}

POLL_SECONDS = 120  # 2분 (원하면 1800=30분으로)

async def poll_loop():
    while True:
        try:
            ews_events, risk_events = await run_once()
            for e in ews_events:
                insert_ews(e)
            for r in risk_events:
                insert_risk(r)
            print(f"[POLL] inserted ews={len(ews_events)} risk={len(risk_events)}")
        except Exception as e:
            print("[POLL ERROR]", repr(e))

        await asyncio.sleep(POLL_SECONDS)

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(poll_loop())
    print("[STARTUP] poll_loop task created")