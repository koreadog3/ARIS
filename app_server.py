# app_server.py (ARIS backend)
import os
import asyncio
import sqlite3
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Response, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from ews_core import run_once
from storage import init_db, DB_PATH, insert_ews, insert_risk

# storage.py에 clear_all이 있으면 그걸 쓰고,
# 없으면 여기서 안전하게 fallback
try:
    from storage import clear_all  # type: ignore
except Exception:
    def clear_all():
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("DELETE FROM ews_events")
        cur.execute("DELETE FROM risk_events")
        con.commit()
        con.close()


app = FastAPI(title="ARIS Backend", version="0.3.0")

# -------------------------
# CORS
# -------------------------
# 기본은 전체 허용, 필요하면 ENV로 제한 가능
# 예: CORS_ORIGINS=https://aris.pe.kr,https://arisf.pages.dev
raw_origins = os.getenv("CORS_ORIGINS", "*")
if raw_origins.strip() == "*":
    allow_origins = ["*"]
else:
    allow_origins = [o.strip() for o in raw_origins.split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------
# DB fetch helper
# -------------------------
def fetch_all(table: str, limit: int = 200) -> List[Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {table} ORDER BY id DESC LIMIT ?", (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows

# 수동 run과 poll이 겹치는 것을 막기 위한 Lock
_run_lock = asyncio.Lock()

# -------------------------
# Routes
# -------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "ARIS"}

@app.head("/")
def head_root():
    return Response(status_code=200)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.head("/health")
def head_health():
    return Response(status_code=200)

@app.get("/ews")
def get_ews(limit: int = 200):
    # limit 과도하게 커지는 것 방지
    limit = max(1, min(1000, int(limit)))
    return fetch_all("ews_events", limit=limit)

@app.get("/risk")
def get_risk(limit: int = 200):
    limit = max(1, min(1000, int(limit)))
    return fetch_all("risk_events", limit=limit)

@app.post("/run")
async def run_manual():
    # 수동 1회 수집/분석
    async with _run_lock:
        try:
            ews_events, risk_events = await run_once()
            for e in ews_events:
                insert_ews(e)
            for r in risk_events:
                insert_risk(r)

            return {
                "inserted_ews": len(ews_events),
                "inserted_risk": len(risk_events),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/clear")
def api_clear():
    clear_all()
    return {"ok": True}

# -------------------------
# Poll loop
# -------------------------
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))

async def poll_loop():
    while True:
        try:
            async with _run_lock:
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

