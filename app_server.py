# app_server.py
import os
import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ews_core import run_once
from storage import init_db, insert_ews, insert_risk, get_ews, get_risk, clear_all

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))  # 기본 2분
EWS_LIMIT = int(os.getenv("EWS_LIMIT", "100"))
RISK_LIMIT = int(os.getenv("RISK_LIMIT", "200"))

app = FastAPI(title="ARIS / EWS + RISK")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

poll_task = None

async def poll_loop():
    while True:
        try:
            ews_events, risk_events = await run_once()
            ews_n = insert_ews(ews_events)
            risk_n = insert_risk(risk_events)
            print(f"[POLL] inserted ews={ews_n} risk={risk_n}")
        except Exception as e:
            print("[POLL ERROR]", e)
        await asyncio.sleep(POLL_SECONDS)

@app.on_event("startup")
async def on_startup():
    global poll_task
    init_db()
    poll_task = asyncio.create_task(poll_loop())
    print("[STARTUP] poll_loop task created")

@app.get("/ews")
def api_ews():
    return get_ews(EWS_LIMIT)

@app.get("/risk")
def api_risk():
    return get_risk(RISK_LIMIT)

@app.post("/run")
async def api_run_manual():
    ews_events, risk_events = await run_once()
    ews_n = insert_ews(ews_events)
    risk_n = insert_risk(risk_events)
    return {"inserted_ews": ews_n, "inserted_risk": risk_n}

@app.post("/clear")
def api_clear():
    clear_all()
    return {"ok": True}

