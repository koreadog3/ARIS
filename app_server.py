# app_server.py  (ARIS backend - 안정화 버전)

import os
import asyncio
import sqlite3
from typing import List, Dict, Any

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from storage import (
    init_db,
    DB_PATH,
    insert_ews,
    insert_risk,
    clear_all,
)
from ews_core import run_once


app = FastAPI(title="ARIS Backend", version="0.3.1")

# -------------------------
# CORS 설정
# -------------------------
# 기본은 전체 허용. 원하면 CORS_ORIGINS 환경변수로 제한 가능:
# 예) CORS_ORIGINS="https://aris.pe.kr,https://arisf.pages.dev"
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

# 수동 / 자동 실행이 서로 겹치지 않도록 Lock
_run_lock = asyncio.Lock()


# -------------------------
# DB helper
# -------------------------
def fetch_all(table: str, limit: int = 200) -> List[Dict[str, Any]]:
    """테이블에서 최신(pub) 순으로 최대 limit개 가져오기"""
    limit = max(1, min(1000, int(limit)))
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(
        f"""
        SELECT *
        FROM {table}
        ORDER BY datetime(pub) DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def count_rows(table: str) -> int:
    """행 개수 확인용 (상태 체크)"""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    (cnt,) = cur.fetchone()
    con.close()
    return int(cnt)


# -------------------------
# Routes
# -------------------------
@app.get("/")
def root():
    return {"ok": True, "service": "ARIS"}


# UptimeRobot / Koyeb 헬스체크용: 어떤 메서드로 와도 200 OK
@app.api_route("/health", methods=["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"])
def health_all():
    return Response(
        content='{"status":"ok"}',
        media_type="application/json",
        status_code=200,
    )


@app.get("/status")
def status():
    """DB에 실제로 몇 개 들어가 있는지 확인용 간단 상태"""
    return {
        "ews_count": count_rows("ews_events"),
        "risk_count": count_rows("risk_events"),
        "db_path": str(DB_PATH),
    }


@app.get("/ews")
def get_ews(limit: int = 200):
    return fetch_all("ews_events", limit=limit)


@app.get("/risk")
def get_risk(limit: int = 200):
    return fetch_all("risk_events", limit=limit)


@app.post("/run")
async def run_manual():
    """
    수동 1회 수집/분석.
    generated_* : run_once가 만든 개수
    inserted_*  : 실제로 DB에 새로 들어간 개수
    """
    async with _run_lock:
        ews_events, risk_events = await run_once()

        inserted_ews = 0
        for e in ews_events:
            if insert_ews(e):
                inserted_ews += 1

        inserted_risk = 0
        for r in risk_events:
            if insert_risk(r):
                inserted_risk += 1

        return {
            "generated_ews": len(ews_events),
            "generated_risk": len(risk_events),
            "inserted_ews": inserted_ews,
            "inserted_risk": inserted_risk,
        }


# -------------------------
# Poll loop (백그라운드 주기 실행)
# -------------------------
# 기본 120초. 환경변수 POLL_SECONDS 로 조절 가능.
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))


async def poll_loop():
    while True:
        try:
            async with _run_lock:
                ews_events, risk_events = await run_once()

                inserted_ews = 0
                for e in ews_events:
                    if insert_ews(e):
                        inserted_ews += 1

                inserted_risk = 0
                for r in risk_events:
                    if insert_risk(r):
                        inserted_risk += 1

                print(
                    f"[POLL] generated ews={len(ews_events)} "
                    f"risk={len(risk_events)} | "
                    f"saved ews={inserted_ews} risk={inserted_risk}"
                )
        except Exception as e:
            print("[POLL ERROR]", repr(e))

        await asyncio.sleep(POLL_SECONDS)


@app.post("/clear")
def api_clear():
    """ews/risk 모두 삭제 (테이블은 유지)"""
    clear_all()
    return {"ok": True}


# -------------------------
# startup
# -------------------------
@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(poll_loop())
    print("[STARTUP] poll_loop task created")


