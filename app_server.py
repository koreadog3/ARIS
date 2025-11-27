# app_server.py

import asyncio
import sqlite3

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from storage import (
    init_db,
    DB_PATH,
    insert_ews,
    insert_risk,
    clear_all,
)
from ews_core import run_once

app = FastAPI()

# CORS: 프론트(Cloudflare / 로컬 5173 등)에서 호출 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def fetch_all(table: str, limit: int = 200):
    """테이블에서 최신 순으로 최대 limit개 가져오기"""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    # pub이 ISO datetime 문자열이라 datetime(pub) 기준 정렬 + id 보조 정렬
    cur.execute(
        f"SELECT * FROM {table} ORDER BY datetime(pub) DESC, id DESC LIMIT ?",
        (limit,),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def count_rows(table: str) -> int:
    """행 개수 확인용 (디버그)"""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    (cnt,) = cur.fetchone()
    con.close()
    return int(cnt)


@app.get("/")
def root():
    return {"ok": True, "service": "ARIS"}


@app.get("/health")
def health():
    # UptimeRobot 등 헬스 체크용
    return {"status": "ok"}


@app.get("/status")
def status():
    """DB에 실제로 몇 개 들어가 있는지 확인용 간단 상태"""
    return {
        "ews_count": count_rows("ews_events"),
        "risk_count": count_rows("risk_events"),
        "db_path": str(DB_PATH),
    }


@app.get("/ews")
def get_ews():
    return fetch_all("ews_events")


@app.get("/risk")
def get_risk():
    return fetch_all("risk_events")


@app.post("/run")
async def run_manual():
    """
    수동 1회 수집/분석.
    몇 개 '만들었는지'가 아니라 실제로 DB에
    몇 개 저장됐는지로 숫자를 반환하게 바꿉니다.
    """
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


POLL_SECONDS = 120  # 2분 간격 (원하면 600~1800으로 늘리면 됨)


async def poll_loop():
    while True:
        try:
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
    

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(poll_loop())
    print("[STARTUP] poll_loop task created")


