# ews_core.py
import re
import json
import asyncio
import hashlib
from math import exp
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque

import aiohttp
import async_timeout
from bs4 import BeautifulSoup

# -------------------------
# 기본 설정
# -------------------------
REQUEST_TIMEOUT = 15
MAX_RSS_SCAN = 120
DEDUP_HOURS = 48

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

KST = timezone(timedelta(hours=9))

NEWS_FEEDS = [
    # KR
    "https://www.khan.co.kr/rss/rssdata/politic_news.xml",
    "https://www.khan.co.kr/rss/rssdata/total_news.xml",

    # Google News 키워드 RSS
    "https://news.google.com/rss/search?q=%EC%A3%BC%ED%95%9C%20%EB%8C%80%EC%82%AC%EA%B4%80%20%EC%B2%A0%EC%88%98&hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/search?q=%EB%8C%80%EC%82%AC%EA%B4%80%20%EC%97%85%EB%AC%B4%20%EC%A4%91%EB%8B%A8&hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/search?q=%EC%97%AC%ED%96%89%EA%B2%BD%EB%B3%B4%20%EA%B2%A9%EC%83%81%20%ED%95%9C%EA%B5%AD&hl=ko&gl=KR&ceid=KR:ko",

    # EN / INTL
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://www.theguardian.com/world/rss",
    "https://www.france24.com/en/rss",

    # 한겨레 RSS 허브(본문 403 많아서 RSS 요약만 쓰게 됨)
    "https://www.hanion.co.kr/rss/S1N1.xml",
    "https://www.hanion.co.kr/rss/S1N2.xml",
    "https://www.hanion.co.kr/rss/S1N3.xml",
]

# -------------------------
# EWS 필터(강화판)
# -------------------------
EMBASSY_TERMS = re.compile(
    r"(?i)\b(embassy|consulate|mission|diplomat(s)?|ambassador|foreign\s+mission|대사관|영사관|공관|대표부|외교관|대사)\b"
)
WITHDRAW_TERMS = re.compile(
    r"(?i)\b(withdraw|pull\s*out|evacuate|evacuation|leave|relocate|close|suspend|shutdown|"
    r"철수|대피|폐쇄|휴관|업무\s*중단|잠정\s*중단|이전|철수령|대피령)\b"
)
KOREA_CONTEXT = re.compile(
    r"(?i)\b("
    r"주한|서울\s*주재|서울의|서울에\s*있는|한국\s*내|한국에서|대한민국에서|"
    r"in\s+seoul|seoul[-\s]?based|embassy\s+in\s+seoul|"
    r"in\s+south\s+korea|in\s+the\s+republic\s+of\s+korea|south\s*korean\s*capital"
    r")\b"
)
NON_KOREA_EMBASSY_HINT = re.compile(
    r"(?i)\b(embassy\s+in\s+baghdad|embassy\s+in\s+kyiv|embassy\s+in\s+tehran|embassy\s+in\s+beirut|"
    r"미\s*대사관\s*\(?바그다드\)?|키이우\s*주재\s*대사관|테헤란\s*주재\s*대사관)\b"
)

def is_ews_event(title: str, body: str) -> bool:
    blob = f"{title}\n{body}"
    if not (EMBASSY_TERMS.search(blob) and WITHDRAW_TERMS.search(blob)):
        return False
    if not KOREA_CONTEXT.search(blob):
        return False
    if NON_KOREA_EMBASSY_HINT.search(blob) and not re.search(r"(주한|서울)", blob):
        return False
    return True

# -------------------------
# RISK 시그널 정의
# -------------------------
SIGNAL_DEFS = {
    "military": {
        r"(missile|rocket|drone|airstrike|bombing|shelling|artillery|cluster\s+munitions?)": 3,
        r"(invasion|offensive|counteroffensive|frontline|battle|combat|war\b|conflict\b)": 2,
        r"(mobilization|conscription|martial\s+law|state\s+of\s+emergency)": 2,
    },
    "diplomacy": {
        r"(ceasefire|peace\s+talks?|negotiations?|summit|envoy|truce)": 1,
        r"(sanctions?|embargo|export\s+controls?|tariffs?)": 1,
        r"(expel|recall).*(ambassador|diplomat)": 2,
        r"(tensions?|standoff|ultimatum)": 1,
    },
    "civil": {
        r"(protest|riot|unrest|curfew|violent\s+clashes?)": 2,
        r"(terror(ism|ist)?\b|bomb\s+threat|hostage)": 3,
        r"(mass\s+evacuation|displacement|refugee\s+flow)": 2,
        r"(coup|mutiny|insurrection)": 2,
    },
    "economy": {
        r"(currency|FX|exchange\s+rate).*(crash|collapse|plunge|spike)": 1,
        r"(oil|gas|energy).*(disruption|cut|halt|shock)": 1,
        r"(항공편|항공\s*편|flights?).*(대량|mass|wide|many).*(취소|cancel)": 1,
        r"(보험|war risk).*(할증|premium|spike)": 1,
        r"(해운|항만).*(중단|지연|적체)": 1,
    }
}

NEG_RISK_FILTER = re.compile(
    r"(?i)\b("
    r"Netflix|film|movie|series|documentary|biopic|drama|trailer|episode|season|box\s*office|"
    r"celebrity|entertainment|interview|review|feature|opinion|"
    r"sports?|match|tournament|league|"
    r"crime|killer|murder|manhunt|cold\s*case|serial\s*killer|police\s*case|detective"
    r")\b"
)

def extract_signals(text: str) -> dict[str, int]:
    found = defaultdict(int)
    for bucket, rules in SIGNAL_DEFS.items():
        bucket_score = 0
        for pat, w in rules.items():
            if re.search(pat, text, flags=re.IGNORECASE):
                bucket_score = max(bucket_score, w)
        if bucket_score > 0:
            found[bucket] = bucket_score
    return found

# -------------------------
# 나라 추출 (가볍게)
# -------------------------
COUNTRY_CANON = {
    "Russia":"Russia","Ukraine":"Ukraine","China":"China","Taiwan":"Taiwan","Japan":"Japan",
    "South Korea":"South Korea","North Korea":"North Korea","United States":"United States",
    "United Kingdom":"United Kingdom","France":"France","Germany":"Germany","Israel":"Israel",
    "Iran":"Iran","Iraq":"Iraq","Syria":"Syria","Lebanon":"Lebanon","Vietnam":"Vietnam",
    "Sudan":"Sudan","Myanmar":"Myanmar","Thailand":"Thailand",

    "러시아":"Russia","우크라이나":"Ukraine","중국":"China","대만":"Taiwan","일본":"Japan",
    "대한민국":"South Korea","한국":"South Korea","북한":"North Korea","미국":"United States",
    "영국":"United Kingdom","프랑스":"France","독일":"Germany","이스라엘":"Israel",
    "이란":"Iran","이라크":"Iraq","시리아":"Syria","레바논":"Lebanon","베트남":"Vietnam",
    "수단":"Sudan","미얀마":"Myanmar","태국":"Thailand",
}
ALIASES = {
    "ROK":"South Korea","Korea":"South Korea","Republic of Korea":"South Korea",
    "DPRK":"North Korea","U.S.":"United States","US":"United States","USA":"United States",
    "UK":"United Kingdom",
}
NAME_TOKEN = re.compile(r"[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3}|[가-힣]{2,10}")

def extract_country_keys(title: str, body: str, top_k: int = 2) -> list[str]:
    text = f"{title}\n{body}"
    tokens = NAME_TOKEN.findall(text)
    found = []
    for t in tokens:
        t = t.strip()
        if t in ALIASES:
            t = ALIASES[t]
        if t in COUNTRY_CANON:
            found.append(COUNTRY_CANON[t])
    if not found:
        return ["미상"]
    freq = defaultdict(int)
    for c in found:
        freq[c] += 1
    ranked = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return [k for k, _ in ranked[:top_k]]

def extract_country_pairs(title: str, body: str):
    keys = extract_country_keys(title, body, top_k=3)
    keys = [k for k in keys if k != "미상"]
    pairs = []
    for i in range(len(keys)):
        for j in range(i+1, len(keys)):
            pairs.append((keys[i], keys[j]))
    return pairs

# -------------------------
# 위험도 계산
# -------------------------
def compute_risk_score(signals: dict[str, int], hours_ago: float, repeated_bonus: float = 0.0) -> float:
    raw = sum(signals.values()) + repeated_bonus
    decay = exp(-hours_ago / 48.0)
    score = raw * decay
    return max(0.0, min(10.0, score))

def band_from_score(score: float) -> str:
    if score >= 7.0: return "고위험"
    if score >= 4.0: return "경계"
    if score >= 2.0: return "주의"
    return "낮음"

# -------------------------
# 반복 보너스
# -------------------------
signal_history = defaultdict(lambda: deque(maxlen=64))

def repeated_bonus(country: str, now: datetime) -> float:
    hist = signal_history[country]
    bonus = 0.0
    for t, _ in hist:
        if (now - t).total_seconds() <= 24*3600:
            bonus += 0.5
    return min(2.0, bonus)

# -------------------------
# HTML → 텍스트
# -------------------------
def html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")

    candidates = []
    for sel in [
        "article", 'div[itemprop="articleBody"]', ".article-body", ".article_content",
        ".story-body", ".paywall", ".content__article-body", ".content"
    ]:
        candidates.extend(soup.select(sel))

    def clean(node):
        for bad in node.find_all(["script","style","noscript","header","footer","nav","aside","form"]):
            bad.decompose()
        return node.get_text(" ", strip=True)

    if candidates:
        texts = [clean(n) for n in candidates]
        text = max(texts, key=len, default="")
        if text:
            return " ".join(text.split())

    for bad in soup.find_all(["script","style","noscript","header","footer","nav","aside","form"]):
        bad.decompose()

    return " ".join(soup.get_text(" ", strip=True).split())

def rss_snippet(item) -> str:
    candidates = []
    for tag in ("content:encoded","content","description","summary","dc:description"):
        node = item.find(tag)
        if node and node.text:
            candidates.append(node.text.strip())
    for node in item.find_all(True):
        name = (node.name or "").lower()
        if any(name.endswith(x) for x in ("description","summary","encoded")) and node.text:
            candidates.append(node.text.strip())
    return max(candidates, key=len, default="")

def parse_pubdate(item) -> datetime:
    txt = ""
    for tag in ("pubDate","updated","published"):
        node = item.find(tag)
        if node and node.text:
            txt = node.text.strip()
            break
    fmts = [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(txt, fmt)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(KST)
        except Exception:
            continue
    return datetime.now(KST)

def pick_best_link(item) -> str:
    links = item.find_all("link") or []
    for ln in links:
        href = ln.get("href") or (ln.string.strip() if ln.string else "")
        rel  = (ln.get("rel") or "").lower()
        typ  = (ln.get("type") or "").lower()
        if href.startswith("http") and (rel == "alternate" or "html" in typ):
            return href
    # fallback
    lnode = item.find("link")
    if lnode:
        if getattr(lnode, "string", None):
            return lnode.string.strip()
        if lnode.get("href"):
            return lnode.get("href").strip()
    g = item.find("guid")
    if g and g.text.strip().startswith("http"):
        return g.text.strip()
    return ""

def _host(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""

BLOCK_ARTICLE_DOMAINS = {"hani.co.kr", "hanion.co.kr"}

async def fetch_text(session: aiohttp.ClientSession, url: str, is_rss: bool = False) -> str:
    if not url:
        return ""
    for attempt in range(3):
        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, headers=UA, allow_redirects=True) as resp:
                    if resp.status != 200:
                        tag = "RSS" if is_rss else "FETCH"
                        print(f"[{tag} FAIL] {resp.status} {url}")
                        return ""
                    return await resp.text()
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(0.7 * (attempt + 1))
                continue
            tag = "RSS" if is_rss else "FETCH"
            print(f"[{tag} ERROR] {url} {repr(e)}")
            return ""
    return ""

async def search_news(session: aiohttp.ClientSession):
    items = []
    for url in NEWS_FEEDS:
        xml = await fetch_text(session, url, is_rss=True)
        if not xml:
            print(f"[RSS EMPTY] {url}")
            continue
        soup = BeautifulSoup(xml, "xml")
        got = soup.find_all("item") + soup.find_all("entry")
        print(f"[RSS OK] {url} items={len(got)}")
        items.extend(got)

    norm = [(parse_pubdate(it), it) for it in items]
    norm.sort(key=lambda x: x[0], reverse=True)
    return [it for _, it in norm]

# -------------------------
# dedupe
# -------------------------
seen_fp_expiry = {}

def _fingerprint(title: str, link: str) -> str:
    base = (link or title or "").strip().lower()
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def _purge_seen(now: datetime):
    for fp, exp in list(seen_fp_expiry.items()):
        if exp <= now:
            seen_fp_expiry.pop(fp, None)

# -------------------------
# 핵심 1회 실행
# -------------------------
async def run_once():
    ews_events = []
    risk_events = []

    now = datetime.now(KST)
    _purge_seen(now)

    scanned = 0
    ews_passed = 0
    sig_hits = 0

    async with aiohttp.ClientSession() as session:
        rss_items = await search_news(session)

        for item in rss_items[:MAX_RSS_SCAN]:
            title_node = item.find("title")
            title = title_node.text.strip() if title_node and title_node.text else ""
            link = pick_best_link(item)

            if not (title or link):
                continue

            fp = _fingerprint(title, link)
            if fp in seen_fp_expiry:
                continue

            pub = parse_pubdate(item)
            if (now - pub).total_seconds() > 72 * 3600:
                continue

            article_text = ""
            if link and _host(link) not in BLOCK_ARTICLE_DOMAINS:
                html = await fetch_text(session, link, is_rss=False)
                article_text = html_to_text(html) if html else ""

            if len(article_text) < 200:
                article_text = html_to_text(rss_snippet(item)) or title

            if not article_text:
                continue

            scanned += 1
            blob = f"{title}\n{article_text}"

            # ---- EWS ----
            if is_ews_event(title, article_text):
                ews_passed += 1
                keys = extract_country_keys(title, article_text, top_k=2)
                keys = [k for k in keys if k != "미상"]
                ews_events.append({
                    "pub": pub.isoformat(),
                    "title": title,
                    "link": link,
                    "countries": keys or ["미상"],
                    "summary": ""
                })
                seen_fp_expiry[fp] = now + timedelta(hours=DEDUP_HOURS)
                continue

            # ---- RISK ----
            signals = extract_signals(blob)
            if signals:
                sig_hits += 1
            else:
                continue

            strong = (sum(signals.values()) >= 3) or (signals.get("military", 0) >= 3)
            if NEG_RISK_FILTER.search(title) and not strong:
                continue

            total_sig = sum(signals.values())
            multi_bucket = sum(1 for v in signals.values() if v > 0) >= 2
            if not (strong or multi_bucket or total_sig >= 2):
                continue

            hours_ago = max(0.0, (now - pub).total_seconds() / 3600.0)

            pairs = extract_country_pairs(title, article_text)
            if pairs:
                for a, b in pairs:
                    key = f"{a} | {b}"
                    signal_history[a].append((now, signals))
                    signal_history[b].append((now, signals))
                    repeat_b = max(repeated_bonus(a, now), repeated_bonus(b, now))
                    score = compute_risk_score(signals, hours_ago, repeat_b + 0.5)
                    band = band_from_score(score)
                    risk_events.append({
                        "pub": pub.isoformat(),
                        "title": title,
                        "link": link,
                        "key": key,
                        "score": float(score),
                        "band": band,
                        "signals": json.dumps(signals, ensure_ascii=False)
                    })
            else:
                score = compute_risk_score(signals, hours_ago, 0.0)
                band = band_from_score(score)
                risk_events.append({
                    "pub": pub.isoformat(),
                    "title": title,
                    "link": link,
                    "key": "미상",
                    "score": float(score),
                    "band": band,
                    "signals": json.dumps(signals, ensure_ascii=False)
                })

            seen_fp_expiry[fp] = now + timedelta(hours=DEDUP_HOURS)

    # ✅ 여기 로그는 파일이 정말 이 버전일 때만 뜹니다
    print(f"[DEBUG] scanned={scanned} ews_pass={ews_passed} sig_hits={sig_hits} "
          f"ews_events={len(ews_events)} risk_events={len(risk_events)}")

    return ews_events, risk_events






