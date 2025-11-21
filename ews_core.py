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
MAX_RSS_SCAN = 120          # RSS에서 최대 몇 개 아이템까지 훑을지
MAX_ARTICLE_FETCH = 80      # 실제 기사 본문/요약 분석할 최대 개수
DEDUP_HOURS = 48

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

KST = timezone(timedelta(hours=9))

# -------------------------
# RSS 피드 목록
# - 서버에서 404/차단 잦은 것(ReutersAgency/DW/연합뉴스 검색식 등)은 기본 제외
# - 안정 피드 + Google News 키워드 RSS로 대체
# -------------------------
NEWS_FEEDS = [
    # KR (정치/외교 관련)
    "https://www.khan.co.kr/rss/rssdata/politic_news.xml",   # 경향 정치 (공식 RSS)
    "https://www.khan.co.kr/rss/rssdata/total_news.xml",     # 경향 전체

    # Google News (한국 키워드 검색 RSS)
    # ※ 키워드 추가/수정은 여기에서 하시면 됩니다.
    "https://news.google.com/rss/search?q=%EC%A3%BC%ED%95%9C%20%EB%8C%80%EC%82%AC%EA%B4%80%20%EC%B2%A0%EC%88%98&hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/search?q=%EB%8C%80%EC%82%AC%EA%B4%80%20%EC%97%85%EB%AC%B4%20%EC%A4%91%EB%8B%A8&hl=ko&gl=KR&ceid=KR:ko",
    "https://news.google.com/rss/search?q=%EC%97%AC%ED%96%89%EA%B2%BD%EB%B3%B4%20%EA%B2%A9%EC%83%81%20%ED%95%9C%EA%B5%AD&hl=ko&gl=KR&ceid=KR:ko",

    # EN / INTL (안정적인 공개 RSS)
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://www.theguardian.com/world/rss",
    "https://www.france24.com/en/rss",

    # 한겨레:온 RSS (공식 RSS 허브)
    "https://www.hanion.co.kr/rss/S1N1.xml",
    "https://www.hanion.co.kr/rss/S1N2.xml",
    "https://www.hanion.co.kr/rss/S1N3.xml",
]

# -------------------------
# EWS 필터 강화:
# 1) 공관/대사관/외교용어 + 철수/대피/업무중단
# 2) "주한/서울/한국 내" 맥락이 반드시 있어야 EWS
# -------------------------
EMBASSY_TERMS = re.compile(
    r"(?i)\b(embassy|consulate|mission|diplomat(s)?|ambassador|foreign\s+mission|대사관|영사관|공관|대표부|외교관|대사)\b"
)
WITHDRAW_TERMS = re.compile(
    r"(?i)\b(withdraw|pull\s*out|evacuate|evacuation|leave|relocate|close|suspend|shutdown|"
    r"철수|대피|폐쇄|휴관|업무\s*중단|잠정\s*중단|철거|이전|철수령|대피령)\b"
)
KOREA_CONTEXT = re.compile(
    r"(?i)\b("
    r"주한|서울\s*주재|서울의|서울에\s*있는|한국\s*내|한국에서|대한민국에서|"
    r"in\s+seoul|seoul[-\s]?based|embassy\s+in\s+seoul|"
    r"in\s+south\s+korea|in\s+the\s+republic\s+of\s+korea|south\s*korean\s*capital"
    r")\b"
)
# EWS에 오탐을 만들기 쉬운 표현(다른 나라 공관 철수) 최소 차단
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
    # 한국 맥락이 있어도 “다른 나라 공관 철수” 힌트가 강하면 EWS 제외
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
                bucket_score = max(bucket_score, w)  # 합산 대신 최댓값
        if bucket_score > 0:
            found[bucket] = bucket_score
    return found

# -------------------------
# 나라 추출 (가볍게)
# -------------------------
COUNTRY_CANON = {
    # EN
    "Russia":"Russia","Ukraine":"Ukraine","Poland":"Poland","Belarus":"Belarus",
    "Armenia":"Armenia","Azerbaijan":"Azerbaijan","India":"India","Pakistan":"Pakistan",
    "China":"China","Taiwan":"Taiwan","Japan":"Japan","South Korea":"South Korea",
    "North Korea":"North Korea","United States":"United States","United Kingdom":"United Kingdom",
    "France":"France","Germany":"Germany","Turkey":"Turkey","Iran":"Iran","Iraq":"Iraq",
    "Israel":"Israel","Lebanon":"Lebanon","Syria":"Syria","Saudi Arabia":"Saudi Arabia",
    "United Arab Emirates":"United Arab Emirates","Qatar":"Qatar","Egypt":"Egypt", "Vietnam":"Vietnam",
    "Sudan":"Sudan","South Sudan":"South Sudan","Myanmar":"Myanmar","Thailand":"Thailand",
    "Philippines":"Philippines","Indonesia":"Indonesia","Malaysia":"Malaysia","Singapore":"Singapore",
    "Australia":"Australia","Canada":"Canada","Mexico":"Mexico","Brazil":"Brazil","Argentina":"Argentina",
    "Chile":"Chile","Peru":"Peru","Colombia":"Colombia","Venezuela":"Venezuela","Nigeria":"Nigeria",
    "Ethiopia":"Ethiopia","Kenya":"Kenya","Tanzania":"Tanzania","Uganda":"Uganda","Somalia":"Somalia",
    "Libya":"Libya","Tunisia":"Tunisia","Algeria":"Algeria","Morocco":"Morocco","Spain":"Spain",
    "Italy":"Italy","Greece":"Greece","Sweden":"Sweden","Norway":"Norway","Finland":"Finland",
    "Netherlands":"Netherlands","Belgium":"Belgium","Switzerland":"Switzerland","Austria":"Austria",
    "Czechia":"Czechia","Slovakia":"Slovakia","Hungary":"Hungary","Romania":"Romania",
    "Bulgaria":"Bulgaria","Serbia":"Serbia","Croatia":"Croatia","Slovenia":"Slovenia",
    "Denmark":"Denmark","Portugal":"Portugal","Ireland":"Ireland","New Zealand":"New Zealand",

    # KO
    "러시아":"Russia","우크라이나":"Ukraine","폴란드":"Poland","벨라루스":"Belarus",
    "아르메니아":"Armenia","아제르바이잔":"Azerbaijan","인도":"India","파키스탄":"Pakistan",
    "중국":"China","대만":"Taiwan","일본":"Japan","대한민국":"South Korea","한국":"South Korea",
    "북한":"North Korea","미국":"United States","영국":"United Kingdom","프랑스":"France",
    "독일":"Germany","터키":"Turkey","튀르키예":"Turkey","이란":"Iran","이라크":"Iraq",
    "이스라엘":"Israel","레바논":"Lebanon","시리아":"Syria","사우디아라비아":"Saudi Arabia",
    "아랍에미리트":"United Arab Emirates","카타르":"Qatar","이집트":"Egypt","베트남":"Vietnam",
    "수단":"Sudan","남수단":"South Sudan","미얀마":"Myanmar","태국":"Thailand",
    "필리핀":"Philippines","인도네시아":"Indonesia","말레이시아":"Malaysia","싱가포르":"Singapore",
    "호주":"Australia","캐나다":"Canada","멕시코":"Mexico","브라질":"Brazil","아르헨티나":"Argentina",
    "칠레":"Chile","페루":"Peru","콜롬비아":"Colombia","베네수엘라":"Venezuela",
    "나이지리아":"Nigeria","에티오피아":"Ethiopia","케냐":"Kenya","탄자니아":"Tanzania",
    "우간다":"Uganda","소말리아":"Somalia","리비아":"Libya","튀니지":"Tunisia",
    "알제리":"Algeria","모로코":"Morocco","스페인":"Spain","이탈리아":"Italy",
    "그리스":"Greece","스웨덴":"Sweden","노르웨이":"Norway","핀란드":"Finland",
    "네덜란드":"Netherlands","벨기에":"Belgium","스위스":"Switzerland","오스트리아":"Austria",
    "체코":"Czechia","슬로바키아":"Slovakia","헝가리":"Hungary","루마니아":"Romania",
    "불가리아":"Bulgaria","세르비아":"Serbia","크로아티아":"Croatia","슬로베니아":"Slovenia",
    "덴마크":"Denmark","포르투갈":"Portugal","아일랜드":"Ireland","뉴질랜드":"New Zealand",
}
ALIASES = {
    "ROK":"South Korea","Korea":"South Korea","Republic of Korea":"South Korea",
    "DPRK":"North Korea","U.S.":"United States","US":"United States","USA":"United States",
    "UK":"United Kingdom","UAE":"United Arab Emirates","KSA":"Saudi Arabia",
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
        for j in range(i + 1, len(keys)):
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
# 반복/에스컬레이션 보너스
# -------------------------
signal_history = defaultdict(lambda: deque(maxlen=64))

def repeated_bonus(country: str, now: datetime) -> float:
    hist = signal_history[country]
    bonus = 0.0
    for t, _ in hist:
        if (now - t).total_seconds() <= 24 * 3600:
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
        "article",
        'div[itemprop="articleBody"]',
        ".article-body",
        ".article_content",
        ".story-body",
        ".paywall",
        ".content__article-body",
        ".content"
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

# -------------------------
# 네트워크 fetch (재시도 포함)
# -------------------------
def _host(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""

# 한겨레 등 본문 403이 잦은 곳은 RSS 요약만 사용
BLOCK_ARTICLE_DOMAINS = {"hani.co.kr", "hanion.co.kr"}

async def fetch_text(session: aiohttp.ClientSession, url: str, is_rss: bool = False) -> str:
    # RSS/HTML 공통 fetch, 2회 재시도
    for attempt in range(2):
        try:
            # ❗ 여기: with -> async with 로 고침
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, headers=UA, allow_redirects=True) as resp:
                    if resp.status != 200:
                        tag = "RSS" if is_rss else "FETCH"
                        print(f"[{tag} FAIL] {resp.status} {url}")
                        return ""
                    return await resp.text()

        except Exception as e:
            if attempt == 0:
                await asyncio.sleep(0.6)
                continue
            tag = "RSS" if is_rss else "FETCH"
            print(f"[{tag} ERROR] {url} {repr(e)}")
            return ""

    return ""


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

def extract_link(item) -> str:
    link = ""
    lnode = item.find("link")
    if lnode:
        if getattr(lnode, "string", None):
            link = lnode.string.strip()
        elif lnode.get("href"):
            link = lnode.get("href").strip()
    if not link and item.find("guid"):
        g = item.find("guid").text.strip()
        if g.startswith("http"):
            link = g
    return link

def pick_best_link(item) -> str:
    links = item.find_all("link") or []
    for ln in links:
        href = ln.get("href") or (ln.string.strip() if ln.string else "")
        rel  = (ln.get("rel") or "").lower()
        typ  = (ln.get("type") or "").lower()
        if href.startswith("http") and (rel == "alternate" or "html" in typ):
            return href
    return extract_link(item)

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
# dedupe (48h)
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

            # 본문/요약 확보
            article_text = ""
            if link and _host(link) not in BLOCK_ARTICLE_DOMAINS:
                html = await fetch_text(session, link)
                article_text = html_to_text(html) if html else ""

            if len(article_text) < 200:
                article_text = html_to_text(rss_snippet(item))

            if not article_text:
                article_text = title

            blob = f"{title}\n{article_text}"

            # ---- EWS ----
            if is_ews_event(title, article_text):
                keys = extract_country_keys(title, article_text, top_k=2)
                keys = [k for k in keys if k != "미상"]

                ews_events.append({
                    "pub": pub.isoformat(),
                    "title": title,
                    "link": link,
                    "countries": keys or ["미상"],
                    "summary": ""  # 요약/번역은 서버 다른 단계에서 붙이기
                })

                seen_fp_expiry[fp] = now + timedelta(hours=DEDUP_HOURS)
                if len(ews_events) >= 40:
                    continue
                continue

            # ---- RISK ----
            signals = extract_signals(blob)
            if not signals:
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

            if ews_events or risk_events:
                seen_fp_expiry[fp] = now + timedelta(hours=DEDUP_HOURS)

            if len(risk_events) >= 80:
                continue

    return ews_events, risk_events








