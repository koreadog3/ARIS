# ews_core.py (quiet + stable)
import re
import json
import os
import asyncio
import aiohttp
import async_timeout
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from math import exp
from openai import AsyncOpenAI

# -------------------------
# 기본 설정
# -------------------------
REQUEST_TIMEOUT = 15
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

KST = timezone(timedelta(hours=9))

# -------------------------
# 뉴스 피드 목록
# -------------------------
NEWS_FEEDS = [
    # KR - 경향신문
    "https://www.khan.co.kr/rss/rssdata/politic_news.xml",
    "https://www.khan.co.kr/rss/rssdata/society_news.xml",
    "https://www.khan.co.kr/rss/rssdata/kh_world.xml",
    "https://www.khan.co.kr/rss/rssdata/total_news.xml",

    # KR - 한겨레
    "http://www.hani.co.kr/rss/",
    "https://www.hani.co.kr/rss/international/",
    "https://www.hani.co.kr/rss/politics/",

    # KR - 연합뉴스(섹션)
    "https://www.yna.co.kr/rss/politics.xml",
    "https://www.yna.co.kr/rss/international.xml",
    "https://www.yna.co.kr/rss/northkorea.xml",
    "https://www.yna.co.kr/rss/society.xml",

    # EN / INTL
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://www.theguardian.com/world/rss",
    "https://rss.dw.com/rdf/rss-en-top",
    "https://www.france24.com/en/rss",
]

# -------------------------
# EWS 필터 (주한 외국공관 철수만)
# -------------------------
EWS_ACTION = re.compile(
    r"(?i)("
    r"embassy\s+(withdraw|evacuate|evacuation|pull\s*out|close|closure|shutdown|suspend)"
    r"|diplomat(s)?\s+(leave|withdraw|evacuate|relocate)"
    r"|evacuation\s+order"
    r"|(대사관|영사관|공관).{0,40}(철수|대피|폐쇄|휴관|업무\s*중단)"
    r"|(여행경보|여행\s*경보).{0,20}(상향|격상|발령|3단계|4단계)"
    r"|(주재원|교민|국민).{0,20}(대피|철수)"
    r")"
)

EWS_KOREA_CTX = re.compile(
    r"(?i)("
    r"주한|서울|용산|광화문|"
    r"대한민국|한국|Korea|South\s*Korea|Republic\s+of\s+Korea|"
    r"Seoul"
    r")"
)

EWS_KOREA_NEG = re.compile(
    r"(?i)("
    r"Korean\s+embassy|ROK\s+embassy|Republic\s+of\s+Korea\s+embassy|"
    r"대한민국\s*대사관"
    r")"
)

def quick_filter(text: str) -> bool:
    """주한 외국공관 철수/대피/폐쇄에 해당하는 경우만 True."""
    if not text:
        return False
    if not EWS_ACTION.search(text):
        return False
    if not EWS_KOREA_CTX.search(text):
        return False
    if EWS_KOREA_NEG.search(text) and "주한" not in text:
        return False
    return True

# -------------------------
# RISK 시그널 정의 (EN + KO)
# -------------------------
SIGNAL_DEFS = {
    "military": {
        r"(missile|rocket|drone|airstrike|bombing|shelling|artillery|cluster\s+munitions?)": 3,
        r"(invasion|offensive|counteroffensive|frontline|battle|combat|war\b|conflict\b)": 2,
        r"(mobilization|conscription|martial\s+law|state\s+of\s+emergency)": 2,
        r"(미사일|로켓|드론|공습|폭격|포격|포탄|다연장|대공포|군사훈련|실사격|훈련\s*강화|무력\s*시위)": 3,
        r"(침공|공세|반격|전선|교전|전투|전쟁|무력충돌|충돌|교전\s*확대)": 2,
        r"(동원령|징집|예비군\s*소집|계엄|비상사태|준전시|전시태세|전투태세\s*격상)": 2,
    },
    "diplomacy": {
        r"(ceasefire|peace\s+talks?|negotiations?|summit|envoy)": 1,
        r"(sanctions?|embargo|export\s+controls?)": 1,
        r"(expel|recall).{0,20}(ambassador|diplomat)": 2,
        r"(휴전|평화회담|협상|정상회담|특사|중재|대화\s*재개)": 1,
        r"(제재|금수|수출통제|경제\s*제재|무역\s*제한)": 1,
        r"(대사|외교관).{0,20}(추방|소환|철수|귀국\s*명령)": 2,
    },
    "civil": {
        r"(protest|riot|unrest|curfew|violent\s+clashes?)": 2,
        r"(terror(ism|ist)?\b|bomb\s+threat|hostage)": 3,
        r"(mass\s+evacuation|displacement|refugee\s+flow)": 2,
        r"(시위|폭동|소요|불안|통행금지|유혈충돌|치안\s*불안)": 2,
        r"(테러|폭탄\s*위협|인질|총격|자폭|폭발\s*사건)": 3,
        r"(대규모\s*대피|피란|난민|탈출행렬|피난민\s*급증)": 2,
    },
    "economy": {
        r"(currency|FX|exchange\s+rate).{0,20}(crash|collapse|plunge|spike)": 1,
        r"(oil|gas|energy).{0,20}(disruption|cut|halt|shock)": 1,
        r"(항공편|항공\s*편|flights?).{0,40}(대량|mass|wide|many).{0,20}(취소|cancel)": 1,
        r"(보험|war risk).{0,20}(할증|premium|spike)": 1,
        r"(해운|항만).{0,20}(중단|지연|적체)": 1,
        r"(환율|외환).{0,20}(급락|폭락|급등|불안|패닉|쇼크)": 1,
        r"(유가|가스|에너지).{0,20}(차질|중단|충격|급등|공급\s*차단)": 1,
        r"(항공편).{0,20}(결항|취소|대규모\s*취소|운항\s*중단)": 1,
        r"(전쟁보험|워리스크).{0,20}(할증|급등|프리미엄\s*상승)": 1,
        r"(해운|항만).{0,20}(마비|중단|지연|적체|봉쇄)": 1,
    },
}

NEG_RISK_FILTER = re.compile(
    r"(?i)("
    r"Netflix|film|movie|series|documentary|biopic|drama|trailer|episode|season|box\s*office|"
    r"celebrity|entertainment|interview|review|feature|opinion|"
    r"sports?|match|tournament|league|"
    r"crime|killer|murder|manhunt|cold\s*case|serial\s*killer|police\s*case|detective"
    r")"
)

def extract_signals(text: str) -> dict[str, int]:
    found: dict[str, int] = defaultdict(int)
    if not text:
        return found

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
    # EN
    "Russia":"Russia","Ukraine":"Ukraine","Poland":"Poland","Belarus":"Belarus",
    "Armenia":"Armenia","Azerbaijan":"Azerbaijan","India":"India","Pakistan":"Pakistan",
    "China":"China","Taiwan":"Taiwan","Japan":"Japan","South Korea":"South Korea",
    "North Korea":"North Korea","United States":"United States","United Kingdom":"United Kingdom",
    "France":"France","Germany":"Germany","Turkey":"Turkey","Iran":"Iran","Iraq":"Iraq",
    "Israel":"Israel","Lebanon":"Lebanon","Syria":"Syria","Saudi Arabia":"Saudi Arabia",
    "United Arab Emirates":"United Arab Emirates","Qatar":"Qatar","Egypt":"Egypt","Vietnam":"Vietnam",
    "Sudan":"Sudan","South Sudan":"South Sudan","Myanmar":"Myanmar","Thailand":"Thailand",
    "Philippines":"Philippines","Indonesia":"Indonesia","Malaysia":"Malaysia","Singapore":"Singapore",
    "Australia":"Australia","Canada":"Canada","Mexico":"Mexico","Brazil":"Brazil","Argentina":"Argentina",
    "Chile":"Chile","Peru":"Peru","Colombia":"Colombia","Venezuela":"Venezuela",
    "Nigeria":"Nigeria","Ethiopia":"Ethiopia","Kenya":"Kenya","Tanzania":"Tanzania","Uganda":"Uganda",
    "Somalia":"Somalia","Libya":"Libya","Tunisia":"Tunisia","Algeria":"Algeria","Morocco":"Morocco",
    "Spain":"Spain","Italy":"Italy","Greece":"Greece","Sweden":"Sweden","Norway":"Norway","Finland":"Finland",
    "Netherlands":"Netherlands","Belgium":"Belgium","Switzerland":"Switzerland","Austria":"Austria",
    "Czechia":"Czechia","Slovakia":"Slovakia","Hungary":"Hungary","Romania":"Romania","Bulgaria":"Bulgaria",
    "Serbia":"Serbia","Croatia":"Croatia","Slovenia":"Slovenia","Denmark":"Denmark","Portugal":"Portugal",
    "Ireland":"Ireland","New Zealand":"New Zealand",
    # KO
    "러시아":"Russia","우크라이나":"Ukraine","폴란드":"Poland","벨라루스":"Belarus",
    "아르메니아":"Armenia","아제르바이잔":"Azerbaijan","인도":"India","파키스탄":"Pakistan",
    "중국":"China","대만":"Taiwan","일본":"Japan","대한민국":"South Korea","한국":"South Korea",
    "북한":"North Korea","미국":"United States","영국":"United Kingdom","프랑스":"France",
    "독일":"Germany","터키":"Turkey","튀르키예":"Turkey","이란":"Iran","이라크":"Iraq",
    "이스라엘":"Israel","레바논":"Lebanon","시리아":"Syria","사우디아라비아":"Saudi Arabia",
    "아랍에미리트":"United Arab Emirates","카타르":"Qatar","이집트":"Egypt","베트남":"Vietnam",
    "수단":"Sudan","남수단":"South Sudan","미얀마":"Myanmar","태국":"Thailand","필리핀":"Philippines",
    "인도네시아":"Indonesia","말레이시아":"Malaysia","싱가포르":"Singapore","호주":"Australia",
    "캐나다":"Canada","멕시코":"Mexico","브라질":"Brazil","아르헨티나":"Argentina","칠레":"Chile",
    "페루":"Peru","콜롬비아":"Colombia","베네수엘라":"Venezuela","나이지리아":"Nigeria",
    "에티오피아":"Ethiopia","케냐":"Kenya","탄자니아":"Tanzania","우간다":"Uganda","소말리아":"Somalia",
    "리비아":"Libya","튀니지":"Tunisia","알제리":"Algeria","모로코":"Morocco","스페인":"Spain",
    "이탈리아":"Italy","그리스":"Greece","스웨덴":"Sweden","노르웨이":"Norway","핀란드":"Finland",
    "네덜란드":"Netherlands","벨기에":"Belgium","스위스":"Switzerland","오스트리아":"Austria",
    "체코":"Czechia","슬로바키아":"Slovakia","헝가리":"Hungary","루마니아":"Romania","불가리아":"Bulgaria",
    "세르비아":"Serbia","크로아티아":"Croatia","슬로베니아":"Slovenia","덴마크":"Denmark",
    "포르투갈":"Portugal","아일랜드":"Ireland","뉴질랜드":"New Zealand",
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
    found: list[str] = []
    for t in tokens:
        t = t.strip()
        if t in ALIASES:
            t = ALIASES[t]
        if t in COUNTRY_CANON:
            found.append(COUNTRY_CANON[t])
    if not found:
        return ["미상"]
    freq: dict[str, int] = defaultdict(int)
    for c in found:
        freq[c] += 1
    ranked = sorted(freq.items(), key=lambda x: x[1], reverse=True)
    return [k for k, _ in ranked[:top_k]]

def extract_country_pairs(title: str, body: str):
    keys = extract_country_keys(title, body, top_k=3)
    keys = [k for k in keys if k != "미상"]
    pairs: list[tuple[str, str]] = []
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
    if score >= 7.0:
        return "고위험"
    if score >= 4.0:
        return "경계"
    if score >= 2.0:
        return "주의"
    return "낮음"

# -------------------------
# 반복 보너스
# -------------------------
signal_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=64))

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
        ".content",
    ]:
        candidates.extend(soup.select(sel) or [])

    def clean(node):
        for bad in node.find_all(
            ["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]
        ):
            bad.decompose()
        return node.get_text(" ", strip=True)

    if candidates:
        texts = [clean(n) for n in candidates]
        text = max(texts, key=len, default="")
        if text:
            return " ".join(text.split())

    for bad in soup.find_all(
        ["script", "style", "noscript", "header", "footer", "nav", "aside", "form"]
    ):
        bad.decompose()

    return " ".join(soup.get_text(" ", strip=True).split())

# -------------------------
# 네트워크 fetch (quiet)
# -------------------------
QUIET_ARTICLE_STATUSES = {403, 404, 410, 429}

def _domain_of(link: str) -> str:
    try:
        return urlparse(link).netloc.lower() or "unknown"
    except Exception:
        return "unknown"

# 본문 fetch 시도 자체를 막고 RSS 스니펫만 쓰는 도메인
BLOCK_ARTICLE_DOMAINS = {
    "france24.com",
    "www.france24.com",
    "hani.co.kr",          # 한겨레는 본문 403/유료벽 케이스 많음 → 스니펫 위주
    "www.hani.co.kr",
}

async def fetch_text(session: aiohttp.ClientSession, url: str, is_rss: bool = False) -> str:
    if not url:
        return ""

    # 간단 재시도(조용히)
    for attempt in range(3):
        try:
            async with async_timeout.timeout(REQUEST_TIMEOUT):
                async with session.get(url, headers=UA, allow_redirects=True, ssl=False) as resp:
                    status = resp.status

                    # 기사 본문에서 흔한 차단/노이즈는 로그 없이 폴백
                    if (not is_rss) and (status in QUIET_ARTICLE_STATUSES):
                        return ""

                    if status >= 400:
                        # RSS 쪽만 최소 로그
                        if is_rss:
                            print(f"[RSS FAIL] {status} {url}")
                        return ""

                    text = await resp.text(errors="ignore")
                    return text or ""

        except Exception:
            if attempt < 2:
                await asyncio.sleep(0.6 * (attempt + 1))
                continue
            if is_rss:
                print(f"[RSS ERROR] {url}")
            return ""

    return ""

def rss_snippet(item) -> str:
    candidates: list[str] = []

    for tag in ("content:encoded", "content", "description", "summary", "dc:description"):
        node = item.find(tag)
        if node and node.text:
            candidates.append(node.text.strip())

    for node in item.find_all(True):
        name = (node.name or "").lower()
        if any(name.endswith(x) for x in ("description", "summary", "encoded")) and node.text:
            candidates.append(node.text.strip())

    return max(candidates, key=len, default="")

def parse_pubdate(item) -> datetime:
    txt = ""
    for tag in ("pubDate", "updated", "published"):
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
            link = (lnode.string or "").strip()
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
        rel = (ln.get("rel") or "").lower()
        typ = (ln.get("type") or "").lower()
        if href.startswith("http") and (rel == "alternate" or "html" in typ):
            return href
    return extract_link(item)

# -------------------------
# RSS 수집 (피드별 상한)
# -------------------------
async def search_news(session: aiohttp.ClientSession):
    items = []
    PER_FEED_CAP = 30

    for url in NEWS_FEEDS:
        xml = await fetch_text(session, url, is_rss=True)
        if not xml:
            continue
        try:
            soup = BeautifulSoup(xml, "xml")
        except Exception:
            # RSS 파싱 실패도 조용히 넘김
            continue

        feed_items = (soup.find_all("item") or []) + (soup.find_all("entry") or [])
        items.extend(feed_items[:PER_FEED_CAP])

    norm = []
    for it in items:
        try:
            dt = parse_pubdate(it)
        except Exception:
            dt = datetime.now(KST)
        norm.append((dt, it))

    norm.sort(key=lambda x: x[0], reverse=True)
    return [it for _, it in norm]

# -------------------------
# dedupe (48h)
# -------------------------
DEDUP_HOURS = 48
seen_fp_expiry: dict[str, datetime] = {}

def _fingerprint(title: str, link: str) -> str:
    base = (link or title or "").strip().lower()
    return str(hash(base))

def _purge_seen(now: datetime):
    for fp, exp in list(seen_fp_expiry.items()):
        if exp <= now:
            seen_fp_expiry.pop(fp, None)

# -------------------------
# AI 요약/번역 (EWS 전용) - GPT-5 Nano 고정
# -------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-5-nano"

_ai_client = AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

def looks_english(s: str) -> bool:
    if not s:
        return False
    letters = sum(c.isascii() and c.isalpha() for c in s)
    korean = sum("가" <= c <= "힣" for c in s)
    return letters > max(40, korean * 2)

async def summarize_translate_ews(title: str, body: str) -> str:
    if not _ai_client:
        return ""

    try:
        text = (body or "")[:4000]

        prompt = f"""
다음은 '주한 외국공관 철수/대피/폐쇄/여행경보 상향'과 관련될 수 있는 기사입니다.

요구사항:
1) 한국어로 자연스럽게 번역한 요약을 3줄 이내로 작성하세요.
2) 정말로 주한 공관 철수/대피/폐쇄/여행경보 상향이 핵심이면 마지막에
   '경보: ...' 형식으로 한 줄 더 붙이세요.
3) 불확실하면 '경보: 판단 보류'로 적으세요.
4) 과장하지 말고 기사 근거만 사용하세요.

제목: {title}
본문: {text}
""".strip()

        resp = await _ai_client.responses.create(
            model=OPENAI_MODEL,
            input=prompt,
            reasoning={"effort": "minimal"},
            text={"verbosity": "low"},
            max_output_tokens=320,
            temperature=0.2,
        )

        out_text = ""
        for item in getattr(resp, "output", []) or []:
            if getattr(item, "type", "") == "message":
                for c in getattr(item, "content", []) or []:
                    if getattr(c, "type", "") == "output_text":
                        out_text += c.text

        return out_text.strip()

    except Exception:
        return ""

# -------------------------
# 도메인 다양성 강제 선택
# -------------------------
def diversify_items(rss_items, cap_total=80, domain_cap=10):
    selected = []
    domain_count = defaultdict(int)

    for it in rss_items:
        link = pick_best_link(it)
        dom = _domain_of(link)

        if domain_count[dom] >= domain_cap:
            continue

        selected.append(it)
        domain_count[dom] += 1

        if len(selected) >= cap_total:
            break

    return selected

# -------------------------
# 핵심 1회 실행
# -------------------------
async def run_once():
    ews_events = []
    risk_events = []

    now = datetime.now(KST)
    _purge_seen(now)

    connector = aiohttp.TCPConnector(ssl=False)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            rss_items = await search_news(session)
        except Exception:
            rss_items = []

        rss_items = diversify_items(rss_items, cap_total=80, domain_cap=10)

        for item in rss_items:
            try:
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
                dom = _domain_of(link)

                # 차단 도메인은 본문 fetch 자체를 하지 않음(RSS 스니펫만 사용)
                if link and dom not in BLOCK_ARTICLE_DOMAINS:
                    article_html = await fetch_text(session, link, is_rss=False)
                    if article_html:
                        article_text = html_to_text(article_html)

                if len(article_text) < 200:
                    article_text = html_to_text(rss_snippet(item))
                if not article_text:
                    article_text = title
                if not article_text:
                    continue

                blob = f"{title}\n{article_text}"

                # ---- EWS ----
                if quick_filter(blob):
                    keys = extract_country_keys(title, article_text, top_k=2)
                    keys = [k for k in keys if k != "미상"]

                    summary = ""
                    if looks_english(blob):
                        summary = await summarize_translate_ews(title, article_text)

                    ews_events.append({
                        "pub": pub.isoformat(),
                        "title": title,
                        "link": link,
                        "countries": keys or ["미상"],
                        "summary": summary
                    })

                    seen_fp_expiry[fp] = now + timedelta(hours=DEDUP_HOURS)
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

            except Exception:
                continue

    return ews_events, risk_events





