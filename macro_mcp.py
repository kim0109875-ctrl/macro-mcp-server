"""
macro_mcp.py
미국 10년물 국채 금리 / 종목 정보 / 네마녀의 날(파생상품 동시 만기) 조회용 MCP 서버.

이 서버는 두 가지 역할을 동시에 합니다.
  1. MCP 도구 (/mcp) — 엔노이아 같은 AI 에이전트가 호출하는 표준 MCP 엔드포인트.
  2. 일반 REST API (/api/...) — "종목노트" 프론트엔드(index.html)가 fetch()로 바로
     불러다 쓰는 JSON 엔드포인트. 로직은 동일하고 출력 형식만 다릅니다.

     GET /api/treasury                        - 미국 10년물 금리
     GET /api/schedule                         - 네마녀의 날 등 시장 일정
     GET /api/stock/{ticker}                   - 종목 기본 정보 (에이전트/구버전 프론트용)
     GET /api/dashboard                        - 대시보드 화면 전체 (보유/관심/평가금액/차트/일정/뉴스)
     GET /api/stock/{ticker}/detail            - 종목상세 화면 전체
         ?range=1d|1w|1m|3m|1y&interval=1m|5m|15m|60m|1d  (캔들 조회 옵션)

  프론트엔드(index.html)는 원티드랩 Claude Design에서 만든 "연동용 스켈레톤"
  디자인의 실제 DOM(각 값 자리에 holdings[].name 같은 바인딩 키가 있는 span.key)에
  그대로 값을 채워 넣는 방식이라, 위 두 엔드포인트가 한 화면에 필요한 모든 필드를
  한 번에 묶어서 내려줍니다 (요청 수를 줄이기 위한 설계이며, 값 자체는 아래 함수들이
  실시간으로 계산합니다).

엔노이아의 "MCP 서버 추가" 화면은 URL 입력 방식(예: https://example.com/mcp)을
요구하므로, 아래처럼 "streamable-http" 모드로 실행해야 합니다.

로컬 실행:
  - stdio 방식 (로컬 프로세스로 직접 실행되는 host용, 엔노이아에는 해당 없음):
        python macro_mcp.py
  - HTTP 방식 (엔노이아 + 프론트엔드 API 겸용):
        python macro_mcp.py http
    로컬 접속 주소: http://127.0.0.1:8000/mcp, http://127.0.0.1:8000/api/...
    (PORT 환경변수를 지정하면 그 포트로 뜹니다. Render 등 클라우드 배포 시 자동으로 쓰입니다.)
"""

import os
import sys
import time
import threading
import datetime
from concurrent.futures import ThreadPoolExecutor

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse, FileResponse
import yfinance as yf

mcp = FastMCP("MacroMarketTools")

_INDEX_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
_KST = datetime.timezone(datetime.timedelta(hours=9))

# ---------------------------------------------------------------------------
# 짧은 TTL 응답 캐시
#
# 화면 하나를 열 때마다 yfinance를 실시간으로 다시 조회하면, 심사 기간처럼 짧은 시간에
# 여러 명이 몰릴 경우 (1) Render 무료/저사양 인스턴스에 부하가 걸리고 (2) 같은 IP에서
# 짧은 간격으로 요청이 반복되면 Yahoo Finance 쪽에서 레이트리밋을 걸 수 있습니다.
# 어차피 실시간 시세도 초 단위로 미묘하게 계속 바뀌는 값이라, 30초 정도는 캐시된 값을
# 보여줘도 사용자 체감상 차이가 없습니다. 그래서 무거운 조합 엔드포인트(대시보드/종목상세)
# 결과를 프로세스 메모리에 짧게 캐싱해서, 같은 내용을 몇 명이 동시에 보든 실제 yfinance
# 호출은 30초에 한 번만 나가도록 합니다. (Claude/LLM 호출과는 무관 — 순수 백엔드 성능 조치)
# ---------------------------------------------------------------------------
_cache_lock = threading.Lock()
_response_cache: dict[str, tuple[float, dict]] = {}


def _cached(key: str, ttl_seconds: float, builder):
    now = time.monotonic()
    with _cache_lock:
        hit = _response_cache.get(key)
        if hit and hit[0] > now:
            return hit[1]
    value = builder()
    with _cache_lock:
        _response_cache[key] = (now + ttl_seconds, value)
    return value

# 데모 포트폴리오 (원래 Claude Design 목업에 있던 보유 종목/관심 종목 구성 그대로).
# 보유 수량 / 평단가는 개인 계좌 정보라 실시간으로 가져올 수 없는 값이라 여기 고정값으로 둡니다.
# 대신 "현재가"는 실시간으로 조회해서 평가금액/손익은 항상 실제 시세 기준으로 계산됩니다.
DEMO_HOLDINGS = [
    {"ticker": "005930.KS", "name": "삼성전자", "market": "KOSPI", "qty": 120, "avg_price": 71300},
    {"ticker": "NVDA", "name": "엔비디아", "market": "NASDAQ", "qty": 32, "avg_price": 121.7},
    {"ticker": "360750.KS", "name": "TIGER 미국S&P500", "market": "KOSPI", "qty": 340, "avg_price": 18900},
    {"ticker": "005380.KS", "name": "현대차", "market": "KOSPI", "qty": 45, "avg_price": 262500},
    {"ticker": "AAPL", "name": "애플", "market": "NASDAQ", "qty": 14, "avg_price": 215.5},
    {"ticker": "091160.KS", "name": "KODEX 반도체", "market": "KOSPI", "qty": 12, "avg_price": 41000},
]
DEMO_WATCHLIST = [
    {"ticker": "035720.KS", "name": "카카오", "market": "KOSPI"},
    {"ticker": "TSLA", "name": "테슬라", "market": "NASDAQ"},
    {"ticker": "000660.KS", "name": "SK하이닉스", "market": "KOSPI"},
    {"ticker": "ASML", "name": "ASML", "market": "NASDAQ"},
    {"ticker": "069500.KS", "name": "KODEX 200", "market": "KOSPI"},
]
# 계좌 연동은 실제 증권사 오픈API 인증이 필요한 영역이라 이번 범위에는 없습니다.
# 화면에 빈 값이 뜨지 않도록 데모 값 하나만 고정으로 둡니다 (실시간 데이터 아님).
DEMO_ACCOUNTS = [{"broker": "데모 증권사 (연동 준비 중)", "connected": False}]

_NEWS_POSITIVE_KEYWORDS = ["급등", "상승", "호재", "최대", "기대", "신고가", "흑자", "확대", "강세",
                           "성장", "개선", "상회", "돌파", "기록", "beat", "surge", "rally", "record"]
_NEWS_NEGATIVE_KEYWORDS = ["급락", "하락", "악재", "적자", "부진", "우려", "리스크", "약세", "축소",
                           "경고", "불안", "위기", "부담", "miss", "plunge", "slump", "warn", "cut"]


def _classify_news_impact(title: str) -> str:
    """뉴스 제목의 간단한 키워드 기반 호재/악재 추정. 정교한 감성분석이 아니라
    제목에 등장하는 단어만 보는 휴리스틱이라 참고용입니다."""
    if not title:
        return "neutral"
    t = title.lower()
    pos = any(k.lower() in t for k in _NEWS_POSITIVE_KEYWORDS)
    neg = any(k.lower() in t for k in _NEWS_NEGATIVE_KEYWORDS)
    if pos and not neg:
        return "positive"
    if neg and not pos:
        return "negative"
    return "neutral"


# ---------------------------------------------------------------------------
# 기본 데이터 조회 로직
# ---------------------------------------------------------------------------

def _fetch_treasury_yield() -> dict:
    try:
        tnx = yf.Ticker("^TNX")
        hist = tnx.history(period="5d")
        if hist.empty or len(hist) < 2:
            return {"error": "금리 데이터를 가져오지 못했습니다 (조회된 데이터가 없습니다)."}
        raw_latest = float(hist["Close"].iloc[-1])
        raw_prev = float(hist["Close"].iloc[-2])
        # Yahoo Finance ^TNX 표기가 소스에 따라 실제값의 10배로 오기도 해서
        # 20%를 넘으면(=미국 10년물 역사상 없던 수치) 10배 표기로 보고 보정합니다.
        scale = 10 if raw_latest > 20 else 1
        latest_yield = raw_latest / scale
        prev_yield = raw_prev / scale
        diff = latest_yield - prev_yield
        trend = "상승" if diff > 0 else ("하락" if diff < 0 else "보합")
        return {
            "yield_pct": round(latest_yield, 2),
            "prev_yield_pct": round(prev_yield, 2),
            "diff_pct": round(diff, 2),
            "trend": trend,
        }
    except Exception as e:
        return {"error": f"금리 데이터를 가져오는데 실패했습니다: {e}"}


def _is_witching_week(d: datetime.date) -> bool:
    return d.month in (3, 6, 9, 12) and 15 <= d.day <= 21


def _is_witching_month(d: datetime.date) -> bool:
    return d.month in (3, 6, 9, 12)


def _fetch_market_schedule() -> dict:
    today = datetime.date.today()
    is_witching_month = _is_witching_month(today)
    is_witching_week = _is_witching_week(today)
    if is_witching_week:
        level = "high"
        message = ("이번 주는 네마녀의 날(선물/옵션 동시 만기일)이 있는 주간입니다. "
                   "장중 변동성 극대화 및 수급 왜곡에 주의해야 합니다.")
    elif is_witching_month:
        level = "normal"
        message = ("이번 달은 네마녀의 날이 있는 달이지만, 이번 주는 만기 주간이 아닙니다. "
                   "일반적인 수준의 변동성을 예상합니다.")
    else:
        level = "normal"
        message = "특이 파생상품 만기 일정이 없는 평이한 주간입니다."
    return {
        "date": today.strftime("%Y-%m-%d"),
        "is_witching_week": is_witching_week,
        "is_witching_month": is_witching_month,
        "volatility_level": level,
        "message": message,
    }


def _market_session() -> dict:
    """아주 단순한 국내장 기준 개장 여부 판단 (공휴일 캘린더는 반영하지 않습니다)."""
    now = datetime.datetime.now(_KST)
    is_weekday = now.weekday() < 5
    open_t = now.replace(hour=9, minute=0, second=0, microsecond=0)
    close_t = now.replace(hour=15, minute=30, second=0, microsecond=0)
    is_open = is_weekday and open_t <= now <= close_t
    label = "장중" if is_open else ("장 마감" if is_weekday else "휴장일")
    return {"sessionLabel": label, "asOf": now.isoformat(), "isOpen": is_open}


def _get_info_with_retry(t, tries: int = 2) -> dict:
    """yfinance의 .info는 history()보다 훨씬 자주 실패/차단되는 별도 엔드포인트를 씁니다
    (야후 쪽 크럼(crumb) 인증이 간헐적으로 풀리는 경우가 있음). 한 번 비어서 돌아와도
    바로 포기하지 않고 한 번 더 시도합니다 (그래도 실패하면 빈 dict — 화면에서는 해당
    필드만 비어 보이고 나머지는 정상 표시됩니다)."""
    for _ in range(tries):
        try:
            info = t.info
            if info:
                return info
        except Exception:
            pass
    return {}


def _fetch_stock_info(ticker: str) -> dict:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")
        if hist.empty:
            return {
                "error": f"'{ticker}' 종목 데이터를 찾을 수 없습니다. 티커 형식을 확인해 주세요 "
                         f"(한국 주식은 .KS/.KQ 필요)."
            }
        info = _get_info_with_retry(t)

        name = info.get("longName") or info.get("shortName") or ticker
        # .info가 실패해서 currency가 비어있을 때, 국내(.KS/.KQ) 티커를 외화로 잘못 취급하면
        # 포트폴리오 평가금액에 환율(USD/KRW)이 이중으로 곱해져 손익이 수십~수백 배로
        # 부풀려지는 심각한 계산 오류가 생깁니다. 티커 접미사로 안전하게 기본값을 채웁니다.
        currency = info.get("currency") or ("KRW" if ticker.endswith((".KS", ".KQ")) else "")
        latest_close = float(hist["Close"].iloc[-1])
        prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else info.get("previousClose")
        volume = int(hist["Volume"].iloc[-1]) if "Volume" in hist.columns and len(hist) else info.get("volume")

        change = None
        change_pct = None
        if prev_close:
            change = latest_close - prev_close
            change_pct = (change / prev_close) * 100

        try:
            raw_news = t.news or []
        except Exception:
            raw_news = []
        news_items = []
        for item in raw_news[:6]:
            content = item.get("content") if isinstance(item, dict) else None
            title = None
            link = None
            published = None
            source = None
            if isinstance(content, dict):
                title = content.get("title")
                published = content.get("pubDate")
                provider = content.get("provider")
                if isinstance(provider, dict):
                    source = provider.get("displayName")
                canonical = content.get("canonicalUrl")
                if isinstance(canonical, dict):
                    link = canonical.get("url")
            if not title and isinstance(item, dict):
                title = item.get("title")
                link = link or item.get("link")
            if title:
                news_items.append({
                    "title": title, "link": link, "source": source, "publishedAt": published,
                    "impact": _classify_news_impact(title),
                })

        return {
            "ticker": ticker, "name": name, "currency": currency,
            "market": info.get("exchange") or ("KOSPI" if ticker.endswith(".KS") else
                     ("KOSDAQ" if ticker.endswith(".KQ") else "")),
            "price": round(latest_close, 2),
            "prev_close": round(prev_close, 2) if prev_close else None,
            "change": round(change, 2) if change is not None else None,
            "change_pct": round(change_pct, 2) if change_pct is not None else None,
            "volume": volume,
            "market_cap": info.get("marketCap"),
            "per": info.get("trailingPE"),
            "pbr": info.get("priceToBook"),
            "eps": info.get("trailingEps"),
            "week52_high": info.get("fiftyTwoWeekHigh"),
            "week52_low": info.get("fiftyTwoWeekLow"),
            "target_mean": info.get("targetMeanPrice"),
            "target_high": info.get("targetHighPrice"),
            "target_low": info.get("targetLowPrice"),
            "analyst_count": info.get("numberOfAnalystOpinions"),
            "news": news_items,
        }
    except Exception as e:
        return {"error": f"'{ticker}' 정보를 가져오는데 실패했습니다: {e}"}


_fx_cache = {"rate": None, "ts": None}


def _fetch_usd_krw_rate() -> float | None:
    now = datetime.datetime.now()
    if _fx_cache["rate"] and _fx_cache["ts"] and (now - _fx_cache["ts"]).total_seconds() < 300:
        return _fx_cache["rate"]
    try:
        hist = yf.Ticker("KRW=X").history(period="1d")
        if hist.empty:
            return _fx_cache["rate"]
        rate = float(hist["Close"].iloc[-1])
        _fx_cache["rate"] = rate
        _fx_cache["ts"] = now
        return rate
    except Exception:
        return _fx_cache["rate"]


_RANGE_TO_PERIOD = {"1d": "5d", "1w": "5d", "1m": "1mo", "3m": "3mo", "1y": "1y"}
_INTRADAY_INTERVALS = {"1m", "5m", "15m", "60m"}


def _fetch_candles(ticker: str, range_key: str = "3m", interval: str = "1d") -> list[dict]:
    """캔들 조회. range: 1d/1w/1m/3m/1y, interval: 1m/5m/15m/60m/1d.
    분봉은 Yahoo Finance 정책상 최근 며칠치만 제공되므로, 요청한 조합이 지원 범위를
    벗어나면 조용히 일봉으로 내려받습니다 (화면이 비는 것보다 낫다는 판단)."""
    period = _RANGE_TO_PERIOD.get(range_key, "3mo")
    yf_interval = interval if interval in _INTRADAY_INTERVALS else "1d"
    if yf_interval != "1d":
        # 분봉은 짧은 기간만 지원되므로 range와 무관하게 최근 5일로 제한
        period = "5d" if yf_interval in ("1m", "5m", "15m") else "1mo"

    t = yf.Ticker(ticker)
    try:
        hist = t.history(period=period, interval=yf_interval)
    except Exception:
        hist = None
    if hist is None or hist.empty:
        # 분봉 실패 시 일봉으로 재시도
        if yf_interval != "1d":
            try:
                hist = t.history(period=_RANGE_TO_PERIOD.get(range_key, "3mo"), interval="1d")
            except Exception:
                hist = None
        if hist is None or hist.empty:
            return []

    closes = hist["Close"]
    ma20 = closes.rolling(20).mean()
    ma60 = closes.rolling(60).mean()
    ma120 = closes.rolling(120).mean()

    # 화면에 너무 많은 캔들이 몰리지 않도록 최근 120개로 제한
    display = hist.tail(120)

    def _r(v, d=2):
        try:
            if v is None or v != v:
                return None
            return round(float(v), d)
        except Exception:
            return None

    fmt = "%H:%M" if yf_interval != "1d" else "%m/%d"
    candles = []
    for idx, row in display.iterrows():
        candles.append({
            "t": idx.strftime(fmt), "o": _r(row.get("Open")), "h": _r(row.get("High")),
            "l": _r(row.get("Low")), "c": _r(row.get("Close")),
            "v": int(row["Volume"]) if "Volume" in row and row["Volume"] == row["Volume"] else None,
            "ma20": _r(ma20.get(idx)), "ma60": _r(ma60.get(idx)), "ma120": _r(ma120.get(idx)),
        })
    return candles


def _fetch_recent_moves(ticker: str, days: int = 7) -> list[dict]:
    t = yf.Ticker(ticker)
    try:
        hist = t.history(period="3mo")
    except Exception:
        hist = None
    if hist is None or hist.empty:
        return []
    closes = hist["Close"].tail(days + 1)
    rows = list(closes.items())
    out = []
    for i in range(1, len(rows)):
        idx, close = rows[i]
        prev = rows[i - 1][1]
        pct = ((close - prev) / prev * 100) if prev else None
        note = "-"
        if pct is not None:
            if pct >= 3:
                note = "급등"
            elif pct <= -3:
                note = "급락"
        out.append({"date": idx.strftime("%m/%d"), "close": round(float(close), 2),
                    "changeRate": round(pct, 2) if pct is not None else None, "note": note})
    out.reverse()
    return out


def _compute_trend(candles: list[dict], quote: dict) -> dict:
    """최근 7일 추세를 실제 수치로 판정합니다 (애널리스트 코멘트가 아니라 규칙 기반).
    가능한 사실만 근거로 사용하고, 계산 불가능한 항목은 조용히 건너뜁니다."""
    window = 7
    daily = candles[-window:] if len(candles) >= window else candles[:]
    reasons = []
    verdict = "횡보"
    strength = "보통"

    closes = [c["c"] for c in daily if c.get("c") is not None]
    if len(closes) >= 2:
        pct = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] else 0
        if pct > 1.5:
            verdict = "상승"
        elif pct < -1.5:
            verdict = "하락"
        else:
            verdict = "횡보"
        strength = "강함" if abs(pct) >= 5 else ("보통" if abs(pct) >= 1.5 else "약함")
        sign = "+" if pct >= 0 else ""
        reasons.append({"tone": "positive" if pct > 0 else ("negative" if pct < 0 else "neutral"),
                        "text": f"최근 7일간 {sign}{pct:.1f}% {'상승' if pct>0 else ('하락' if pct<0 else '보합')}"})

    last = candles[-1] if candles else None
    if last and last.get("c") is not None and last.get("ma20") is not None:
        above = last["c"] >= last["ma20"]
        reasons.append({"tone": "positive" if above else "negative",
                        "text": f"현재가가 20일 이동평균선을 {'상회' if above else '하회'}"})

    vols = [c["v"] for c in candles[-20:] if c.get("v") is not None]
    if vols and candles and candles[-1].get("v") is not None and len(vols) >= 5:
        avg_vol = sum(vols) / len(vols)
        if avg_vol:
            ratio = candles[-1]["v"] / avg_vol * 100
            reasons.append({"tone": "neutral",
                            "text": f"최근 거래량이 20일 평균 대비 {ratio:.0f}% 수준"})

    week_high = quote.get("week52_high")
    week_low = quote.get("week52_low")
    price = quote.get("price")
    if week_high and price:
        gap = (week_high - price) / week_high * 100
        if gap <= 3:
            reasons.append({"tone": "positive", "text": "52주 신고가 근접"})
    if week_low and price and week_low > 0:
        gap = (price - week_low) / week_low * 100
        if gap <= 3:
            reasons.append({"tone": "negative", "text": "52주 최저가 근접"})

    return {
        "windowDays": window, "verdict": verdict, "strength": strength,
        "reasons": reasons[:4], "daily": daily,
    }


def _fetch_stock_events(ticker: str) -> list[dict]:
    """해당 종목의 실적발표일/배당락일 등 일정. yfinance 캘린더 데이터는 제공 여부가
    종목마다 달라서 실패해도 조용히 빈 리스트를 돌려줍니다."""
    events = []
    try:
        t = yf.Ticker(ticker)
        cal = t.calendar
        if isinstance(cal, dict):
            earnings_dates = cal.get("Earnings Date")
            if isinstance(earnings_dates, list):
                for d in earnings_dates[:2]:
                    events.append({"date": str(d), "title": "실적 발표(예정)", "meta": ticker, "type": "stock"})
    except Exception:
        pass
    return events


def _compute_dday(date_str: str) -> str | None:
    try:
        d = datetime.date.fromisoformat(date_str[:10])
        delta = (d - datetime.date.today()).days
        if delta == 0:
            return "D-DAY"
        return f"D{'+' if delta>0 else ''}{delta}"
    except Exception:
        return None


# ---------------------------------------------------------------------------
# 시장 매크로 이벤트 캘린더 (하드코딩, 2026년 기준)
#
# FOMC/한국은행 회의 일정은 각 기관이 전년도에 미리 연간 일정을 공표하기 때문에
# (예: 한국은행은 매년 10월 말 다음 해 일정을 발표) 매번 조회하는 에이전트를 두기보다
# 여기 고정값으로 관리하고 연 1회 정도 갱신하는 편이 훨씬 가볍고, 페이지 조회 경로에
# 추가 API/에이전트 호출이 없어 비용·지연 부담도 없습니다.
#
# 중요도(importance)는 사용자가 정리한 분류 기준을 그대로 따릅니다.
#   상(high)   : FOMC, 미국 대선·중간선거, 빅테크(개별 종목) 실적, MSCI 리밸런싱,
#                네마녀의 날, 한국 기업 실적시즌
#   중(medium) : 미국 고용보고서(비농업고용지표), 한국은행 금융통화위원회
#   하(low)    : CES, MWC, JP모건 헬스케어 컨퍼런스, 한국 수출입 동향
#
# 출처: federalreserve.gov/monetarypolicy/fomccalendars.htm (2026 FOMC 일정),
#       bok.or.kr 2026년 금융통화위원회 정기회의 일정(2025-10-30 발표), CES.tech,
#       mwcbarcelona.com, jpmorgan.com/events-conferences (2026-09 기준 확인).
# ---------------------------------------------------------------------------

_IMPORTANCE_LABEL = {"high": "상", "medium": "중", "low": "하"}


def _importance_meta(level: str) -> dict:
    return {"importance": level, "importanceLabel": _IMPORTANCE_LABEL.get(level, "")}


# 연준 FOMC 정례회의 — 이틀 회의 중 정책 결정/기자회견이 있는 둘째 날 기준
_FOMC_2026 = ["2026-01-28", "2026-03-18", "2026-04-29", "2026-06-17",
              "2026-07-29", "2026-09-16", "2026-10-28", "2026-12-09"]

# 한국은행 금융통화위원회 통화정책방향 결정회의
_BOK_2026 = ["2026-01-15", "2026-02-26", "2026-04-10", "2026-05-28",
             "2026-07-16", "2026-08-27", "2026-10-22", "2026-11-26"]

# 미국 중간선거 (헌법상 짝수해 11월 첫 월요일 다음 화요일로 고정 — 2026년은 11/3)
_US_MIDTERM_ELECTION_2026 = "2026-11-03"

# 대형 컨퍼런스 (정확한 날짜는 매년 주최측이 공지 — 아래는 2026년 확정 일정)
_CES_2026 = ("2026-01-06", "2026-01-09")
_MWC_2026 = ("2026-03-02", "2026-03-05")
_JPM_HEALTHCARE_2026 = ("2026-01-12", "2026-01-15")

# 국내 기업 실적시즌 (분기마다 반복되는 관행적 구간)
_KR_EARNINGS_SEASON_2026 = [
    ("2026-01-08", "2026-02-13", "2025년 4분기·연간 실적시즌"),
    ("2026-04-08", "2026-05-15", "2026년 1분기 실적시즌"),
    ("2026-07-08", "2026-08-14", "2026년 2분기 실적시즌"),
    ("2026-10-08", "2026-11-14", "2026년 3분기 실적시즌"),
]


def _last_weekday_of_month(year: int, month: int) -> datetime.date:
    if month == 12:
        d = datetime.date(year, 12, 31)
    else:
        d = datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)
    while d.weekday() >= 5:  # 토/일이면 직전 평일로
        d -= datetime.timedelta(days=1)
    return d


def _msci_rebalance_dates(year: int) -> list[str]:
    """MSCI 정기 리뷰(2·5·8·11월) 효력 발생일 = 해당 월 마지막 영업일. 5월/11월은
    반기 리뷰(변경폭이 큰 편), 2월/8월은 분기 리뷰입니다. 사용자 분류 기준상 넷 다
    '상' 등급으로 취급합니다."""
    return [_last_weekday_of_month(year, m).isoformat() for m in (2, 5, 8, 11)]


def _us_jobs_report_dates(year: int) -> list[str]:
    """미국 고용보고서(비농업고용지표)는 관례상 매월 첫째 주 금요일에 발표됩니다
    (드물게 하루 이틀 밀리는 경우가 있어 참고용 근사치입니다)."""
    out = []
    for month in range(1, 13):
        d = datetime.date(year, month, 1)
        while d.weekday() != 4:
            d += datetime.timedelta(days=1)
        out.append(d.isoformat())
    return out


def _collect_macro_events(days_ahead: int = 45) -> list[dict]:
    """상/중/하 중요도가 매겨진 매크로 일정을 모아 dDay와 함께 돌려줍니다. 실시간 조회가
    아니라 위의 하드코딩/연간 규칙 기반이며, 오늘부터 days_ahead일 이내 일정만 남깁니다."""
    today = datetime.date.today()
    horizon = today + datetime.timedelta(days=days_ahead)
    raw = []

    for d in _FOMC_2026:
        raw.append({"date": d, "title": "FOMC 정례회의 (미국 금리 결정)", "meta": "미국 연준",
                    "type": "market", **_importance_meta("high")})
    for d in _BOK_2026:
        raw.append({"date": d, "title": "한국은행 금융통화위원회", "meta": "한국은행",
                    "type": "market", **_importance_meta("medium")})
    raw.append({"date": _US_MIDTERM_ELECTION_2026, "title": "미국 중간선거", "meta": "정치 이벤트",
               "type": "market", **_importance_meta("high")})

    for y in (today.year, today.year + 1):
        for d in _msci_rebalance_dates(y):
            raw.append({"date": d, "title": "MSCI 지수 리밸런싱", "meta": "글로벌 수급 이벤트",
                       "type": "market", **_importance_meta("high")})
        for d in _us_jobs_report_dates(y):
            raw.append({"date": d, "title": "미국 고용보고서 (비농업고용지표)", "meta": "미국 경제지표",
                       "type": "market", **_importance_meta("medium")})

    ces_s, ces_e = _CES_2026
    raw.append({"date": ces_s, "title": "CES 개막", "meta": f"~{ces_e[5:]} · IT 전시회",
               "type": "market", **_importance_meta("low")})
    mwc_s, mwc_e = _MWC_2026
    raw.append({"date": mwc_s, "title": "MWC 바르셀로나 개막", "meta": f"~{mwc_e[5:]} · 통신 컨퍼런스",
               "type": "market", **_importance_meta("low")})
    jpm_s, jpm_e = _JPM_HEALTHCARE_2026
    raw.append({"date": jpm_s, "title": "JP모건 헬스케어 컨퍼런스 개막", "meta": f"~{jpm_e[5:]} · 헬스케어",
               "type": "market", **_importance_meta("low")})
    for start, end, label in _KR_EARNINGS_SEASON_2026:
        raw.append({"date": start, "title": f"{label} 시작", "meta": f"~{end[5:]}",
                   "type": "market", **_importance_meta("high")})

    # 네마녀의 날 (기존 판정 로직 재사용, 당월 세번째 금요일)
    sched = _fetch_market_schedule()
    if sched.get("is_witching_month"):
        for day in range(15, 22):
            try:
                d = today.replace(day=day)
                if d.weekday() == 4:
                    raw.append({"date": d.isoformat(), "title": "네마녀의 날 (선물·옵션 동시만기)",
                               "meta": "시장 공통 일정", "type": "market", **_importance_meta("high")})
                    break
            except ValueError:
                pass

    out, seen = [], set()
    for e in raw:
        try:
            d = datetime.date.fromisoformat(e["date"])
        except Exception:
            continue
        if not (today <= d <= horizon):
            continue
        key = (e["date"], e["title"])
        if key in seen:
            continue
        seen.add(key)
        e["dDay"] = _compute_dday(e["date"])
        out.append(e)
    out.sort(key=lambda e: e["date"])
    return out


# ---------------------------------------------------------------------------
# 화면 단위 조합 함수 (프론트엔드가 한 번의 fetch로 받는 대시보드 / 종목상세 데이터)
# ---------------------------------------------------------------------------

def _fetch_holding_row(h: dict, fx: float | None, info: dict | None = None) -> dict:
    info = info if info is not None else _fetch_stock_info(h["ticker"])
    row = {"ticker": h["ticker"], "name": h["name"], "market": h["market"], "quantity": h["qty"]}
    if "error" in info:
        row["error"] = info["error"]
        return row
    price = info["price"]
    currency = info["currency"]
    eval_amt = price * h["qty"]
    cost_amt = h["avg_price"] * h["qty"]
    # currency가 비어있는 경우(.info 조회 실패 등)는 원화로 잘못 이중환산되지 않도록
    # "명확히 원화가 아니라고 확인된 경우"에만 외화로 취급합니다 (아래 두 계산 모두 동일 기준).
    is_foreign = bool(currency) and currency.upper() != "KRW"
    eval_krw, cost_krw, fx_ok = eval_amt, cost_amt, True
    if is_foreign:
        if fx:
            eval_krw, cost_krw = eval_amt * fx, cost_amt * fx
        else:
            fx_ok = False
    pnl = eval_amt - cost_amt
    pnl_pct = (pnl / cost_amt * 100) if cost_amt else None
    day_pnl_krw = (info["change"] * h["qty"] * (fx if (is_foreign and fx) else 1)) \
        if info.get("change") is not None and fx_ok else 0
    row.update({
        "price": price, "currency": currency, "changeRate": info["change_pct"],
        "avgPrice": h["avg_price"], "marketValue": round(eval_amt, 0),
        "marketValueKrw": round(eval_krw, 0) if fx_ok else None,
        "costKrw": round(cost_krw, 0) if fx_ok else None,
        "profitLoss": round(pnl, 0), "rate": round(pnl_pct, 2) if pnl_pct is not None else None,
        "dayProfitLossKrw": round(day_pnl_krw, 0) if fx_ok else 0,
        "fxOk": fx_ok,
    })
    return row


def _watchlist_row(w: dict) -> dict:
    info = _fetch_stock_info(w["ticker"])
    row = {"ticker": w["ticker"], "name": w["name"], "market": w["market"]}
    if "error" in info:
        row["error"] = info["error"]
    else:
        row["changeRate"] = info["change_pct"]
        row["price"] = info["price"]
    return row


def _fetch_dashboard() -> dict:
    fx = _fetch_usd_krw_rate()
    # 보유/관심 종목이 10개 넘게 순차 조회되면 Render 무료 티어에서 체감 지연이 커서
    # 스레드풀로 병렬 조회합니다 (yfinance 자체는 동기 라이브러리라 I/O만 병렬화).
    with ThreadPoolExecutor(max_workers=8) as ex:
        holding_infos = list(ex.map(lambda h: _fetch_stock_info(h["ticker"]), DEMO_HOLDINGS))
        watchlist = list(ex.map(_watchlist_row, DEMO_WATCHLIST))
    holdings = [_fetch_holding_row(h, fx, info) for h, info in zip(DEMO_HOLDINGS, holding_infos)]

    ok_holdings = [h for h in holdings if "error" not in h]
    total_eval = sum(h["marketValueKrw"] for h in ok_holdings if h.get("fxOk"))
    total_cost = sum(h["costKrw"] for h in ok_holdings if h.get("fxOk"))
    total_day = sum(h["dayProfitLossKrw"] for h in ok_holdings if h.get("fxOk"))
    for h in ok_holdings:
        h["weight"] = round(h["marketValueKrw"] / total_eval * 100, 1) if total_eval and h.get("fxOk") else None

    total_pnl = total_eval - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost else None
    prev_eval = total_eval - total_day
    day_pct = (total_day / prev_eval * 100) if prev_eval else None

    # 포트폴리오 가치 추이 (최근 3개월, 일 단위). 각 보유종목의 일별 종가 × 수량을 합산합니다.
    # 환율은 현재 시점 환율을 과거에도 동일 적용하는 근사치입니다 (과거 환율 API는 별도 연동 필요).
    series = []
    try:
        def _hist_close(h):
            try:
                hist = yf.Ticker(h["ticker"]).history(period="3mo")
                return h["ticker"], (hist["Close"] if not hist.empty else None)
            except Exception:
                return h["ticker"], None
        with ThreadPoolExecutor(max_workers=8) as ex:
            hist_map = dict(ex.map(_hist_close, DEMO_HOLDINGS))
        # 공통 날짜 축: 삼성전자(국내) 기준 날짜 사용
        base_dates = None
        for h in DEMO_HOLDINGS:
            s = hist_map.get(h["ticker"])
            if s is not None and len(s) > 0:
                base_dates = s.index
                break
        if base_dates is not None:
            for date in base_dates:
                total = 0.0
                any_ok = False
                for h in DEMO_HOLDINGS:
                    s = hist_map.get(h["ticker"])
                    if s is None or date not in s.index:
                        continue
                    price = float(s.loc[date])
                    cur = None
                    for hh in ok_holdings:
                        if hh["ticker"] == h["ticker"]:
                            cur = hh["currency"]
                            break
                    val = price * h["qty"]
                    if cur and cur.upper() != "KRW" and fx:
                        val *= fx
                    total += val
                    any_ok = True
                if any_ok:
                    series.append({"date": date.strftime("%m/%d"), "value": round(total, 0)})
    except Exception:
        series = []

    # 매크로 일정(FOMC/한국은행/MSCI/실적시즌 등, 하드코딩 기반)과 보유종목 개별 실적일정을
    # 합쳐서 중요도(상/중/하)를 매긴 뒤, 이번 주(7일 이내) 건수와 위젯에 보여줄 상위 목록을 뽑습니다.
    macro_events = _collect_macro_events()
    with ThreadPoolExecutor(max_workers=4) as ex:
        stock_events_lists = list(ex.map(lambda h: _fetch_stock_events(h["ticker"]), DEMO_HOLDINGS[:3]))
    stock_events = []
    for h, evs in zip(DEMO_HOLDINGS[:3], stock_events_lists):
        for ev in evs:
            ev["dDay"] = _compute_dday(ev["date"])
            ev["title"] = f"{h['name']} {ev['title']}"
            ev.update(_importance_meta("high"))  # 개별 종목 실적발표는 상(high)로 취급
            stock_events.append(ev)

    all_events = macro_events + stock_events
    today = datetime.date.today()
    week_count = sum(
        1 for e in all_events
        if 0 <= (datetime.date.fromisoformat(e["date"][:10]) - today).days <= 7
    )
    _rank = {"high": 0, "medium": 1, "low": 2}
    upcoming_events = sorted(all_events, key=lambda e: (e["date"], _rank.get(e.get("importance"), 3)))[:6]

    # 보유 종목들의 최신 뉴스를 모아 최대 5건 (이미 조회해 둔 holding_infos 재사용)
    news = []
    for info in holding_infos[:3]:
        for n in info.get("news", [])[:2]:
            news.append(n)
    news = news[:5]

    return {
        "holdings": holdings, "watchlist": watchlist, "accounts": DEMO_ACCOUNTS,
        "sync": {"updatedAt": datetime.datetime.now(_KST).isoformat()},
        "notifications": {"unreadCount": 0},
        "market": _market_session(),
        "portfolio": {
            "principal": round(total_cost, 0), "marketValue": round(total_eval, 0),
            "profitLoss": round(total_pnl, 0),
            "profitRate": round(total_pnl_pct, 2) if total_pnl_pct is not None else None,
            "dayProfitLoss": round(total_day, 0),
            "dayProfitRate": round(day_pct, 2) if day_pct is not None else None,
            "stockCount": len(ok_holdings), "series": series,
        },
        "accountsCount": len(DEMO_ACCOUNTS),
        "events": {"weekCount": week_count, "upcoming": upcoming_events},
        "news": news,
    }


def _fetch_stock_detail(ticker: str, range_key: str = "3m", interval: str = "1d") -> dict:
    base = _fetch_stock_info(ticker)
    if "error" in base:
        return base

    candles = _fetch_candles(ticker, range_key, interval)
    quote = {
        "price": base["price"], "change": base["change"], "changeRate": base["change_pct"],
        "open": candles[-1]["o"] if candles else None,
        "high": candles[-1]["h"] if candles else None,
        "low": candles[-1]["l"] if candles else None,
        "volume": base["volume"], "asOf": datetime.datetime.now(_KST).isoformat(),
        "week52_high": base.get("week52_high"), "week52_low": base.get("week52_low"),
    }
    trend = _compute_trend(candles, quote) if candles else \
        {"windowDays": 7, "verdict": "-", "strength": "-", "reasons": [], "daily": []}
    if not trend.get("daily"):
        trend["daily"] = _fetch_recent_moves(ticker)

    is_holding = any(h["ticker"] == ticker for h in DEMO_HOLDINGS)
    position = None
    if is_holding:
        fx = _fetch_usd_krw_rate()
        h = next(h for h in DEMO_HOLDINGS if h["ticker"] == ticker)
        row = _fetch_holding_row(h, fx)
        if "error" not in row:
            position = {
                "profitLoss": row["profitLoss"], "profitRate": row["rate"],
                "marketValue": row["marketValue"], "quantity": row["quantity"],
                "avgPrice": row["avgPrice"], "weight": None,
            }

    consensus = None
    if base.get("target_mean"):
        upside = ((base["target_mean"] - base["price"]) / base["price"] * 100) if base["price"] else None
        consensus = {
            "reportCount": base.get("analyst_count"), "targetAvg": base.get("target_mean"),
            "targetHigh": base.get("target_high"), "targetLow": base.get("target_low"),
            "upsideRate": round(upside, 2) if upside is not None else None,
        }

    events = _fetch_stock_events(ticker)
    for ev in events:
        ev["dDay"] = _compute_dday(ev["date"])

    return {
        **base,
        "stock": {"name": base["name"], "ticker": base["ticker"], "market": base.get("market", ""),
                  "isHolding": is_holding, "marketCap": base.get("market_cap"),
                  "high52w": base.get("week52_high")},
        "quote": quote, "candles": candles, "trend": trend, "position": position,
        "consensus": consensus, "events": events,
    }


def _fetch_portfolio() -> dict:
    """구버전 호환용 (기존 /api/portfolio). 새 화면은 /api/dashboard를 사용합니다."""
    d = _fetch_dashboard()
    return {
        "holdings": d["holdings"], "watchlist": d["watchlist"],
        "total_eval_krw": d["portfolio"]["marketValue"], "total_cost_krw": d["portfolio"]["principal"],
        "total_pnl_krw": d["portfolio"]["profitLoss"], "total_pnl_pct": d["portfolio"]["profitRate"],
    }


# ---------------------------------------------------------------------------
# MCP 도구 (엔노이아 에이전트용 — 사람이 읽는 문장으로 반환)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_treasury_yield() -> str:
    """미국 10년물 국채 금리(^TNX)의 최신 수치를 가져와서 밸류에이션 판단의 근거로 사용합니다."""
    data = _fetch_treasury_yield()
    if "error" in data:
        return data["error"]
    return (f"[Macro Data] 미국 10년물 국채 금리: {data['yield_pct']:.2f}% "
           f"(전일 대비 {data['trend']}, {data['diff_pct']:+.2f}%p)")


@mcp.tool()
def get_stock_info(ticker: str) -> str:
    """지정한 종목의 현재가/등락률/거래량, 주요 재무 지표(PER, PBR, 시가총액, EPS),
    최근 관련 뉴스를 조회합니다.

    ticker 형식:
      - 미국 주식: 티커 그대로 (예: AAPL, NVDA, TSLA)
      - 한국 코스피: 종목코드 뒤에 '.KS' (예: 삼성전자 005930.KS)
      - 한국 코스닥: 종목코드 뒤에 '.KQ' (예: 에코프로 086520.KQ)
    """
    data = _fetch_stock_info(ticker)
    if "error" in data:
        return data["error"]

    lines = [f"[{data['name']} ({data['ticker']})]"]
    price_line = f"현재가: {data['price']:,.2f} {data['currency']}".strip()
    if data["change"] is not None:
        sign = "+" if data["change"] >= 0 else ""
        price_line += f" ({sign}{data['change']:,.2f}, {sign}{data['change_pct']:.2f}%, 전일 종가 대비)"
    lines.append(price_line)
    if data["volume"]:
        lines.append(f"거래량: {data['volume']:,}")
    fin_parts = []
    if data["market_cap"]:
        fin_parts.append(f"시가총액: {data['market_cap']:,.0f} {data['currency']}".strip())
    if data["per"]:
        fin_parts.append(f"PER: {data['per']:.2f}")
    if data["pbr"]:
        fin_parts.append(f"PBR: {data['pbr']:.2f}")
    if data["eps"]:
        fin_parts.append(f"EPS: {data['eps']:.2f}")
    lines.append("재무 지표 - " + ", ".join(fin_parts) if fin_parts else "재무 지표: 조회 불가 (데이터 제공 안 됨)")
    if data["news"]:
        lines.append("최근 관련 뉴스:")
        for n in data["news"][:3]:
            lines.append(f"- {n['title']}")
    else:
        lines.append("최근 관련 뉴스: 조회된 뉴스 없음")
    return "\n".join(lines)


@mcp.tool()
def check_market_schedule() -> str:
    """오늘을 기준으로 파생상품 만기일(네마녀의 날) 등 수급에 영향을 주는 주요 일정을 확인합니다."""
    data = _fetch_market_schedule()
    tag = "[Market Warning]" if data["volatility_level"] == "high" else "[Market Notice]"
    return f"오늘 날짜: {data['date']}\n{tag} {data['message']}"


# ---------------------------------------------------------------------------
# REST API (프론트엔드용). CORS는 모든 origin에 대해 열어둡니다 (인증 없는 개인 프로젝트 기준).
# ---------------------------------------------------------------------------

@mcp.custom_route("/api/treasury", methods=["GET"])
async def api_treasury(request: Request) -> JSONResponse:
    return JSONResponse(_cached("treasury", 60, _fetch_treasury_yield))


@mcp.custom_route("/api/schedule", methods=["GET"])
async def api_schedule(request: Request) -> JSONResponse:
    return JSONResponse(_cached("schedule", 300, _fetch_market_schedule))


@mcp.custom_route("/api/stock/{ticker}", methods=["GET"])
async def api_stock(request: Request) -> JSONResponse:
    ticker = request.path_params["ticker"]
    return JSONResponse(_cached(f"stock:{ticker}", 30, lambda: _fetch_stock_info(ticker)))


@mcp.custom_route("/api/stock/{ticker}/detail", methods=["GET"])
async def api_stock_detail(request: Request) -> JSONResponse:
    ticker = request.path_params["ticker"]
    range_key = request.query_params.get("range", "3m")
    interval = request.query_params.get("interval", "1d")
    cache_key = f"detail:{ticker}:{range_key}:{interval}"
    return JSONResponse(_cached(cache_key, 30, lambda: _fetch_stock_detail(ticker, range_key, interval)))


@mcp.custom_route("/api/dashboard", methods=["GET"])
async def api_dashboard(request: Request) -> JSONResponse:
    return JSONResponse(_cached("dashboard", 30, _fetch_dashboard))


@mcp.custom_route("/api/portfolio", methods=["GET"])
async def api_portfolio(request: Request) -> JSONResponse:
    return JSONResponse(_cached("portfolio", 30, _fetch_portfolio))


# ---------------------------------------------------------------------------
# 프론트엔드 화면 (index.html)을 같은 서버의 "/" 에서 그대로 서빙합니다.
# ---------------------------------------------------------------------------

@mcp.custom_route("/", methods=["GET"])
async def index(request: Request):
    if not os.path.exists(_INDEX_HTML_PATH):
        return HTMLResponse("<h1>index.html이 서버에 없습니다.</h1>", status_code=404)
    return FileResponse(_INDEX_HTML_PATH, media_type="text/html")


if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "stdio"

    if mode in ("http", "streamable-http"):
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware

        mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("PORT", "8000"))
        mcp.settings.transport_security.enable_dns_rebinding_protection = False

        app = mcp.streamable_http_app()
        app = CORSMiddleware(app, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

        print(f"HTTP 모드로 실행합니다.")
        print(f"  MCP 엔드포인트: http://{mcp.settings.host}:{mcp.settings.port}/mcp")
        print(f"  REST API: /api/dashboard, /api/stock/{{ticker}}/detail, /api/treasury, /api/schedule")
        uvicorn.run(app, host=mcp.settings.host, port=mcp.settings.port)
    elif mode == "sse":
        mcp.run(transport="sse")
    else:
        mcp.run()
