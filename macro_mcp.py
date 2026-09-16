"""
macro_mcp.py
미국 10년물 국채 금리 / 종목 정보 / 네마녀의 날(파생상품 동시 만기) 조회용 MCP 서버.

이 서버는 두 가지 역할을 동시에 합니다.
  1. MCP 도구 (/mcp) — 엔노이아 같은 AI 에이전트가 호출하는 표준 MCP 엔드포인트.
  2. 일반 REST API (/api/...) — 직접 만든 프론트엔드(HTML/JS) 화면이 fetch()로
     바로 불러다 쓸 수 있는, 그냥 JSON을 주는 엔드포인트. 로직은 동일하고
     출력 형식만 다릅니다 (에이전트용은 사람이 읽는 문장, 프론트엔드용은 JSON).

     GET /api/treasury         - 미국 10년물 금리
     GET /api/schedule         - 네마녀의 날 등 시장 일정
     GET /api/stock/{ticker}   - 종목 정보 (예: /api/stock/NVDA, /api/stock/005930.KS)

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
import datetime

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse, HTMLResponse
import yfinance as yf

mcp = FastMCP("MacroMarketTools")

_INDEX_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")


# ---------------------------------------------------------------------------
# 데이터 조회 로직 (MCP 도구와 REST API가 함께 사용하는 공용 함수들)
# ---------------------------------------------------------------------------

def _fetch_treasury_yield() -> dict:
    try:
        tnx = yf.Ticker("^TNX")
        hist = tnx.history(period="5d")

        if hist.empty or len(hist) < 2:
            return {"error": "금리 데이터를 가져오지 못했습니다 (조회된 데이터가 없습니다)."}

        raw_latest = float(hist["Close"].iloc[-1])
        raw_prev = float(hist["Close"].iloc[-2])

        # 주의: Yahoo Finance의 ^TNX는 예전에는 실제 수익률의 10배 값으로 표시됐지만
        # (예: 실제 4.25% -> 42.5), 현재는 데이터 소스에 따라 실제 퍼센트 값을 그대로
        # 주기도 합니다. 미국 10년물 금리가 역사적으로 20%를 넘은 적이 없으므로,
        # 값이 20보다 크면 10배 표기로 보고 나눠주는 방식으로 자동 보정합니다.
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


def _fetch_market_schedule() -> dict:
    today = datetime.date.today()

    is_witching_month = today.month in [3, 6, 9, 12]
    is_third_week = 15 <= today.day <= 21  # 그 달의 세 번째 금요일이 속한 주간
    is_witching_week = is_witching_month and is_third_week

    if is_witching_week:
        level = "high"
        message = (
            "이번 주는 네마녀의 날(선물/옵션 동시 만기일)이 있는 주간입니다. "
            "장중 변동성 극대화 및 수급 왜곡에 주의해야 합니다."
        )
    elif is_witching_month:
        level = "normal"
        message = (
            "이번 달은 네마녀의 날이 있는 달이지만, 이번 주는 만기 주간이 아닙니다. "
            "일반적인 수준의 변동성을 예상합니다."
        )
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


def _fetch_stock_info(ticker: str) -> dict:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d")

        if hist.empty:
            return {
                "error": f"'{ticker}' 종목 데이터를 찾을 수 없습니다. 티커 형식을 확인해 주세요 "
                         f"(한국 주식은 .KS/.KQ 필요)."
            }

        try:
            info = t.info or {}
        except Exception:
            info = {}

        name = info.get("longName") or info.get("shortName") or ticker
        currency = info.get("currency", "")

        latest_close = float(hist["Close"].iloc[-1])
        prev_close = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else info.get("previousClose")
        volume = int(hist["Volume"].iloc[-1]) if "Volume" in hist.columns and len(hist) else info.get("volume")

        change = None
        change_pct = None
        if prev_close:
            change = latest_close - prev_close
            change_pct = (change / prev_close) * 100

        # 최근 관련 뉴스 (최대 3건). yfinance 응답 형식이 버전에 따라 조금씩 달라서
        # 여러 형태를 방어적으로 시도하고, 실패해도 나머지 정보는 그대로 반환합니다.
        try:
            raw_news = t.news or []
        except Exception:
            raw_news = []

        news_items = []
        for item in raw_news[:3]:
            content = item.get("content") if isinstance(item, dict) else None
            title = None
            link = None
            if isinstance(content, dict):
                title = content.get("title")
                canonical = content.get("canonicalUrl")
                if isinstance(canonical, dict):
                    link = canonical.get("url")
            if not title and isinstance(item, dict):
                title = item.get("title")
                link = link or item.get("link")
            if title:
                news_items.append({"title": title, "link": link})

        return {
            "ticker": ticker,
            "name": name,
            "currency": currency,
            "price": round(latest_close, 2),
            "prev_close": round(prev_close, 2) if prev_close else None,
            "change": round(change, 2) if change is not None else None,
            "change_pct": round(change_pct, 2) if change_pct is not None else None,
            "volume": volume,
            "market_cap": info.get("marketCap"),
            "per": info.get("trailingPE"),
            "pbr": info.get("priceToBook"),
            "eps": info.get("trailingEps"),
            "news": news_items,
        }
    except Exception as e:
        return {"error": f"'{ticker}' 정보를 가져오는데 실패했습니다: {e}"}


# ---------------------------------------------------------------------------
# MCP 도구 (엔노이아 에이전트용 — 사람이 읽는 문장으로 반환)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_treasury_yield() -> str:
    """미국 10년물 국채 금리(^TNX)의 최신 수치를 가져와서 밸류에이션 판단의 근거로 사용합니다."""
    data = _fetch_treasury_yield()
    if "error" in data:
        return data["error"]
    return (
        f"[Macro Data] 미국 10년물 국채 금리: {data['yield_pct']:.2f}% "
        f"(전일 대비 {data['trend']}, {data['diff_pct']:+.2f}%p)"
    )


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
        for n in data["news"]:
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
# REST API (직접 만든 프론트엔드 화면용 — JSON으로 반환)
# CORS는 아래 실행부에서 모든 origin에 대해 열어둡니다 (인증 없는 개인 프로젝트 기준).
# ---------------------------------------------------------------------------

@mcp.custom_route("/api/treasury", methods=["GET"])
async def api_treasury(request: Request) -> JSONResponse:
    return JSONResponse(_fetch_treasury_yield())


@mcp.custom_route("/api/schedule", methods=["GET"])
async def api_schedule(request: Request) -> JSONResponse:
    return JSONResponse(_fetch_market_schedule())


@mcp.custom_route("/api/stock/{ticker}", methods=["GET"])
async def api_stock(request: Request) -> JSONResponse:
    ticker = request.path_params["ticker"]
    return JSONResponse(_fetch_stock_info(ticker))


# ---------------------------------------------------------------------------
# 프론트엔드 화면 (index.html)을 같은 서버의 "/" 에서 그대로 서빙합니다.
# 별도 호스팅 없이 https://<서비스>.onrender.com/ 접속만으로 화면이 뜹니다.
# ---------------------------------------------------------------------------

@mcp.custom_route("/", methods=["GET"])
async def index(request: Request) -> HTMLResponse:
    try:
        with open(_INDEX_HTML_PATH, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html이 서버에 없습니다.</h1>", status_code=404)


if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "stdio"

    if mode in ("http", "streamable-http"):
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware

        # 클라우드 배포(Render 등)는 컨테이너 바깥에서 접속해야 하므로 0.0.0.0으로 바인딩하고,
        # 플랫폼이 지정하는 PORT 환경변수를 그대로 사용합니다.
        mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("PORT", "8000"))
        # ngrok/Render처럼 도메인이 127.0.0.1/localhost가 아닐 때 기본 DNS-rebinding 방지
        # 설정이 요청을 막기 때문에 꺼 둡니다. 인증 없는 개인 테스트 서버라 지금은 문제 없습니다.
        mcp.settings.transport_security.enable_dns_rebinding_protection = False

        app = mcp.streamable_http_app()
        # 프론트엔드(디자인 화면)가 다른 도메인에서 /api/... 를 fetch()로 부를 수 있도록 허용.
        app = CORSMiddleware(app, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

        print(f"HTTP 모드로 실행합니다.")
        print(f"  MCP 엔드포인트: http://{mcp.settings.host}:{mcp.settings.port}/mcp")
        print(f"  REST API: /api/treasury, /api/schedule, /api/stock/{{ticker}}")
        uvicorn.run(app, host=mcp.settings.host, port=mcp.settings.port)
    elif mode == "sse":
        mcp.run(transport="sse")
    else:
        mcp.run()
