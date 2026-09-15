"""
macro_mcp.py
미국 10년물 국채 금리 조회 + 네마녀의 날(파생상품 동시 만기) 확인용 MCP 서버.

엔노이아(Ennoia)의 "MCP 서버 추가" 화면은 URL 입력 방식(예: https://example.com/mcp)을
요구하므로, 아래처럼 "streamable-http" 모드로 실행해야 합니다.

로컬 실행:
  - stdio 방식 (로컬 프로세스로 직접 실행되는 host용, 엔노이아에는 해당 없음):
        python macro_mcp.py
  - HTTP 방식 (엔노이아용):
        python macro_mcp.py http
    로컬 접속 주소: http://127.0.0.1:8000/mcp
    (PORT 환경변수를 지정하면 그 포트로 뜹니다. Render 등 클라우드 배포 시 자동으로 쓰입니다.)
"""

import os
import sys
import datetime

from mcp.server.fastmcp import FastMCP
import yfinance as yf

mcp = FastMCP("MacroMarketTools")


@mcp.tool()
def get_treasury_yield() -> str:
    """미국 10년물 국채 금리(^TNX)의 최신 수치를 가져와서 밸류에이션 판단의 근거로 사용합니다."""
    try:
        tnx = yf.Ticker("^TNX")
        hist = tnx.history(period="5d")

        if hist.empty or len(hist) < 2:
            return "금리 데이터를 가져오지 못했습니다 (조회된 데이터가 없습니다)."

        # 주의: Yahoo Finance의 ^TNX는 실제 수익률의 10배 값으로 표시됩니다.
        # (예: 실제 4.25% -> ^TNX 값은 42.5) 그대로 쓰면 자릿수가 틀어지므로 10으로 나눕니다.
        latest_yield = hist["Close"].iloc[-1] / 10
        prev_yield = hist["Close"].iloc[-2] / 10
        diff = latest_yield - prev_yield
        trend = "상승" if diff > 0 else ("하락" if diff < 0 else "보합")

        return (
            f"[Macro Data] 미국 10년물 국채 금리: {latest_yield:.2f}% "
            f"(전일 대비 {trend}, {diff:+.2f}%p)"
        )
    except Exception as e:
        return f"금리 데이터를 가져오는데 실패했습니다: {e}"


@mcp.tool()
def check_market_schedule() -> str:
    """오늘을 기준으로 파생상품 만기일(네마녀의 날) 등 수급에 영향을 주는 주요 일정을 확인합니다."""
    today = datetime.date.today()

    is_witching_month = today.month in [3, 6, 9, 12]
    is_third_week = 15 <= today.day <= 21  # 그 달의 세 번째 금요일이 속한 주간

    report = f"오늘 날짜: {today.strftime('%Y-%m-%d')}\n"

    if is_witching_month and is_third_week:
        report += (
            "[Market Warning] 이번 주는 네마녀의 날(선물/옵션 동시 만기일)이 있는 주간입니다. "
            "장중 변동성 극대화 및 수급 왜곡에 주의해야 합니다."
        )
    elif is_witching_month:
        report += (
            "[Market Notice] 이번 달은 네마녀의 날이 있는 달이지만, 이번 주는 만기 주간이 아닙니다. "
            "일반적인 수준의 변동성을 예상합니다."
        )
    else:
        report += "[Market Notice] 특이 파생상품 만기 일정이 없는 평이한 주간입니다."

    return report


if __name__ == "__main__":
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "stdio"

    if mode in ("http", "streamable-http"):
        # 클라우드 배포(Render 등)는 컨테이너 바깥에서 접속해야 하므로 0.0.0.0으로 바인딩하고,
        # 플랫폼이 지정하는 PORT 환경변수를 그대로 사용합니다.
        mcp.settings.host = os.environ.get("HOST", "0.0.0.0")
        mcp.settings.port = int(os.environ.get("PORT", "8000"))
        # ngrok/Render처럼 도메인이 127.0.0.1/localhost가 아닐 때 기본 DNS-rebinding 방지
        # 설정이 요청을 막기 때문에 꺼 둡니다. 인증 없는 개인 테스트 서버라 지금은 문제 없습니다.
        mcp.settings.transport_security.enable_dns_rebinding_protection = False
        print(f"HTTP 모드로 실행합니다. 엔드포인트: http://{mcp.settings.host}:{mcp.settings.port}/mcp")
        mcp.run(transport="streamable-http")
    elif mode == "sse":
        mcp.run(transport="sse")
    else:
        mcp.run()
