#!/usr/bin/env python3
"""Q-TRON KR Market (Gen05) - 상세 매뉴얼 PDF 생성"""

import os, sys
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, gray
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable, ListFlowable, ListItem,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus.tableofcontents import TableOfContents

# ── Fonts ──
FONT_PATH = "C:/Windows/Fonts/malgun.ttf"
FONT_BOLD_PATH = "C:/Windows/Fonts/malgunbd.ttf"
pdfmetrics.registerFont(TTFont("MalgunGothic", FONT_PATH))
pdfmetrics.registerFont(TTFont("MalgunGothicBold", FONT_BOLD_PATH))

# ── Colors ──
C_PRIMARY   = HexColor("#1a237e")   # Deep blue
C_SECONDARY = HexColor("#283593")
C_ACCENT    = HexColor("#0d47a1")
C_BG_LIGHT  = HexColor("#e8eaf6")
C_BG_CODE   = HexColor("#f5f5f5")
C_RED       = HexColor("#c62828")
C_GREEN     = HexColor("#2e7d32")
C_ORANGE    = HexColor("#e65100")
C_GRAY      = HexColor("#757575")
C_TABLE_HDR = HexColor("#1a237e")
C_TABLE_ALT = HexColor("#f0f0f8")

# ── Styles ──
styles = getSampleStyleSheet()

def make_style(name, fontName="MalgunGothic", fontSize=10, leading=14,
               textColor=black, alignment=TA_LEFT, spaceBefore=0,
               spaceAfter=4, leftIndent=0, bold=False):
    fn = "MalgunGothicBold" if bold else fontName
    return ParagraphStyle(name, fontName=fn, fontSize=fontSize, leading=leading,
                          textColor=textColor, alignment=alignment,
                          spaceBefore=spaceBefore, spaceAfter=spaceAfter,
                          leftIndent=leftIndent, wordWrap='CJK')

S_COVER_TITLE   = make_style("CoverTitle", fontSize=28, leading=36, textColor=C_PRIMARY, alignment=TA_CENTER, bold=True)
S_COVER_SUB     = make_style("CoverSub", fontSize=14, leading=20, textColor=C_SECONDARY, alignment=TA_CENTER)
S_H1            = make_style("H1", fontSize=18, leading=24, textColor=C_PRIMARY, spaceBefore=20, spaceAfter=10, bold=True)
S_H2            = make_style("H2", fontSize=14, leading=19, textColor=C_SECONDARY, spaceBefore=14, spaceAfter=6, bold=True)
S_H3            = make_style("H3", fontSize=12, leading=16, textColor=C_ACCENT, spaceBefore=10, spaceAfter=4, bold=True)
S_BODY          = make_style("Body", fontSize=9.5, leading=14, spaceAfter=4, alignment=TA_JUSTIFY)
S_BODY_SMALL    = make_style("BodySmall", fontSize=8.5, leading=12, spaceAfter=2)
S_CODE          = make_style("Code", fontName="Courier", fontSize=8, leading=11, textColor=HexColor("#333333"), leftIndent=10)
S_BULLET        = make_style("Bullet", fontSize=9.5, leading=14, leftIndent=15, spaceAfter=2)
S_NOTE          = make_style("Note", fontSize=8.5, leading=12, textColor=C_ORANGE, leftIndent=10, spaceBefore=4, spaceAfter=4)
S_CAPTION       = make_style("Caption", fontSize=8, leading=10, textColor=C_GRAY, alignment=TA_CENTER, spaceBefore=2, spaceAfter=6)
S_TOC_H1        = make_style("TOCH1", fontSize=11, leading=15, bold=True, spaceBefore=6, spaceAfter=2)
S_TOC_H2        = make_style("TOCH2", fontSize=9.5, leading=13, leftIndent=15, spaceAfter=1)

# ── Helpers ──
def h1(text):  return Paragraph(text, S_H1)
def h2(text):  return Paragraph(text, S_H2)
def h3(text):  return Paragraph(text, S_H3)
def p(text):   return Paragraph(text, S_BODY)
def ps(text):  return Paragraph(text, S_BODY_SMALL)
def code(text): return Paragraph(text.replace("\n", "<br/>").replace(" ", "&nbsp;"), S_CODE)
def bullet(text): return Paragraph(f"&bull; {text}", S_BULLET)
def note(text): return Paragraph(f"<b>NOTE:</b> {text}", S_NOTE)
def warn(text): return Paragraph(f"<b>WARNING:</b> {text}", S_NOTE)
def sp(h=6): return Spacer(1, h)
def hr(): return HRFlowable(width="100%", thickness=0.5, color=C_GRAY, spaceBefore=6, spaceAfter=6)

def make_table(headers, rows, col_widths=None):
    data = [headers] + rows
    w = col_widths or [None] * len(headers)
    t = Table(data, colWidths=w, repeatRows=1)
    style_cmds = [
        ('BACKGROUND', (0, 0), (-1, 0), C_TABLE_HDR),
        ('TEXTCOLOR', (0, 0), (-1, 0), white),
        ('FONTNAME', (0, 0), (-1, 0), 'MalgunGothicBold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8.5),
        ('FONTNAME', (0, 1), (-1, -1), 'MalgunGothic'),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('LEADING', (0, 0), (-1, -1), 11),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.3, C_GRAY),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ('LEFTPADDING', (0, 0), (-1, -1), 5),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style_cmds.append(('BACKGROUND', (0, i), (-1, i), C_TABLE_ALT))
    t.setStyle(TableStyle(style_cmds))
    return t

def page_header_footer(canvas, doc):
    canvas.saveState()
    canvas.setFont("MalgunGothicBold", 8)
    canvas.setFillColor(C_GRAY)
    canvas.drawString(2*cm, A4[1] - 1.2*cm, "Q-TRON KR Gen05 1.0  |  KR Market Manual v1.0")
    canvas.drawRightString(A4[0] - 2*cm, A4[1] - 1.2*cm, "2026-04-12")
    canvas.setStrokeColor(C_BG_LIGHT)
    canvas.line(2*cm, A4[1] - 1.4*cm, A4[0] - 2*cm, A4[1] - 1.4*cm)
    canvas.setFont("MalgunGothic", 8)
    canvas.drawCentredString(A4[0]/2, 1.2*cm, f"- {doc.page} -")
    canvas.restoreState()


# ══════════════════════════════════════════════
#  CONTENT
# ══════════════════════════════════════════════
def build():
    out_path = os.path.join(os.path.dirname(__file__), "Q-TRON_KR_Gen05_Manual_v1.0.pdf")
    doc = SimpleDocTemplate(out_path, pagesize=A4,
                            topMargin=2*cm, bottomMargin=2*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    story = []

    # ── COVER PAGE ──
    story.append(Spacer(1, 6*cm))
    story.append(Paragraph("Q-TRON KR Gen05 1.0", S_COVER_TITLE))
    story.append(sp(10))
    story.append(Paragraph("KR Market Trading System", make_style("cs2", fontSize=20, leading=26, textColor=C_ACCENT, alignment=TA_CENTER)))
    story.append(sp(6))
    story.append(Paragraph("상세 운영 매뉴얼 v1.0", S_COVER_SUB))
    story.append(sp(30))
    story.append(Paragraph("2026-04-12", make_style("cd", fontSize=11, leading=14, textColor=C_GRAY, alignment=TA_CENTER)))
    story.append(Paragraph("Confidential  |  Internal Use Only", make_style("cc", fontSize=9, leading=12, textColor=C_GRAY, alignment=TA_CENTER)))
    story.append(PageBreak())

    # ── TABLE OF CONTENTS ──
    story.append(h1("목차 (Table of Contents)"))
    story.append(sp(6))
    toc_items = [
        ("1", "시스템 개요 (System Overview)"),
        ("2", "디렉토리 구조 (Directory Structure)"),
        ("3", "설정 파라미터 (Configuration)"),
        ("4", "실행 모드 (Execution Modes)"),
        ("5", "전략 레이어 (Strategy Layer)"),
        ("6", "코어 레이어 (Core Layer)"),
        ("7", "데이터 레이어 (Data Layer)"),
        ("8", "리스크 관리 (Risk Management)"),
        ("9", "런타임 실행 (Runtime Execution)"),
        ("10", "라이프사이클 오케스트레이션 (Lifecycle)"),
        ("11", "Strategy Lab (전략 실험실)"),
        ("12", "AI Advisor (분석 전용)"),
        ("13", "웹 대시보드 (Web Dashboard)"),
        ("14", "리포팅 (Reporting)"),
        ("15", "레짐 예측 (Regime Prediction)"),
        ("16", "알림 시스템 (Notifications)"),
        ("17", "백테스트 (Backtesting)"),
        ("18", "운영 가이드 (Operations Guide)"),
        ("19", "트러블슈팅 (Troubleshooting)"),
        ("20", "확장 로드맵 (Expansion Roadmap)"),
    ]
    for num, title in toc_items:
        style = S_TOC_H1 if True else S_TOC_H2
        story.append(Paragraph(f"{num}.  {title}", style))
    story.append(PageBreak())

    # ═══ 1. SYSTEM OVERVIEW ═══
    story.append(h1("1. 시스템 개요 (System Overview)"))
    story.append(p("Q-TRON Gen05는 한국 주식시장(KOSPI/KOSDAQ)에서 <b>LowVol + Momentum 12-1</b> 전략을 실행하는 자동매매 시스템입니다. 키움증권 REST API를 통해 주문을 실행하며, 실시간 WebSocket 모니터링, DD 기반 리스크 관리, 종합 백테스팅을 지원합니다."))
    story.append(sp(4))

    story.append(h2("1.1 핵심 전략 요약"))
    story.append(make_table(
        ["항목", "값", "설명"],
        [
            ["유니버스", "KOSPI + KOSDAQ", "2,000~2,600 종목"],
            ["변동성 필터", "하위 30%ile (252일)", "저변동성 종목 선별"],
            ["모멘텀 랭킹", "12-1개월 수익률", "최근 1개월 제외"],
            ["포트폴리오", "20종목 균등배분", "리밸런싱 시 동일 비중"],
            ["리밸런싱", "21거래일 (월간)", "매도 우선 → 매수"],
            ["청산", "Trailing Stop -12%", "종가 기준, HWM 갱신"],
            ["레짐", "BULL/SIDE/BEAR", "관찰용 (포지션 사이즈 미변경)"],
        ],
        col_widths=[3*cm, 4*cm, 8*cm]
    ))
    story.append(sp(6))

    story.append(h2("1.2 아키텍처 개요"))
    story.append(p("시스템은 <b>3계층 + 5서브시스템</b> 구조로 구성됩니다:"))
    story.append(bullet("<b>Engine (Core)</b> - portfolio_manager, state_manager: 포트폴리오 및 상태 관리"))
    story.append(bullet("<b>Interface (Strategy)</b> - scoring, rebalancer, trail_stop: 종목 선정 및 주문 생성"))
    story.append(bullet("<b>Operations (Lifecycle)</b> - 5단계 오케스트레이션: STARTUP → RECON → REBAL → MONITOR → EOD"))
    story.append(sp(2))
    story.append(bullet("<b>Data Layer</b> - REST provider, WebSocket, pykrx, DB (SQLite cache)"))
    story.append(bullet("<b>Risk Layer</b> - DD guard, safety checks, graduated response"))
    story.append(bullet("<b>Runtime Layer</b> - order executor, order tracker, fill idempotency"))
    story.append(bullet("<b>Report Layer</b> - CSV 로그, 일간/주간 리포트"))
    story.append(bullet("<b>Web Layer</b> - FastAPI 대시보드, SSE 실시간 스트리밍"))
    story.append(sp(6))

    story.append(h2("1.3 Open API vs REST API 비교"))
    story.append(make_table(
        ["항목", "Open API (kr-legacy, 삭제 예정)", "REST API (Gen05)"],
        [
            ["연결 방식", "Qt QAxWidget (COM)", "HTTP + WebSocket"],
            ["로그인", "COM 라이선스", "OAuth 토큰 (자동 갱신)"],
            ["실시간", "QSignal 콜백", "WebSocket 이벤트 루프"],
            ["Rate Limit", "암묵적", "200ms 최소 간격"],
            ["스레드", "Qt 이벤트 루프", "asyncio 비동기"],
            ["재연결", "수동", "자동 (최대 5회)"],
            ["의존성", "PyQt5 + COM", "순수 Python (requests)"],
        ],
        col_widths=[3*cm, 5.5*cm, 6.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 2. DIRECTORY STRUCTURE ═══
    story.append(h1("2. 디렉토리 구조 (Directory Structure)"))
    story.append(p("Gen05 프로젝트의 전체 디렉토리 구조입니다. 각 디렉토리의 역할을 상세히 설명합니다."))
    story.append(sp(4))

    dirs = [
        ["config.py", "전략/리스크/비용 전체 파라미터 (Single Source of Truth)"],
        ["main.py", "CLI 진입점 (--batch, --live, --mock, --backtest)"],
        ["core/", "포트폴리오 매니저 + 상태 매니저 (Engine 보호 계층)"],
        ["strategy/", "scoring, factor_ranker, rebalancer, trail_stop, regime"],
        ["data/", "rest_provider, rest_websocket, pykrx, db_provider, universe"],
        ["risk/", "exposure_guard (DD guard), safety_checks, risk_management"],
        ["runtime/", "order_executor (mock/kiwoom), order_tracker (fill idempotency)"],
        ["lifecycle/", "5단계 라이브 오케스트레이션 + batch + mock"],
        ["lab/", "Strategy Lab: 10개 전략 시뮬레이션 엔진"],
        ["advisor/", "AI Advisor (읽기 전용, 분석/알림/추천)"],
        ["regime/", "글로벌+국내 지표 기반 레짐 예측"],
        ["report/", "CSV 로그 + 일간/주간 리포트"],
        ["notify/", "Telegram + Kakao 알림"],
        ["web/", "FastAPI 대시보드 + SSE + Surge 스캐너"],
        ["backtest/", "백테스터 (OHLCV 리플레이)"],
        ["tests/", "유닛 테스트 (51건)"],
    ]
    story.append(make_table(["경로", "설명"], dirs, col_widths=[3.5*cm, 11.5*cm]))
    story.append(PageBreak())

    # ═══ 3. CONFIGURATION ═══
    story.append(h1("3. 설정 파라미터 (Configuration)"))
    story.append(p("<b>config.py</b>는 시스템의 모든 파라미터를 관리하는 Single Source of Truth입니다."))
    story.append(sp(4))

    story.append(h2("3.1 전략 파라미터"))
    story.append(make_table(
        ["파라미터", "값", "설명"],
        [
            ["VOL_LOOKBACK", "252", "변동성 계산 윈도우 (12개월)"],
            ["VOL_PERCENTILE", "0.30", "저변동성 필터 (하위 30%)"],
            ["MOM_LOOKBACK", "252", "모멘텀 계산 윈도우 (12개월)"],
            ["MOM_SKIP", "22", "최근 1개월 제외 (~22거래일)"],
            ["N_STOCKS", "20", "포트폴리오 목표 종목 수"],
            ["REBAL_DAYS", "21", "리밸런싱 주기 (21거래일)"],
            ["TRAIL_PCT", "0.12", "트레일링 스톱 -12%"],
            ["CASH_BUFFER_RATIO", "0.95", "매수 시 현금 95%만 사용"],
        ],
        col_widths=[4*cm, 2*cm, 9*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.2 거래 비용 모델"))
    story.append(make_table(
        ["구분", "수수료", "슬리피지", "세금", "합계"],
        [
            ["매수 (BUY)", "0.015%", "0.10%", "-", "0.115%"],
            ["매도 (SELL)", "0.015%", "0.10%", "0.18%", "0.295%"],
        ],
        col_widths=[3*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.3 리스크 파라미터"))
    story.append(make_table(
        ["파라미터", "값", "동작"],
        [
            ["DAILY_DD_LIMIT", "-4%", "일간 DD 한도 → 신규 매수 차단"],
            ["MONTHLY_DD_LIMIT", "-7%", "월간 DD 한도 → 신규 매수 차단"],
            ["DD_CAUTION", "-5%", "매수 70% 축소"],
            ["DD_WARNING", "-10%", "매수 50% 축소"],
            ["DD_CRITICAL", "-15%", "매수 전면 차단"],
            ["DD_SEVERE", "-20%", "매수 차단 + 20% 청산"],
            ["DD_SAFE_MODE", "-25%", "매수 차단 + 20% 청산 + SAFE MODE"],
            ["SAFE_MODE_RELEASE", "-20%", "DD >= -20% 시 SAFE MODE 해제"],
        ],
        col_widths=[4*cm, 2*cm, 9*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.4 유니버스 필터"))
    story.append(make_table(
        ["필터", "값", "설명"],
        [
            ["MARKETS", "KOSPI + KOSDAQ", "대상 시장"],
            ["UNIV_MIN_CLOSE", "2,000원", "최소 종가"],
            ["UNIV_MIN_AMOUNT", "20억원", "20일 평균 거래대금"],
            ["UNIV_MIN_HISTORY", "260일", "최소 거래 이력"],
            ["UNIV_MIN_COUNT", "500", "유니버스 최소 경고 기준"],
        ],
        col_widths=[4*cm, 3*cm, 8*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.5 트레이딩 모드"))
    story.append(make_table(
        ["모드", "브로커", "설명"],
        [
            ["mock", "없음", "내부 시뮬레이션 (파일/브로커 영향 없음)"],
            ["paper", "키움 MOCK 서버", "키움 모의투자"],
            ["paper_test", "키움 MOCK 서버", "격리 테스트 (fast reentry, 강제 리밸)"],
            ["shadow_test", "없음", "드라이런 (주문 미발송)"],
            ["live", "키움 REAL 서버", "실거래"],
        ],
        col_widths=[3*cm, 4*cm, 8*cm]
    ))
    story.append(PageBreak())

    # ═══ 4. EXECUTION MODES ═══
    story.append(h1("4. 실행 모드 (Execution Modes)"))

    story.append(h2("4.1 Batch 모드 (--batch)"))
    story.append(p("매일 장 마감 후 데이터 업데이트 및 목표 포트폴리오 산출을 수행합니다."))
    story.append(sp(2))
    story.append(bullet("<b>Step 1</b>: pykrx로 전 종목 OHLCV 업데이트"))
    story.append(bullet("<b>Step 2</b>: 유니버스 빌드 (가격/거래대금/이력 필터)"))
    story.append(bullet("<b>Step 3</b>: DB(SQLite) 또는 CSV에서 종가 로드"))
    story.append(bullet("<b>Step 4</b>: 전 종목 scoring (vol_12m, mom_12_1)"))
    story.append(bullet("<b>Step 5</b>: LowVol 필터 → 양의 모멘텀 → Top 20 선정"))
    story.append(bullet("<b>Step 6</b>: signals/target_portfolio_{YYYYMMDD}.json 저장"))
    story.append(sp(4))
    story.append(code("python main.py --batch"))
    story.append(sp(6))

    story.append(h2("4.2 Live 모드 (--live)"))
    story.append(p("5단계 라이프사이클 오케스트레이션으로 실시간 매매를 수행합니다. 자세한 내용은 Section 10 참조."))
    story.append(sp(2))
    story.append(code("python main.py --live"))
    story.append(sp(6))

    story.append(h2("4.3 Mock 모드 (--mock)"))
    story.append(p("브로커 없이 내부 시뮬레이션으로 로직을 테스트합니다. state 파일에 영향 없습니다."))
    story.append(sp(2))
    story.append(code("python main.py --mock"))
    story.append(sp(6))

    story.append(h2("4.4 Backtest 모드 (--backtest)"))
    story.append(p("과거 OHLCV 데이터를 리플레이하여 전략 성과를 검증합니다."))
    story.append(sp(2))
    story.append(code("python main.py --backtest --start 2019-01-02 --end 2026-03-20"))
    story.append(PageBreak())

    # ═══ 5. STRATEGY LAYER ═══
    story.append(h1("5. 전략 레이어 (Strategy Layer)"))

    story.append(h2("5.1 Scoring (scoring.py)"))
    story.append(p("모든 종목의 변동성과 모멘텀을 계산하는 <b>SHARED</b> 모듈입니다. 백테스트, 배치, 라이브 모두 동일한 코드를 사용합니다."))
    story.append(sp(2))
    story.append(h3("calc_volatility(close_series, lookback=252)"))
    story.append(p("252일 일간 수익률의 <b>raw standard deviation</b>을 반환합니다. 연율화하지 않으며, 횡단면(cross-sectional) 랭킹에만 사용되므로 순서가 변하지 않습니다."))
    story.append(sp(2))
    story.append(h3("calc_momentum(close_series, lookback=252, skip=22)"))
    story.append(p("12-1개월 모멘텀: <b>close[-22] / close[-252] - 1</b>. 최근 1개월(~22거래일)을 제외하여 단기 반전 효과를 회피합니다."))
    story.append(sp(6))

    story.append(h2("5.2 Factor Ranker (factor_ranker.py)"))
    story.append(p("종목 선정 프로세스:"))
    story.append(bullet("1) 전 종목 scoring (vol_12m, mom_12_1)"))
    story.append(bullet("2) 변동성 하위 30% 필터 (VOL_PERCENTILE = 0.30)"))
    story.append(bullet("3) 양의 모멘텀만 유지 (mom_12_1 > 0)"))
    story.append(bullet("4) 모멘텀 내림차순 정렬"))
    story.append(bullet("5) 상위 20종목 선택 (N_STOCKS = 20)"))
    story.append(sp(4))
    story.append(p("<b>출력 형식</b> (JSON):"))
    story.append(code('{\n  "date": "20260401",\n  "target_tickers": ["000660", "005380", ...],\n  "scores": {"000660": {"vol_12m": 0.0234, "mom_12_1": 0.1567}},\n  "vol_threshold": 0.0289,\n  "universe_size": 2150\n}'))
    story.append(sp(6))

    story.append(h2("5.3 Rebalancer (rebalancer.py)"))
    story.append(p("현재 포트폴리오와 목표 포트폴리오를 비교하여 매도/매수 주문을 생성합니다."))
    story.append(sp(2))
    story.append(bullet("<b>SELL</b>: 보유 중이지만 목표에 없는 종목 → 전량 매도"))
    story.append(bullet("<b>BUY</b>: 목표에 있지만 미보유 종목 → 균등 배분"))
    story.append(bullet("배분 금액 = 총 자산 / N_STOCKS"))
    story.append(bullet("트레일 스톱으로 빠진 슬롯은 빈 상태 유지 (중간 보충 없음)"))
    story.append(sp(6))

    story.append(h2("5.4 Trail Stop (trail_stop.py)"))
    story.append(p("종가 기반 트레일링 스톱:"))
    story.append(bullet("HWM (High Water Mark) = max(기존 HWM, 오늘 종가)"))
    story.append(bullet("발동 조건: 종가 <= HWM x (1 - 0.12)"))
    story.append(bullet("가격 안전 범위: [100원, 1억원] — 범위 밖이면 HOLD"))
    story.append(warn("유효하지 않은 가격(NaN, inf, 0 이하)에서는 절대 매도하지 않습니다."))
    story.append(sp(6))

    story.append(h2("5.5 Regime Detector (regime_detector.py)"))
    story.append(make_table(
        ["레짐", "조건", "Exposure"],
        [
            ["BULL", "KOSPI > MA200 AND breadth > 60%", "100%"],
            ["SIDE", "그 외", "70%"],
            ["BEAR", "KOSPI < MA200 OR breadth < 40%", "40%"],
        ],
        col_widths=[3*cm, 7*cm, 3*cm]
    ))
    story.append(note("현재 레짐은 관찰용이며 포지션 사이즈를 변경하지 않습니다."))
    story.append(PageBreak())

    # ═══ 6. CORE LAYER ═══
    story.append(h1("6. 코어 레이어 (Core Layer)"))

    story.append(h2("6.1 Portfolio Manager (core/portfolio_manager.py)"))
    story.append(p("포지션, 현금, 자산, 리스크 모드를 관리하는 핵심 클래스입니다."))
    story.append(sp(2))
    story.append(h3("Position 데이터 구조"))
    story.append(make_table(
        ["필드", "타입", "설명"],
        [
            ["code", "str", "종목 코드 (6자리)"],
            ["quantity", "int", "보유 수량"],
            ["avg_price", "float", "평균 매수가"],
            ["entry_date", "str", "매수일"],
            ["high_watermark", "float", "최고가 (HWM)"],
            ["trail_stop_price", "float", "트레일 스톱 가격"],
            ["current_price", "float", "현재가"],
            ["invested_total", "float", "투자 금액 (수수료 포함)"],
            ["entry_rank", "int", "진입 시 모멘텀 순위"],
            ["score_mom", "float", "진입 시 모멘텀 스코어"],
            ["last_price_ts", "datetime", "마지막 가격 갱신 시각"],
        ],
        col_widths=[3.5*cm, 2.5*cm, 9*cm]
    ))
    story.append(sp(4))
    story.append(h3("주요 메서드"))
    story.append(bullet("<b>add_position(code, qty, price)</b> — 포지션 추가/증가"))
    story.append(bullet("<b>remove_position(code, qty)</b> — 포지션 감소/제거"))
    story.append(bullet("<b>update_prices(price_dict)</b> — 현재가 갱신 + HWM 업데이트"))
    story.append(bullet("<b>get_daily_pnl_pct()</b> — 당일 수익률"))
    story.append(bullet("<b>get_monthly_dd_pct()</b> — 월간 피크 대비 최대 하락"))
    story.append(bullet("<b>risk_mode()</b> — NORMAL / DAILY_BLOCKED / MONTHLY_BLOCKED"))
    story.append(sp(6))

    story.append(h2("6.2 State Manager (core/state_manager.py)"))
    story.append(p("원자적(Atomic) JSON 파일 저장으로 데이터 무결성을 보장합니다."))
    story.append(sp(2))
    story.append(h3("Atomic Write 패턴"))
    story.append(bullet("1) 임시 파일에 쓰기 (temp_{uuid}.json)"))
    story.append(bullet("2) JSON 유효성 검증 (readback)"))
    story.append(bullet("3) 기존 파일 백업 (.backup)"))
    story.append(bullet("4) 임시 → 최종 파일 rename (원자적)"))
    story.append(sp(2))
    story.append(h3("모드별 파일 분리"))
    story.append(make_table(
        ["파일", "용도"],
        [
            ["portfolio_state_mock.json", "Mock 모드 포트폴리오"],
            ["portfolio_state_paper.json", "Paper 모드 포트폴리오"],
            ["portfolio_state_live.json", "Live 모드 포트폴리오"],
            ["runtime_state_{mode}.json", "런타임 플래그 (리밸 상태, DD 등)"],
        ],
        col_widths=[5.5*cm, 9.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 7. DATA LAYER ═══
    story.append(h1("7. 데이터 레이어 (Data Layer)"))

    story.append(h2("7.1 REST Provider (data/rest_provider.py)"))
    story.append(p("키움증권 REST API 클라이언트입니다. Phase 0(HTTP)과 Phase 1(WebSocket) 두 단계로 동작합니다."))
    story.append(sp(2))
    story.append(h3("주요 메서드"))
    story.append(make_table(
        ["메서드", "설명"],
        [
            ["query_account_holdings()", "계좌 보유종목 조회"],
            ["query_account_summary()", "계좌 총괄 (총자산, 현금 등)"],
            ["query_sellable_qty(code)", "매도 가능 수량 조회"],
            ["send_order(code, side, qty, price)", "주문 발송 (시장가/지정가)"],
            ["get_order_status(order_no)", "주문 상태 확인"],
            ["get_current_price(code)", "현재가 조회"],
            ["register_real(codes, fids)", "실시간 시세 등록"],
        ],
        col_widths=[5*cm, 10*cm]
    ))
    story.append(sp(6))

    story.append(h2("7.2 REST WebSocket (data/rest_websocket.py)"))
    story.append(p("실시간 가격/주문/잔고 이벤트를 수신합니다."))
    story.append(make_table(
        ["구독 코드", "용도"],
        [
            ["0B", "주식 실시간 시세 (종가, 거래량, 호가)"],
            ["00", "주문 체결 통보 (체결가, 체결량, 상태)"],
            ["04", "계좌 잔고 변동 (현금, 보유종목)"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(p("자동 재연결: 최대 5회, 5초 간격"))
    story.append(sp(6))

    story.append(h2("7.3 Token Manager (data/rest_token_manager.py)"))
    story.append(p("OAuth 토큰 자동 갱신을 관리합니다. 토큰 만료 전에 자동으로 새 토큰을 발급받습니다."))
    story.append(sp(6))

    story.append(h2("7.4 Universe Builder (data/universe_builder.py)"))
    story.append(p("필터 체인:"))
    story.append(bullet("1) 우선주 제외 (종목코드 끝자리 5~9)"))
    story.append(bullet("2) 거래 이력 >= 260일"))
    story.append(bullet("3) 20일 평균 거래대금 >= 20억원"))
    story.append(bullet("4) 종가 >= 2,000원"))
    story.append(bullet("5) 거래정지 제외 (종가 == 0)"))
    story.append(PageBreak())

    # ═══ 8. RISK MANAGEMENT ═══
    story.append(h1("8. 리스크 관리 (Risk Management)"))

    story.append(h2("8.1 DD 기반 매수 차단 (Exposure Guard)"))
    story.append(p("일간/월간 Drawdown에 따라 매수를 차단하거나 축소합니다."))
    story.append(sp(2))
    story.append(make_table(
        ["DD 수준", "buy_scale", "trim_ratio", "라벨"],
        [
            ["-5% 이하", "0.70 (70%)", "0%", "DD_CAUTION"],
            ["-10% 이하", "0.50 (50%)", "0%", "DD_WARNING"],
            ["-15% 이하", "0.00 (차단)", "0%", "DD_CRITICAL"],
            ["-20% 이하", "0.00 (차단)", "20%", "DD_SEVERE"],
            ["-25% 이하", "0.00 (차단)", "20%", "DD_SAFE_MODE"],
        ],
        col_widths=[3*cm, 3*cm, 2.5*cm, 3.5*cm]
    ))
    story.append(sp(2))
    story.append(note("SELL은 항상 허용됩니다. BUY만 DD에 의해 차단/축소됩니다."))
    story.append(sp(6))

    story.append(h2("8.2 Safety Checks (Pre-Order Validation)"))
    story.append(bullet("종목코드 형식 검증 (6자리)"))
    story.append(bullet("수량 > 0, 가격 > 0"))
    story.append(bullet("BUY: 총 비용 <= 가용 현금"))
    story.append(bullet("SELL: 수량 <= 보유 수량"))
    story.append(bullet("거래정지 감지 (종가 == 0)"))
    story.append(sp(6))

    story.append(h2("8.3 BuyPermission 상태기계"))
    story.append(make_table(
        ["상태", "설명"],
        [
            ["NORMAL", "정상 리밸런싱 (매도 + 매수)"],
            ["REDUCED", "매수 축소 (DD_CAUTION/WARNING)"],
            ["RECOVERING", "BLOCKED에서 회복 중"],
            ["BLOCKED", "매수 전면 차단"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(p("<b>stateless 설계</b>: 매 세션 NORMAL로 초기화, 실시간 신호로 재판정"))
    story.append(PageBreak())

    # ═══ 9. RUNTIME EXECUTION ═══
    story.append(h1("9. 런타임 실행 (Runtime Execution)"))

    story.append(h2("9.1 Order Executor (runtime/order_executor.py)"))
    story.append(p("Mock/Kiwoom 모드에 따라 주문을 실행합니다."))
    story.append(sp(2))
    story.append(make_table(
        ["메서드", "설명"],
        [
            ["execute_buy(code, qty, reason)", "매수 주문 실행 → {order_no, exec_price, exec_qty}"],
            ["execute_sell(code, qty, reason)", "매도 주문 실행 → {order_no, exec_price, exec_qty}"],
        ],
        col_widths=[5.5*cm, 9.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("9.2 Order Tracker (runtime/order_tracker.py)"))
    story.append(p("주문 등록, 체결 추적, 중복 방지(idempotency)를 담당합니다."))
    story.append(sp(2))
    story.append(h3("주문 상태 흐름"))
    story.append(p("NEW → SUBMITTED → PARTIAL_FILLED → FILLED"))
    story.append(p("또는: SUBMITTED → TIMEOUT_UNCERTAIN → PENDING_EXTERNAL"))
    story.append(p("또는: SUBMITTED → CANCELLED / REJECTED"))
    story.append(sp(2))
    story.append(h3("FillEvent 구조"))
    story.append(make_table(
        ["필드", "설명"],
        [
            ["fill_id", "고유 ID: {order_no}_{side}_{cum_qty}"],
            ["order_no", "주문 번호"],
            ["side", "BUY / SELL"],
            ["exec_qty", "이번 체결 수량"],
            ["exec_price", "체결가"],
            ["source", "CHEJAN / GHOST / RECONCILE"],
        ],
        col_widths=[3.5*cm, 11.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 10. LIFECYCLE ORCHESTRATION ═══
    story.append(h1("10. 라이프사이클 오케스트레이션 (Lifecycle)"))
    story.append(p("Live 모드는 <b>5단계 Phase 시퀀서</b>로 동작합니다. 각 Phase는 명확한 진입/종료 조건과 안전 규칙을 가집니다."))
    story.append(sp(6))

    story.append(make_table(
        ["Phase", "이름", "설명", "주문 허용"],
        [
            ["0", "STARTUP", "Provider 초기화, 로그인, 상태 복원", "차단"],
            ["1", "RECON", "Broker reconciliation, ghost fill 처리", "차단"],
            ["1.5", "PENDING_BUY", "미체결 매수 복구, fast reentry", "매수 (DD 허용 시)"],
            ["2", "REBALANCE", "리밸런싱 실행 (매도 → 매수)", "매수 차단 가능"],
            ["3", "MONITOR", "60초 루프, trail stop 경고, 실시간 가격", "매도 실행"],
            ["4", "EOD", "15:30 대기, trail stop 실행, 리포트", "최종 매도"],
        ],
        col_widths=[1.5*cm, 3*cm, 7.5*cm, 3*cm]
    ))
    story.append(sp(6))

    story.append(h2("10.1 Phase 0+1: Startup + RECON"))
    story.append(bullet("REST Provider 초기화 (OAuth 토큰 발급)"))
    story.append(bullet("portfolio_state JSON에서 상태 복원"))
    story.append(bullet("레짐 스냅샷 (관찰용)"))
    story.append(bullet("Broker 보유종목 조회 → 내부 상태와 비교"))
    story.append(bullet("Ghost fill 감지 → 자동 보정"))
    story.append(bullet("recon_complete = True 설정 (주문 게이트 개방)"))
    story.append(sp(4))

    story.append(h2("10.2 Phase 1.5: Pending Buy"))
    story.append(bullet("runtime_state에서 pending_buys 로드"))
    story.append(bullet("ready_at 시간 경과 AND DD 허용 시 매수 실행"))
    story.append(bullet("PENDING_EXTERNAL (브로커에 걸린 주문) 처리"))
    story.append(sp(4))

    story.append(h2("10.3 Phase 2: Rebalance"))
    story.append(bullet("마지막 리밸 이후 21거래일 경과 확인"))
    story.append(bullet("target_portfolio JSON 로드"))
    story.append(bullet("매도 주문 생성 및 실행 (현금 확보)"))
    story.append(bullet("DD guard 평가 → buy_scale 결정"))
    story.append(bullet("매수 주문 생성 및 실행 (스케일 적용)"))
    story.append(sp(4))

    story.append(h2("10.4 Phase 3: Monitor (60초 루프)"))
    story.append(bullet("WebSocket 실시간 가격 수신"))
    story.append(bullet("HWM 업데이트"))
    story.append(bullet("Trail stop 경고 로그"))
    story.append(bullet("분봉 데이터 수집 (intraday_collector)"))
    story.append(bullet("Fast reentry 체크 (pending 있을 때)"))
    story.append(sp(4))

    story.append(h2("10.5 Phase 4: EOD"))
    story.append(bullet("15:30까지 대기 (종가 확정)"))
    story.append(bullet("Trail stop 종가 기준 실행"))
    story.append(bullet("일간 자산 스냅샷 기록"))
    story.append(bullet("daily report (CSV) 생성"))
    story.append(bullet("portfolio_state 원자적 저장"))
    story.append(bullet("Provider 종료"))
    story.append(PageBreak())

    # ═══ 11. STRATEGY LAB ═══
    story.append(h1("11. Strategy Lab (전략 실험실)"))
    story.append(p("10개 전략을 4개 그룹으로 나누어 비교 시뮬레이션을 수행합니다. 동일 그룹 내에서만 비교합니다."))
    story.append(sp(4))

    story.append(h2("11.1 전략 목록"))
    story.append(make_table(
        ["전략", "그룹", "설명", "리밸"],
        [
            ["momentum_base", "rebal", "순수 12-1 모멘텀 랭킹", "21일"],
            ["lowvol_momentum", "rebal", "LowVol + 모멘텀 (메인 전략)", "21일"],
            ["quality_factor", "rebal", "Quality 스크린 + 모멘텀", "21일"],
            ["hybrid_qscore", "rebal", "다팩터 복합 (Value+Mom+Quality)", "21일"],
            ["mean_reversion", "event", "RSI 과매도 (< 30) 진입", "이벤트"],
            ["liquidity_signal", "event", "거래량 급증 감지", "이벤트"],
            ["breakout_trend", "event", "20/50일 돌파 신호", "이벤트"],
            ["sector_rotation", "macro", "섹터 모멘텀 로테이션", "21일"],
            ["vol_regime", "regime", "변동성 레짐 적응형", "21일"],
        ],
        col_widths=[3.5*cm, 2*cm, 6*cm, 2*cm]
    ))
    story.append(sp(6))

    story.append(h2("11.2 Lab 실행"))
    story.append(code("python -m lab.run_lab --start 2026-03-01 --end 2026-04-08"))
    story.append(code("python -m lab.run_lab --group rebal"))
    story.append(code("python -m lab.run_lab --strategies momentum_base,lowvol_momentum"))
    story.append(PageBreak())

    # ═══ 12. AI ADVISOR ═══
    story.append(h1("12. AI Advisor (분석 전용)"))
    story.append(warn("Advisor는 <b>읽기 전용</b>입니다. 주문 실행, 상태 쓰기, 브로커 접근이 모두 금지됩니다."))
    story.append(sp(4))

    story.append(h2("12.1 분석 파이프라인"))
    story.append(bullet("1) Build Snapshot — equity_log + close_log → DailySnapshot"))
    story.append(bullet("2) Validate — 데이터 완전성 검증"))
    story.append(bullet("3) Analyze — PnL 분석, MDD contributor 분석"))
    story.append(bullet("4) Alert — 레짐 변화, 집중도, 운영 이상 감지"))
    story.append(bullet("5) Recommend — 파라미터 추천 (rebal_days, trail%)"))
    story.append(sp(4))

    story.append(h2("12.2 접근 제어"))
    story.append(make_table(
        ["권한", "설명"],
        [
            ["읽기 허용", "state/, report/output, signals/, logs/"],
            ["쓰기 허용", "advisor/output, advisor/cache, advisor/metrics"],
            ["금지", "주문 실행, state 쓰기, broker 접근, 자동 파라미터 적용"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(PageBreak())

    # ═══ 13. WEB DASHBOARD ═══
    story.append(h1("13. 웹 대시보드 (Web Dashboard)"))
    story.append(p("FastAPI 기반 모니터링 대시보드(73+ 엔드포인트)로, SSE(Server-Sent Events)를 통해 실시간 업데이트를 제공합니다. 3단계 Progressive Disclosure 모드(Basic → Operator → Debug)를 지원합니다."))
    story.append(sp(4))

    # 13.1 UI 모드
    story.append(h2("13.1 대시보드 UI 모드 (Progressive Disclosure)"))
    story.append(p("우측 상단 모드 전환 버튼으로 3단계 뷰를 제공합니다. 설정은 localStorage에 저장됩니다."))
    story.append(make_table(
        ["모드", "표시 패널", "대상 사용자"],
        [
            ["Basic", "Hero 상태 + 요약 카드 + 리밸 일정 + 레짐", "일반 모니터링"],
            ["Operator", "+ DD Guard + 리밸 버튼 + 보유종목 + 수익분석 + 거래내역", "운영자"],
            ["Debug", "+ DB Health + Control + Freshness + Traces + 테스트주문 + WebSocket + Sync + JSON + 히스토그램 + 로그 + Diff", "개발/디버그"],
        ],
        col_widths=[2.5*cm, 7*cm, 5.5*cm]
    ))
    story.append(sp(6))

    # 13.2 Dashboard Layout
    story.append(h2("13.2 대시보드 레이아웃 (15개 섹션)"))
    story.append(sp(2))
    story.append(h3("상단 영역"))
    story.append(bullet("<b>통합 네비게이션 + 시계</b> — 현재 시각, 모드 전환 버튼"))
    story.append(bullet("<b>SSE 연결 상태 바</b> — 실시간 연결 상태 표시 (연결/끊김/재연결)"))
    story.append(bullet("<b>Alert Banner</b> — 이벤트 발생 시 자동 표시 (trail stop, RECON 등)"))
    story.append(sp(2))
    story.append(h3("Hero 영역"))
    story.append(bullet("<b>Hero Status Panel</b> — health dot (GREEN/YELLOW/RED/BLACK), 상태 사유, 뱃지"))
    story.append(bullet("<b>4개 Summary Cards</b>: 보유종목 수 | 현금 | 일간 P&amp;L | 총자산"))
    story.append(sp(2))
    story.append(h3("메인 영역 (Operator+)"))
    story.append(bullet("<b>Rebalance Control Panel</b> — 상태 머신 표시 + 액션 버튼 (Preview/Sell/Buy/Skip)"))
    story.append(bullet("<b>DD Guard Gauges</b> — 일간/월간 DD 게이지 + BUY 허용 상태"))
    story.append(bullet("<b>Regime Display</b> — 오늘 실제 + 내일 예측 + gradient 바"))
    story.append(bullet("<b>Sector Regime Grid</b> — 섹터별 레짐 상태"))
    story.append(bullet("<b>AI Advisor Alerts</b> — Advisor 분석 결과 알림"))
    story.append(bullet("<b>Profit Analysis</b> — 기간별 탭 (일간/주간/월간 수익)"))
    story.append(bullet("<b>Trades Timeline</b> — 최근 거래 타임라인"))
    story.append(bullet("<b>Holdings List</b> — 듀얼 뷰: 종목별 + 섹터별 (도넛 차트)"))
    story.append(bullet("<b>Rebalance Preview</b> — 신규 진입 / 청산 / 유지 종목 미리보기"))
    story.append(sp(2))
    story.append(h3("Debug 영역 (Debug 모드)"))
    story.append(bullet("<b>8개 Debug 패널</b>: DB Health, Control Cards, Freshness Grid, Traces Table, 테스트 주문, WebSocket 상태, Sync 비교, JSON Viewer, Latency Histogram, Log Stream, State Diff"))
    story.append(PageBreak())

    # 13.3 Rebalance State Machine
    story.append(h2("13.3 Rebalance 상태 머신"))
    story.append(p("리밸런싱은 <b>8단계 상태 머신</b>으로 관리됩니다. 각 상태에서 활성화되는 버튼이 다릅니다."))
    story.append(sp(2))
    story.append(h3("상태 흐름"))
    story.append(code("IDLE -> WINDOW_OPEN -> PREVIEW_READY -> SELL_RUNNING -> SELL_COMPLETE\n  -> BUY_READY -> BUY_RUNNING -> BUY_COMPLETE -> IDLE\n  (에러: SKIPPED / BLOCKED)"))
    story.append(sp(4))

    story.append(h3("버튼별 동작"))
    story.append(make_table(
        ["버튼", "활성 상태", "동작"],
        [
            ["Preview (미리보기)", "WINDOW_OPEN, PREVIEW_READY", "스냅샷 생성 + preview_hash 잠금"],
            ["Sell (매도 실행)", "PREVIEW_READY (not blocked)", "매도 주문 실행 (preview_hash 필요)"],
            ["Buy (매수 실행)", "BUY_READY (not blocked)", "매수 주문 실행 (T+1 체크)"],
            ["Skip (건너뛰기)", "WINDOW_OPEN ~ BUY_READY", "이번 사이클 건너뛰기"],
        ],
        col_widths=[3.5*cm, 4.5*cm, 7*cm]
    ))
    story.append(sp(4))

    story.append(h3("Rebalance API 엔드포인트"))
    story.append(make_table(
        ["메서드", "경로", "설명"],
        [
            ["GET", "/api/rebalance/status", "상태 + phase + 버튼 활성화 정보"],
            ["GET", "/api/rebalance/preview", "스냅샷 생성, preview_hash 잠금"],
            ["POST", "/api/rebalance/sell", "매도 실행 (preview_hash 필수)"],
            ["POST", "/api/rebalance/buy", "매수 실행"],
            ["POST", "/api/rebalance/skip", "사이클 건너뛰기"],
            ["POST", "/api/rebalance/mode", "auto/manual 모드 토글"],
        ],
        col_widths=[2*cm, 4.5*cm, 8.5*cm]
    ))
    story.append(sp(6))

    # 13.4 Core API
    story.append(h2("13.4 Core API 엔드포인트"))
    story.append(h3("포트폴리오 & 상태"))
    story.append(make_table(
        ["메서드", "경로", "설명"],
        [
            ["GET", "/api/state", "전체 스냅샷 (health, token, latency, freshness, sync)"],
            ["GET", "/api/health", "빠른 상태 확인 (GREEN/YELLOW/RED/BLACK)"],
            ["GET", "/api/portfolio", "보유종목 + 섹터 요약"],
            ["GET", "/api/profit", "기간별 P&amp;L (일/주/월)"],
            ["GET", "/api/chart/today", "장중 포트폴리오 vs KOSPI"],
            ["GET", "/api/trades/recent", "최근 거래 내역"],
        ],
        col_widths=[2*cm, 4*cm, 9*cm]
    ))
    story.append(sp(2))
    story.append(h3("검증 & 모니터링"))
    story.append(make_table(
        ["메서드", "경로", "설명"],
        [
            ["GET", "/api/crosscheck/today", "3-way 검증 (Gen4 vs REST_DB vs Broker)"],
            ["GET", "/api/traces", "요청 이력 (500건 링 버퍼)"],
            ["GET", "/api/latency-histogram", "지연 시간 분포 차트"],
            ["GET", "/api/logs", "로그 파일 조회"],
            ["GET", "/api/db/health", "PostgreSQL 테이블 통계"],
        ],
        col_widths=[2*cm, 4.5*cm, 8.5*cm]
    ))
    story.append(sp(6))

    # 13.5 SSE Streams
    story.append(h2("13.5 SSE 실시간 스트림"))
    story.append(make_table(
        ["경로", "간격", "설명"],
        [
            ["/sse/state", "5초 or 변경 시", "포트폴리오 상태 업데이트"],
            ["/sse/lab", "실시간", "Lab 시뮬레이터 업데이트"],
            ["/sse/lab-live", "실시간", "Forward trading 업데이트"],
            ["/sse/surge", "실시간", "SURGE 스캐너 업데이트"],
            ["/sse/traces", "실시간", "요청 트레이스 업데이트"],
            ["/sse/health", "실시간", "건강 상태 업데이트"],
        ],
        col_widths=[3.5*cm, 3*cm, 8.5*cm]
    ))
    story.append(sp(6))

    # 13.6 Health & Freshness
    story.append(h2("13.6 Health & Freshness 모니터링"))
    story.append(h3("Health 상태 (api_state.py)"))
    story.append(make_table(
        ["상태", "색상", "조건"],
        [
            ["GREEN", "녹색", "모든 데이터 소스 정상"],
            ["YELLOW", "노란색", "일부 데이터 stale 또는 지연"],
            ["RED", "빨간색", "주요 데이터 소스 장애"],
            ["BLACK", "검은색", "연결 끊김"],
        ],
        col_widths=[3*cm, 3*cm, 9*cm]
    ))
    story.append(sp(2))
    story.append(h3("Freshness (5개 데이터 소스)"))
    story.append(make_table(
        ["상태", "의미"],
        [
            ["FRESH", "데이터 갱신 시각이 정상 범위"],
            ["WARN", "갱신 지연 감지"],
            ["STALE", "데이터 오래됨 (주의 필요)"],
            ["NEVER", "한 번도 갱신되지 않음"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(PageBreak())

    # 13.7 Lab Simulator
    story.append(h2("13.7 Lab Simulator (3전략 비교)"))
    story.append(p("web/lab_simulator.py + lab_realtime.py에서 3개 전략을 동시 시뮬레이션합니다."))
    story.append(sp(2))
    story.append(h3("3개 전략"))
    story.append(make_table(
        ["전략", "유형", "홀딩", "TP", "SL", "특징"],
        [
            ["A (Conservative)", "보수", "1일", "1.0%", "-0.5%", "기본"],
            ["B (Aggressive)", "공격", "3일", "2.0%", "-1.0%", "2x 포지션"],
            ["C (Dynamic)", "동적", "가변", "1.5%", "확대 ~6%", "트레일링 스톱"],
        ],
        col_widths=[3*cm, 1.5*cm, 1.5*cm, 1.5*cm, 1.5*cm, 4*cm]
    ))
    story.append(sp(2))
    story.append(p("<b>12개 파라미터</b>: ranking source, top_n, entry_threshold, TP/SL (A/B/C), trail_max, position sizing"))
    story.append(sp(2))
    story.append(h3("Lab API 엔드포인트"))
    story.append(make_table(
        ["메서드", "경로", "설명"],
        [
            ["GET", "/api/lab/params", "파라미터 범위 조회"],
            ["GET", "/api/lab/ranking", "현재 시장 랭킹"],
            ["POST", "/api/lab/simulate", "오프라인 시뮬레이션 실행"],
            ["POST", "/api/lab/realtime/start", "WebSocket 기반 실시간 시뮬레이션 시작"],
            ["GET", "/api/lab/realtime/state", "실시간 상태 (라이브 가격 포함)"],
            ["POST", "/api/lab/realtime/stop", "시뮬레이션 종료 + 결과 반환"],
            ["GET", "/api/lab/history", "과거 결과 조회"],
            ["POST", "/api/lab/live/start", "Forward trading 세션 시작"],
            ["POST", "/api/lab/live/run-daily", "장 시작 시 자동 실행"],
        ],
        col_widths=[2*cm, 5*cm, 8*cm]
    ))
    story.append(sp(6))

    # 13.8 Surge Scanner
    story.append(h2("13.8 SURGE Scanner (급등 감지)"))
    story.append(p("web/surge/ 디렉토리에서 분봉 기반 급등 패턴을 실시간 감지합니다."))
    story.append(sp(2))
    story.append(h3("워크플로"))
    story.append(bullet("1) 랭킹 조회 → 필터 (가격, 변동률)"))
    story.append(bullet("2) 거래량/거래강도 보강 (Enrich)"))
    story.append(bullet("3) 포지션 진입"))
    story.append(bullet("4) WebSocket 실시간 모니터링"))
    story.append(bullet("5) TP/SL/TRAIL 조건 충족 시 청산"))
    story.append(sp(2))
    story.append(h3("Surge API 엔드포인트"))
    story.append(make_table(
        ["메서드", "경로", "설명"],
        [
            ["GET", "/api/surge/params", "설정값 조회"],
            ["POST", "/api/surge/start", "스캔 시작"],
            ["GET", "/api/surge/state", "실시간 상태"],
            ["POST", "/api/surge/stop", "세션 종료"],
            ["GET", "/api/surge/trades", "거래 이력"],
            ["GET", "/api/surge/summary", "세션 P&amp;L 요약"],
        ],
        col_widths=[2*cm, 4*cm, 9*cm]
    ))
    story.append(sp(6))

    # 13.9 Cross-Validation
    story.append(h2("13.9 Cross-Validation (3-Way 검증)"))
    story.append(p("web/cross_validator.py에서 Gen4 state vs REST_DB vs Broker 3자 비교를 수행합니다."))
    story.append(sp(2))
    story.append(h3("Diff Taxonomy (10종)"))
    story.append(make_table(
        ["Diff 타입", "설명"],
        [
            ["Timing", "시간차에 의한 일시적 불일치"],
            ["CodeSet", "종목 집합 차이"],
            ["Qty", "수량 불일치"],
            ["Cash", "현금 차이"],
            ["AvgPrice", "평균 매수가 차이"],
            ["OpenOrders", "미체결 주문 차이"],
            ["Version", "버전 불일치"],
            ["Partial", "부분 체결에 의한 차이"],
            ["Degraded", "데이터 소스 열화"],
            ["Critical", "심각한 불일치 (즉시 조치 필요)"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(sp(2))
    story.append(h3("핵심 지표"))
    story.append(bullet("<b>strict_zero_rate</b>: 완전 일치 비율"))
    story.append(bullet("<b>eligible_zero_rate</b>: Timing 제외 일치 비율"))
    story.append(bullet("<b>timing_rate</b>: Timing diff 비율"))
    story.append(bullet("<b>critical_count</b>: Critical diff 수 (0이어야 정상)"))
    story.append(PageBreak())

    # ═══ 14. REPORTING ═══
    story.append(h1("14. 리포팅 (Reporting)"))

    story.append(h2("14.1 CSV 출력 파일"))
    story.append(make_table(
        ["파일", "내용"],
        [
            ["trades.csv", "모든 매수/매도 체결 기록"],
            ["close_log.csv", "청산 포지션 상세 (진입가, 청산가, 수익률, 보유일)"],
            ["equity_log.csv", "일간 자산 스냅샷 (equity, cash, PnL, DD, regime)"],
            ["decision_log.csv", "매매 결정 컨텍스트 (포렌식용)"],
            ["reconcile_log.csv", "Broker 동기화 차이 기록"],
            ["daily_positions.csv", "일간 포지션 스냅샷"],
        ],
        col_widths=[4*cm, 11*cm]
    ))
    story.append(sp(6))

    story.append(h2("14.2 모드별 출력 디렉토리"))
    story.append(make_table(
        ["모드", "디렉토리"],
        [
            ["Live / Paper", "report/output/"],
            ["Paper Test", "report/output_test/"],
            ["Shadow Test", "report/output_shadow/"],
        ],
        col_widths=[4*cm, 11*cm]
    ))
    story.append(PageBreak())

    # ═══ 15. REGIME PREDICTION ═══
    story.append(h1("15. 레짐 예측 (Regime Prediction)"))
    story.append(p("regime/ 디렉토리에서 글로벌 + 국내 지표를 수집하여 내일의 레짐을 예측합니다."))
    story.append(sp(4))

    story.append(h2("15.1 데이터 소스"))
    story.append(make_table(
        ["카테고리", "지표"],
        [
            ["글로벌", "S&P 500, NASDAQ, VIX, USD/KRW"],
            ["국내", "KOSPI, KOSDAQ, 거래강도"],
            ["개별", "200일 이동평균, breadth (종목 비중)"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(sp(4))

    story.append(h2("15.2 복합 스코어 산출"))
    story.append(bullet("Global Score (S&P + NASDAQ 추세)"))
    story.append(bullet("Vol Score (VIX 수준)"))
    story.append(bullet("Domestic Score (KOSPI vs MA200)"))
    story.append(bullet("Micro Score (거래강도)"))
    story.append(bullet("FX Score (USD/KRW 추세)"))
    story.append(bullet("Composite = 가중 합산 → BULL(>0.6) / BEAR(<0.4) / SIDE"))
    story.append(PageBreak())

    # ═══ 16. NOTIFICATIONS ═══
    story.append(h1("16. 알림 시스템 (Notifications)"))

    story.append(h2("16.1 Telegram"))
    story.append(p("notify/telegram_bot.py를 통해 주요 이벤트를 Telegram으로 즉시 전송합니다."))
    story.append(make_table(
        ["이벤트", "설명"],
        [
            ["SAFE_MODE 레벨 변화", "DD 안전 모드 진입/해제"],
            ["REBAL_BLOCKED", "리밸런싱 매수 차단"],
            ["Trail Stop 경고", "트레일 근접 종목"],
            ["Trail Stop 발동", "종가 기준 매도 실행"],
            ["일간 요약", "EOD 자산/수익/포지션 요약"],
        ],
        col_widths=[4*cm, 11*cm]
    ))
    story.append(sp(6))

    story.append(h2("16.2 Kakao Talk"))
    story.append(p("notify/kakao_notify.py: Telegram 장애 시 Kakao Talk으로 fallback 전송합니다."))
    story.append(PageBreak())

    # ═══ 17. BACKTESTING ═══
    story.append(h1("17. 백테스트 (Backtesting)"))
    story.append(p("backtest/backtester.py에서 과거 OHLCV를 리플레이하여 전략 성과를 검증합니다."))
    story.append(sp(4))

    story.append(h2("17.1 검증 결과 (2026-03-22)"))
    story.append(make_table(
        ["테스트", "결과", "판정"],
        [
            ["7년 백테스트", "+208.6%, CAGR 17.5%, MDD -21.9%, Sharpe 1.26", "PASS"],
            ["OOS (2023~2026)", "CAGR 18.7%", "PASS (>= 15%)"],
            ["슬리피지 x2", "Sharpe 1.69", "PASS (>= 1.0)"],
            ["BEAR 구간", "MDD -18.6%", "PASS (<= -25%)"],
            ["Survivorship", "동일 성과", "PASS"],
            ["유니버스 Top200", "CAGR 10.2%", "WARNING (500+ 필수)"],
            ["12-1 모멘텀 의존", "다른 윈도우 대비 압도적", "WARNING (파라미터 민감)"],
        ],
        col_widths=[4*cm, 7*cm, 4*cm]
    ))
    story.append(PageBreak())

    # ═══ 18. OPERATIONS GUIDE ═══
    story.append(h1("18. 운영 가이드 (Operations Guide)"))

    story.append(h2("18.1 일일 워크플로"))
    story.append(h3("장 시작 전 (08:30 ~ 09:00)"))
    story.append(code("python main.py --batch                    # 데이터 업데이트 + 목표 산출"))
    story.append(code("cat state/signals/target_portfolio_*.json  # 목표 포트폴리오 확인"))
    story.append(code("python main.py --live                     # 라이브 매매 시작 (09:00)"))
    story.append(sp(4))

    story.append(h3("장중 (09:00 ~ 15:30)"))
    story.append(bullet("웹 대시보드 모니터링: http://localhost:8080"))
    story.append(bullet("로그 출력에서 trail 경고 확인"))
    story.append(bullet("일반적으로 수동 개입 불필요"))
    story.append(sp(4))

    story.append(h3("장 마감 후 (15:30+)"))
    story.append(bullet("EOD phase 자동 완료"))
    story.append(bullet("리포트 생성: report/output/*.csv"))
    story.append(bullet("equity_log.csv에서 일간 P&L 확인"))
    story.append(sp(6))

    story.append(h2("18.2 실행 명령어 요약"))
    story.append(make_table(
        ["명령어", "설명"],
        [
            ["python main.py --batch", "일간 데이터 업데이트 + 목표 산출"],
            ["python main.py --live", "라이브 매매 (5단계 오케스트레이션)"],
            ["python main.py --mock", "Mock 시뮬레이션"],
            ["python main.py --backtest", "백테스트 실행"],
            ["python main.py --paper-test", "Paper 테스트 (격리, fast reentry)"],
        ],
        col_widths=[5.5*cm, 9.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("18.3 안전 규칙"))
    story.append(bullet("<b>Broker = Truth</b>: RECON 결과가 최종 기준"))
    story.append(bullet("<b>SELL always allowed</b>: 매도는 항상 허용"))
    story.append(bullet("<b>TIMEOUT != failure</b>: 서버 미응답 != 미체결 없음"))
    story.append(bullet("<b>State backward-compatible</b>: old JSON → new JSON 로드 가능"))
    story.append(bullet("<b>Engine layer protected</b>: scoring.py, config.py 파라미터 변경 금지"))
    story.append(PageBreak())

    # ═══ 19. TROUBLESHOOTING ═══
    story.append(h1("19. 트러블슈팅 (Troubleshooting)"))

    story.append(h2("19.1 주문 실패"))
    story.append(bullet("equity_log.csv → risk_mode 컬럼 확인 (DD block?)"))
    story.append(bullet("trades.csv → 실행 상세 확인"))
    story.append(bullet("logs/ → REST API 에러 확인"))
    story.append(sp(4))

    story.append(h2("19.2 가격 Stale"))
    story.append(bullet("증상: Trail stop 미발동"))
    story.append(bullet("확인: monitor_phase.py 실시간 tick 등록 상태"))
    story.append(bullet("해결: WebSocket 재연결 또는 재시작"))
    story.append(sp(4))

    story.append(h2("19.3 Reconciliation 불일치"))
    story.append(bullet("확인: decision_log.csv → reconcile_corrections 컬럼"))
    story.append(bullet("해결: 지속 시 --fresh 옵션으로 재시작"))
    story.append(sp(4))

    story.append(h2("19.4 상태 파일 손상"))
    story.append(bullet("증상: JSON 파싱 실패"))
    story.append(bullet("해결: .backup 파일에서 복원"))
    story.append(warn("State 파일 삭제 금지 — 백업 후 신규 생성만 허용"))
    story.append(PageBreak())

    # ═══ 20. EXPANSION ROADMAP ═══
    story.append(h1("20. 확장 로드맵 (Expansion Roadmap)"))
    story.append(p("현재 시스템을 기반으로 한 향후 확장 방향입니다."))
    story.append(sp(6))

    story.append(h2("20.1 우선순위 높음"))
    story.append(bullet("리밸런싱 자동화 완성 (주문 순서: 매도→매수, T+2 정산)"))
    story.append(bullet("LIVE 모드 trail stop 모니터링 (WebSocket 실시간 가격)"))
    story.append(bullet("리스크 관리 자동화 (DD 단계화, SAFE MODE) — Step 5"))
    story.append(bullet("재시작 시 미체결 주문 일괄 취소 (OVERFILL 근본 원인 제거)"))
    story.append(sp(4))

    story.append(h2("20.2 우선순위 보통"))
    story.append(bullet("batch → pykrx 증분 업데이트 연동"))
    story.append(bullet("월초 리밸런싱 예외처리 (거래정지/관리종목/편입불가)"))
    story.append(bullet("리포트 HTML 생성기"))
    story.append(bullet("Dirty exit 감지 + recovery path"))
    story.append(bullet("Monitor-only mode (복원 조건 충족 전)"))
    story.append(sp(4))

    story.append(h2("20.3 자동화 로드맵"))
    story.append(bullet("BIOS Wake-on-RTC (08:00) → 자동 로그인"))
    story.append(bullet("키움 실행 → pyautogui 비밀번호 입력"))
    story.append(bullet("LIVE bat → EOD(15:30) → 배치(16:00) → PC 종료(17:00)"))
    story.append(bullet("카카오 알림: trail stop, RECON 이상, SAFE_MODE, 리밸 완료"))
    story.append(sp(4))

    story.append(h2("20.4 전략 고도화"))
    story.append(bullet("Strategy Lab 결과 기반 전략 개선"))
    story.append(bullet("분봉 활용 Phase 2~5 (데이터 축적 후)"))
    story.append(bullet("Advisor Phase 3: 자동 파라미터 추천 + 시나리오 비교"))
    story.append(bullet("Gen5+ 검토 (Lab 9전략 기반)"))
    story.append(sp(4))

    story.append(h2("20.5 인프라"))
    story.append(bullet("네트워크 장애 단계적 대응"))
    story.append(bullet("Cross-validation Phase 3 (Gen4 vs REST 완전 비교)"))
    story.append(bullet("백테스트 개별종목 조회 방식 전환 (매트릭스 ffill 제거)"))

    # ── Build ──
    doc.build(story, onFirstPage=page_header_footer, onLaterPages=page_header_footer)
    print(f"PDF generated: {out_path}")
    return out_path


if __name__ == "__main__":
    build()
