#!/usr/bin/env python3
"""Q-TRON US 1.0 - 상세 매뉴얼 PDF 생성"""

import os, sys
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm, cm
from reportlab.lib.colors import HexColor, black, white, gray
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, KeepTogether, HRFlowable,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ── Fonts ──
FONT_PATH = "C:/Windows/Fonts/malgun.ttf"
FONT_BOLD_PATH = "C:/Windows/Fonts/malgunbd.ttf"
pdfmetrics.registerFont(TTFont("MalgunGothic", FONT_PATH))
pdfmetrics.registerFont(TTFont("MalgunGothicBold", FONT_BOLD_PATH))

# ── Colors ──
C_PRIMARY   = HexColor("#0d47a1")
C_SECONDARY = HexColor("#1565c0")
C_ACCENT    = HexColor("#1976d2")
C_BG_LIGHT  = HexColor("#e3f2fd")
C_RED       = HexColor("#c62828")
C_GREEN     = HexColor("#2e7d32")
C_ORANGE    = HexColor("#e65100")
C_GRAY      = HexColor("#757575")
C_TABLE_HDR = HexColor("#0d47a1")
C_TABLE_ALT = HexColor("#f0f4ff")

# ── Styles ──
def make_style(name, fontName="MalgunGothic", fontSize=10, leading=14,
               textColor=black, alignment=TA_LEFT, spaceBefore=0,
               spaceAfter=4, leftIndent=0, bold=False):
    fn = "MalgunGothicBold" if bold else fontName
    return ParagraphStyle(name, fontName=fn, fontSize=fontSize, leading=leading,
                          textColor=textColor, alignment=alignment,
                          spaceBefore=spaceBefore, spaceAfter=spaceAfter,
                          leftIndent=leftIndent, wordWrap='CJK')

S_COVER_TITLE = make_style("CoverTitle", fontSize=28, leading=36, textColor=C_PRIMARY, alignment=TA_CENTER, bold=True)
S_COVER_SUB   = make_style("CoverSub", fontSize=14, leading=20, textColor=C_SECONDARY, alignment=TA_CENTER)
S_H1   = make_style("H1", fontSize=18, leading=24, textColor=C_PRIMARY, spaceBefore=20, spaceAfter=10, bold=True)
S_H2   = make_style("H2", fontSize=14, leading=19, textColor=C_SECONDARY, spaceBefore=14, spaceAfter=6, bold=True)
S_H3   = make_style("H3", fontSize=12, leading=16, textColor=C_ACCENT, spaceBefore=10, spaceAfter=4, bold=True)
S_BODY = make_style("Body", fontSize=9.5, leading=14, spaceAfter=4, alignment=TA_JUSTIFY)
S_CODE = make_style("Code", fontName="Courier", fontSize=8, leading=11, textColor=HexColor("#333333"), leftIndent=10)
S_BULLET = make_style("Bullet", fontSize=9.5, leading=14, leftIndent=15, spaceAfter=2)
S_NOTE   = make_style("Note", fontSize=8.5, leading=12, textColor=C_ORANGE, leftIndent=10, spaceBefore=4, spaceAfter=4)
S_TOC_H1 = make_style("TOCH1", fontSize=11, leading=15, bold=True, spaceBefore=6, spaceAfter=2)

# ── Helpers ──
def h1(t):  return Paragraph(t, S_H1)
def h2(t):  return Paragraph(t, S_H2)
def h3(t):  return Paragraph(t, S_H3)
def p(t):   return Paragraph(t, S_BODY)
def code(t): return Paragraph(t.replace("\n","<br/>").replace(" ","&nbsp;"), S_CODE)
def bullet(t): return Paragraph(f"&bull; {t}", S_BULLET)
def note(t): return Paragraph(f"<b>NOTE:</b> {t}", S_NOTE)
def warn(t): return Paragraph(f"<b>WARNING:</b> {t}", S_NOTE)
def sp(h=6): return Spacer(1, h)

def make_table(headers, rows, col_widths=None):
    data = [headers] + rows
    w = col_widths or [None]*len(headers)
    t = Table(data, colWidths=w, repeatRows=1)
    cmds = [
        ('BACKGROUND', (0,0), (-1,0), C_TABLE_HDR),
        ('TEXTCOLOR', (0,0), (-1,0), white),
        ('FONTNAME', (0,0), (-1,0), 'MalgunGothicBold'),
        ('FONTSIZE', (0,0), (-1,0), 8.5),
        ('FONTNAME', (0,1), (-1,-1), 'MalgunGothic'),
        ('FONTSIZE', (0,1), (-1,-1), 8),
        ('LEADING', (0,0), (-1,-1), 11),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('GRID', (0,0), (-1,-1), 0.3, C_GRAY),
        ('TOPPADDING', (0,0), (-1,-1), 3),
        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
        ('LEFTPADDING', (0,0), (-1,-1), 5),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            cmds.append(('BACKGROUND', (0,i), (-1,i), C_TABLE_ALT))
    t.setStyle(TableStyle(cmds))
    return t

def page_hf(canvas, doc):
    canvas.saveState()
    canvas.setFont("MalgunGothicBold", 8)
    canvas.setFillColor(C_GRAY)
    canvas.drawString(2*cm, A4[1]-1.2*cm, "Q-TRON US 1.0  |  US Market Manual v1.0")
    canvas.drawRightString(A4[0]-2*cm, A4[1]-1.2*cm, "2026-04-12")
    canvas.setStrokeColor(C_BG_LIGHT)
    canvas.line(2*cm, A4[1]-1.4*cm, A4[0]-2*cm, A4[1]-1.4*cm)
    canvas.setFont("MalgunGothic", 8)
    canvas.drawCentredString(A4[0]/2, 1.2*cm, f"- {doc.page} -")
    canvas.restoreState()


def build():
    out_path = os.path.join(os.path.dirname(__file__), "Q-TRON_US_Manual_v1.0.pdf")
    doc = SimpleDocTemplate(out_path, pagesize=A4,
                            topMargin=2*cm, bottomMargin=2*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    story = []

    # ── COVER ──
    story.append(Spacer(1, 6*cm))
    story.append(Paragraph("Q-TRON US 1.0", S_COVER_TITLE))
    story.append(sp(10))
    story.append(Paragraph("US Market Trading System", make_style("cs2", fontSize=20, leading=26, textColor=C_ACCENT, alignment=TA_CENTER)))
    story.append(sp(6))
    story.append(Paragraph("상세 운영 매뉴얼 v1.0", S_COVER_SUB))
    story.append(sp(30))
    story.append(Paragraph("2026-04-12", make_style("cd", fontSize=11, leading=14, textColor=C_GRAY, alignment=TA_CENTER)))
    story.append(Paragraph("Confidential  |  Internal Use Only", make_style("cc", fontSize=9, leading=12, textColor=C_GRAY, alignment=TA_CENTER)))
    story.append(PageBreak())

    # ── TOC ──
    story.append(h1("목차 (Table of Contents)"))
    story.append(sp(6))
    toc = [
        ("1", "시스템 개요 (System Overview)"),
        ("2", "디렉토리 구조 (Directory Structure)"),
        ("3", "설정 파라미터 (Configuration)"),
        ("4", "실행 모드 (Execution Modes)"),
        ("5", "전략 레이어 (Strategy Layer)"),
        ("6", "코어 레이어 (Core Layer)"),
        ("7", "데이터 레이어 (Data Layer)"),
        ("8", "리스크 관리 (Risk Management)"),
        ("9", "Alpaca API 통합 (Broker Integration)"),
        ("10", "Telegram 알림 (Notifications)"),
        ("11", "웹 대시보드 (Web Dashboard)"),
        ("12", "레짐 예측 (Regime Prediction)"),
        ("13", "Strategy Lab (백테스트)"),
        ("14", "데이터베이스 (PostgreSQL)"),
        ("15", "운영 가이드 (Operations Guide)"),
        ("16", "KR vs US 비교"),
        ("17", "확장 로드맵 (Expansion Roadmap)"),
    ]
    for n, t in toc:
        story.append(Paragraph(f"{n}.  {t}", S_TOC_H1))
    story.append(PageBreak())

    # ═══ 1. SYSTEM OVERVIEW ═══
    story.append(h1("1. 시스템 개요 (System Overview)"))
    story.append(p("Q-TRON US 1.0은 미국 주식시장(S&amp;P 500)에서 <b>LowVol + Momentum 12-1</b> 전략을 실행하는 자동매매 시스템입니다. Alpaca Paper Trading API를 통해 주문을 실행하며, PostgreSQL 데이터베이스, Telegram 알림, FastAPI 대시보드를 지원합니다."))
    story.append(sp(4))

    story.append(h2("1.1 핵심 전략 요약"))
    story.append(make_table(
        ["항목", "값", "설명"],
        [
            ["유니버스", "S&P 500", "~500 종목 (Wikipedia 스크래핑)"],
            ["변동성 필터", "하위 20%ile (252일)", "저변동성 종목 선별"],
            ["모멘텀 랭킹", "12-1개월 수익률", "최근 1개월 제외"],
            ["포트폴리오", "20종목 균등배분", "리밸런싱 시 동일 비중"],
            ["리밸런싱", "21거래일 (월간)", "매도 우선 → 매수"],
            ["청산", "Trailing Stop -12%", "종가 기준, HWM 갱신"],
            ["비용", "0.05% 슬리피지", "수수료 0% (Alpaca)"],
            ["데이터", "yfinance + PostgreSQL", "OHLCV 2년치"],
        ],
        col_widths=[3*cm, 4*cm, 8*cm]
    ))
    story.append(sp(6))

    story.append(h2("1.2 아키텍처"))
    story.append(bullet("<b>Core</b> — PortfolioManagerUS, StateManagerUS"))
    story.append(bullet("<b>Strategy</b> — scoring, factor_ranker, rebalancer, trail_stop, execution_gate"))
    story.append(bullet("<b>Data</b> — AlpacaProvider, USDataCollector, DbProviderUS, UniverseBuilder"))
    story.append(bullet("<b>Regime</b> — SPY/VIX/섹터 기반 예측"))
    story.append(bullet("<b>Lab</b> — 10개 전략 백테스트 프레임워크"))
    story.append(bullet("<b>Web</b> — FastAPI 대시보드 (port 8081)"))
    story.append(bullet("<b>Notify</b> — Telegram 알림"))
    story.append(sp(6))

    story.append(h2("1.3 핵심 설계 원칙"))
    story.append(bullet("<b>Broker = Truth</b>: Alpaca 보유종목이 최종 기준"))
    story.append(bullet("<b>Single Writer Pattern</b>: 메인 루프만 포트폴리오 수정"))
    story.append(bullet("<b>Atomic State</b>: JSON tmp → verify → bak → rename"))
    story.append(bullet("<b>Snapshot Consistency</b>: scoring과 execution 동일 snapshot_id 강제"))
    story.append(bullet("<b>5-Gate Execution</b>: 주문 전 5단계 안전 검증"))
    story.append(PageBreak())

    # ═══ 2. DIRECTORY STRUCTURE ═══
    story.append(h1("2. 디렉토리 구조 (Directory Structure)"))
    dirs = [
        ["config.py", "USConfig 데이터클래스 (모든 파라미터)"],
        ["main.py", "CLI 진입점 (--test, --batch, --live, --server)"],
        ["core/", "portfolio_manager.py + state_manager.py"],
        ["strategy/", "scoring, factor_ranker, rebalancer, trail_stop, execution_gate, snapshot_guard"],
        ["data/", "alpaca_provider, alpaca_data (yfinance), db_provider (PostgreSQL), universe_builder"],
        ["regime/", "SPY/VIX/섹터 기반 레짐 감지 + 예측"],
        ["lab/", "10개 전략 Lab 엔진 + metrics + job_store"],
        ["web/", "FastAPI 대시보드 (app.py + templates + static)"],
        ["notify/", "Telegram 알림 (telegram_bot.py)"],
        ["state/", "JSON 상태 파일 (portfolio + runtime)"],
        ["logs/", "애플리케이션 로그"],
        ["report/", "리포트 출력"],
    ]
    story.append(make_table(["경로", "설명"], dirs, col_widths=[3.5*cm, 11.5*cm]))
    story.append(PageBreak())

    # ═══ 3. CONFIGURATION ═══
    story.append(h1("3. 설정 파라미터 (Configuration)"))
    story.append(p("<b>config.py</b>의 <b>USConfig</b> 데이터클래스에서 모든 파라미터를 관리합니다."))
    story.append(sp(4))

    story.append(h2("3.1 전략 파라미터"))
    story.append(make_table(
        ["파라미터", "값", "설명"],
        [
            ["VOL_LOOKBACK", "252", "변동성 계산 윈도우 (12개월)"],
            ["VOL_PERCENTILE", "0.20", "저변동성 필터 (하위 20%)"],
            ["MOM_LOOKBACK", "252", "모멘텀 계산 윈도우 (12개월)"],
            ["MOM_SKIP", "22", "최근 1개월 제외"],
            ["N_STOCKS", "20", "포트폴리오 목표 종목 수"],
            ["REBAL_DAYS", "21", "리밸런싱 주기"],
            ["TRAIL_PCT", "0.12", "트레일링 스톱 -12%"],
            ["CASH_BUFFER_RATIO", "0.95", "매수 시 현금 95%만 사용"],
        ],
        col_widths=[4*cm, 2*cm, 9*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.2 비용 모델 (Alpaca Zero-Commission)"))
    story.append(make_table(
        ["구분", "수수료", "슬리피지", "세금", "합계"],
        [
            ["매수 (BUY)", "0%", "0.05%", "-", "0.05%"],
            ["매도 (SELL)", "0%", "0.05%", "0% (Paper)", "0.05%"],
        ],
        col_widths=[3*cm, 2.5*cm, 2.5*cm, 3*cm, 2*cm]
    ))
    story.append(note("Alpaca Paper Trading은 수수료와 세금이 없습니다. 슬리피지만 적용됩니다."))
    story.append(sp(6))

    story.append(h2("3.3 리스크 파라미터"))
    story.append(make_table(
        ["파라미터", "값", "동작"],
        [
            ["DAILY_DD_LIMIT", "-4%", "일간 DD 한도 → 매수 차단"],
            ["MONTHLY_DD_LIMIT", "-7%", "월간 DD 한도 → 매수 차단"],
            ["DD_CAUTION", "-5%", "매수 70% 축소"],
            ["DD_WARNING", "-10%", "매수 50% 축소"],
            ["DD_CRITICAL", "-15%", "매수 전면 차단"],
            ["DD_SEVERE", "-20%", "매수 차단 + 20% 청산"],
            ["DD_SAFE_MODE", "-25%", "SAFE MODE 진입"],
        ],
        col_widths=[4*cm, 2*cm, 9*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.4 유니버스 필터"))
    story.append(make_table(
        ["필터", "값", "설명"],
        [
            ["대상", "S&P 500", "Wikipedia 스크래핑"],
            ["UNIV_MIN_CLOSE", "$5", "최소 주가"],
            ["UNIV_MIN_AMOUNT", "$10M", "일간 최소 거래대금"],
            ["UNIV_MIN_HISTORY", "260일", "최소 거래 이력"],
            ["UNIV_MAX_CANDIDATES", "300", "유니버스 최대 후보"],
        ],
        col_widths=[4*cm, 3*cm, 8*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.5 시장 시간 & 자본"))
    story.append(make_table(
        ["항목", "값"],
        [
            ["Market Open", "09:30 ET"],
            ["Market Close", "16:00 ET"],
            ["초기 자본", "$100,000"],
            ["Trading Mode", "paper (default)"],
            ["Alpaca Base URL", "https://paper-api.alpaca.markets"],
        ],
        col_widths=[4*cm, 11*cm]
    ))
    story.append(sp(6))

    story.append(h2("3.6 Fill 안전 파라미터"))
    story.append(make_table(
        ["파라미터", "값", "설명"],
        [
            ["FILL_TIMEOUT_SEC", "30", "주문 타임아웃"],
            ["MAX_GHOST_AGE_SEC", "300", "Ghost 주문 RECON 임계"],
            ["GHOST_RECONCILE_INTERVAL_SEC", "60", "RECON 체크 간격"],
            ["SNAPSHOT_MAX_STALE_HOURS", "24", "스코어링 데이터 유효 기간"],
        ],
        col_widths=[5.5*cm, 2*cm, 7.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 4. EXECUTION MODES ═══
    story.append(h1("4. 실행 모드 (Execution Modes)"))

    story.append(h2("4.1 Test 모드 (--test)"))
    story.append(p("연결 검증용. Alpaca API 헬스 체크, PostgreSQL 테이블 확인, 가격 조회 테스트를 수행합니다."))
    story.append(code("python main.py --test"))
    story.append(sp(2))
    story.append(bullet("Alpaca API: clock, account, positions 조회"))
    story.append(bullet("PostgreSQL: 테이블 통계 (ohlcv_us, sector_map_us 등)"))
    story.append(bullet("가격 테스트: AAPL 현재가 조회"))
    story.append(sp(6))

    story.append(h2("4.2 Batch 모드 (--batch)"))
    story.append(p("일간 데이터 업데이트 및 목표 포트폴리오 산출:"))
    story.append(code("python main.py --batch"))
    story.append(sp(2))
    story.append(bullet("<b>Step 1</b>: get_sp500_tickers() — Wikipedia에서 S&amp;P 500 목록 스크래핑"))
    story.append(bullet("<b>Step 2</b>: collect_ohlcv(symbols, period='2y') — yfinance로 OHLCV 다운로드 → DB upsert"))
    story.append(bullet("<b>Step 3</b>: collect_index(['SPY','QQQ','IWM']) — 인덱스 데이터 수집"))
    story.append(bullet("<b>Step 4</b>: build_universe(db, config) — 유동성/이력 필터 적용"))
    story.append(bullet("<b>Step 5</b>: load_close_dict(min_history=272) — DB에서 종가 시리즈 로드"))
    story.append(bullet("<b>Step 6</b>: 전 종목 calc_volatility() + calc_momentum()"))
    story.append(bullet("<b>Step 7</b>: select_top_n() — LowVol 하위 20%ile → 모멘텀 Top 20"))
    story.append(bullet("<b>Step 8</b>: save_target_portfolio(target, snapshot_id) → DB 저장"))
    story.append(bullet("<b>Step 9</b>: Telegram 알림 전송"))
    story.append(sp(6))

    story.append(h2("4.3 Live 모드 (--live)"))
    story.append(p("실시간 모니터링 + trail stop 실행 + RECON:"))
    story.append(code("python main.py --live"))
    story.append(sp(2))
    story.append(h3("Phase 0: Connect"))
    story.append(bullet("Alpaca API 연결 확인"))
    story.append(bullet("서버 타입 로그 (PAPER/LIVE)"))
    story.append(sp(2))
    story.append(h3("Phase 1: Broker Snapshot"))
    story.append(bullet("계좌 조회 (equity, cash, buying power)"))
    story.append(bullet("보유종목 조회 + 미체결 주문 조회"))
    story.append(sp(2))
    story.append(h3("Phase 2: State Load + RECON"))
    story.append(bullet("JSON 상태 파일 로드 (또는 fresh 생성)"))
    story.append(bullet("Dirty exit 감지 (started_at 있지만 shutdown_at 없음)"))
    story.append(bullet("pending_sell_qty와 broker open orders 동기화"))
    story.append(bullet("Broker reconciliation → FORCE_SYNC (필요 시)"))
    story.append(sp(2))
    story.append(h3("Phase 3: Fill Monitor"))
    story.append(bullet("Fill callback 설정 (Single Writer pattern)"))
    story.append(bullet("Fill monitor 백그라운드 스레드 시작"))
    story.append(sp(2))
    story.append(h3("Phase 4: Monitor Loop"))
    story.append(bullet("장중 1분 / 장외 5분 간격으로 루프"))
    story.append(bullet("Fill queue drain → handle_fill()"))
    story.append(bullet("가격 업데이트 → HWM + trail stop 계산"))
    story.append(bullet("Trail stop 발동 → SELL 주문 (pending 없을 때)"))
    story.append(bullet("5분마다 상태 저장, 10분마다 RECON 체크"))
    story.append(bullet("Telegram 알림 (체결, 근접 경고)"))
    story.append(sp(2))
    story.append(h3("Shutdown (Ctrl+C)"))
    story.append(bullet("Final fill drain + broker snapshot"))
    story.append(bullet("save_all(portfolio, runtime) + mark_shutdown"))
    story.append(bullet("Telegram: 'Live Stopped' 전송"))
    story.append(sp(6))

    story.append(h2("4.4 Server 모드 (--server)"))
    story.append(p("FastAPI 대시보드 서버를 시작합니다:"))
    story.append(code("python main.py --server    # http://localhost:8081"))
    story.append(PageBreak())

    # ═══ 5. STRATEGY LAYER ═══
    story.append(h1("5. 전략 레이어 (Strategy Layer)"))

    story.append(h2("5.1 Scoring (scoring.py)"))
    story.append(p("KR과 동일한 SHARED 스코어링 모듈입니다:"))
    story.append(bullet("<b>calc_volatility</b>: 252일 raw std (연율화 X)"))
    story.append(bullet("<b>calc_momentum</b>: close[-22] / close[-252] - 1"))
    story.append(sp(6))

    story.append(h2("5.2 Factor Ranker (factor_ranker.py)"))
    story.append(p("종목 선정 프로세스 (KR과 유사하나 VOL_PERCENTILE 차이):"))
    story.append(bullet("1) 전 종목 scoring"))
    story.append(bullet("2) 변동성 하위 <b>20%</b> 필터 (KR은 30%)"))
    story.append(bullet("3) 양의 모멘텀만 유지"))
    story.append(bullet("4) 모멘텀 내림차순 Top 20"))
    story.append(sp(6))

    story.append(h2("5.3 Rebalancer (rebalancer.py)"))
    story.append(p("KR과 동일 로직: SELL(보유 but not target) → BUY(target but not held)"))
    story.append(bullet("균등배분: equity / N_STOCKS"))
    story.append(bullet("Trail-hit 슬롯은 빈 상태 유지"))
    story.append(sp(6))

    story.append(h2("5.4 Trail Stop (trail_stop.py)"))
    story.append(p("종가 기준 -12% 트레일링 스톱. KR과 동일 로직."))
    story.append(note("portfolio_manager.py 내장 로직으로도 동작합니다 (check_trail_stops 메서드)."))
    story.append(sp(6))

    story.append(h2("5.5 Execution Gate (execution_gate.py)"))
    story.append(p("주문 전 <b>5단계 안전 게이트</b>를 통과해야 합니다:"))
    story.append(make_table(
        ["Gate", "검증 내용", "실패 시"],
        [
            ["1. Stale Data", "snapshot_age <= 24시간", "주문 차단"],
            ["2. RECON Safe", "Broker 연결 상태", "주문 차단"],
            ["3. Open Orders", "미체결 주문 없음", "주문 차단"],
            ["4. Market Hours", "장중 여부", "주문 차단"],
            ["5. Snapshot ID", "scoring == execution snapshot", "주문 차단 + CRITICAL 알림"],
        ],
        col_widths=[3*cm, 5.5*cm, 5.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("5.6 Snapshot Guard (snapshot_guard.py)"))
    story.append(p("scoring과 execution이 동일한 데이터를 사용하는지 강제합니다:"))
    story.append(bullet("snapshot_id 형식: US_YYYYMMDD"))
    story.append(bullet("불일치 시 CRITICAL 로그 + Telegram 알림 + 주문 차단"))
    story.append(PageBreak())

    # ═══ 6. CORE LAYER ═══
    story.append(h1("6. 코어 레이어 (Core Layer)"))

    story.append(h2("6.1 PortfolioManagerUS"))
    story.append(p("포지션 추적, HWM 갱신, trail stop 체크, RECON을 담당합니다."))
    story.append(sp(2))

    story.append(h3("USPosition 데이터 구조"))
    story.append(make_table(
        ["필드", "타입", "설명"],
        [
            ["symbol", "str", "티커 (e.g., AAPL)"],
            ["quantity", "int", "보유 수량"],
            ["avg_price", "float", "평균 매수가 (broker 기준)"],
            ["entry_date", "str", "매수일"],
            ["high_watermark", "float", "최고가 (HWM)"],
            ["current_price", "float", "현재가"],
            ["trail_stop_price", "float", "HWM x (1 - 0.12)"],
            ["drawdown_pct", "float", "(현재가/HWM - 1) x 100"],
            ["pending_sell_qty", "int", "매도 대기 수량 (중복 방지)"],
            ["last_sell_order_at", "str", "마지막 매도 주문 시각 (쿨다운)"],
            ["source", "str", "broker / state / reconciled"],
        ],
        col_widths=[3.5*cm, 2.5*cm, 9*cm]
    ))
    story.append(sp(4))

    story.append(h3("가격 업데이트 가드"))
    story.append(make_table(
        ["가드", "임계값", "동작"],
        [
            ["Stale Guard", "> 600초", "가격 업데이트 스킵"],
            ["Jump Guard (이전 대비)", "> 25% 변동", "가격 업데이트 스킵"],
            ["Jump Guard (HWM 대비)", "> 30% 변동", "가격 업데이트 스킵"],
        ],
        col_widths=[4*cm, 3*cm, 8*cm]
    ))
    story.append(sp(4))

    story.append(h3("RECON (reconcile_with_broker)"))
    story.append(make_table(
        ["Action", "조건", "동작"],
        [
            ["NONE", "완전 일치", "변경 없음"],
            ["LOG_ONLY", "미체결 주문 존재", "대기 (주문 완료까지)"],
            ["LOG_WARNING", "avg_price drift < 0.5%", "경고 로그"],
            ["SAFE_SYNC", "수량 불일치 or drift > 0.5%", "broker 기준 업데이트"],
            ["FORCE_SYNC", "Dirty exit 감지", "broker에서 전면 재구축"],
        ],
        col_widths=[3*cm, 4.5*cm, 7.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("6.2 StateManagerUS"))
    story.append(p("Atomic paired save: portfolio + runtime을 version_seq로 묶어 저장합니다."))
    story.append(bullet("portfolio_state_us_{mode}.json"))
    story.append(bullet("runtime_state_us_{mode}.json"))
    story.append(bullet("was_dirty_exit(): started_at 있고 shutdown_at 없으면 True"))
    story.append(bullet("mark_startup() / mark_shutdown(reason)"))
    story.append(PageBreak())

    # ═══ 7. DATA LAYER ═══
    story.append(h1("7. 데이터 레이어 (Data Layer)"))

    story.append(h2("7.1 AlpacaProvider (data/alpaca_provider.py)"))
    story.append(p("Alpaca REST API 클라이언트. Non-blocking 주문 패턴을 사용합니다."))
    story.append(sp(2))
    story.append(make_table(
        ["메서드", "설명"],
        [
            ["is_connected()", "GET /v2/clock으로 연결 테스트"],
            ["query_account_summary()", "equity, cash, buying_power 반환"],
            ["query_account_holdings()", "보유종목 리스트 반환"],
            ["query_open_orders()", "미체결 주문 리스트"],
            ["send_order(symbol, side, qty)", "시장가 주문 제출 (non-blocking)"],
            ["get_current_price(symbol)", "최신 bid/ask 중간값"],
            ["start_fill_monitor()", "체결 모니터 백그라운드 스레드 시작"],
            ["set_fill_callback(func)", "체결 콜백 등록"],
        ],
        col_widths=[5*cm, 10*cm]
    ))
    story.append(sp(2))
    story.append(h3("Non-Blocking 주문 패턴"))
    story.append(bullet("send_order() → 즉시 반환 (SUBMITTED)"))
    story.append(bullet("fill_monitor 스레드가 1~2초 간격으로 active orders 폴링"))
    story.append(bullet("FILLED/PARTIAL 감지 → event queue에 추가"))
    story.append(bullet("메인 루프가 process_events()로 queue drain → handle_fill()"))
    story.append(sp(6))

    story.append(h2("7.2 USDataCollector (data/alpaca_data.py)"))
    story.append(p("yfinance를 사용하여 OHLCV 데이터를 수집합니다."))
    story.append(bullet("collect_ohlcv(symbols, period='2y') — S&amp;P 500 OHLCV → DB upsert"))
    story.append(bullet("collect_index(['SPY','QQQ','IWM'], period='7y') — 인덱스 데이터"))
    story.append(sp(6))

    story.append(h2("7.3 Universe Builder (data/universe_builder.py)"))
    story.append(make_table(
        ["함수", "설명"],
        [
            ["get_sp500_tickers()", "Wikipedia에서 S&P 500 스크래핑"],
            ["get_russell1000_tickers()", "Russell 1000 (Wikipedia + fallback)"],
            ["get_russell3000_tickers()", "Russell 3000 근사 (R1000 + Alpaca assets)"],
            ["save_universe_snapshot()", "CSV 스냅샷 저장 (data/universes/)"],
            ["load_universe_snapshot()", "최신 스냅샷 로드"],
        ],
        col_widths=[5*cm, 10*cm]
    ))
    story.append(sp(6))

    story.append(h2("7.4 DbProviderUS (data/db_provider.py)"))
    story.append(p("PostgreSQL 데이터 프로바이더. 상세 스키마는 Section 14 참조."))
    story.append(make_table(
        ["메서드", "설명"],
        [
            ["get_ohlcv(symbol, start, end)", "단일 종목 OHLCV 조회"],
            ["load_close_dict(min_history)", "전 종목 종가 시리즈 로드 (scoring용)"],
            ["upsert_ohlcv(symbol, df)", "OHLCV upsert (ON CONFLICT)"],
            ["save_target_portfolio(target, snapshot_id)", "목표 포트폴리오 DB 저장"],
            ["get_target_portfolio(date)", "최신 목표 포트폴리오 조회"],
            ["health_check()", "테이블 통계 반환"],
        ],
        col_widths=[5.5*cm, 9.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 8. RISK MANAGEMENT ═══
    story.append(h1("8. 리스크 관리 (Risk Management)"))

    story.append(h2("8.1 DD 단계적 대응"))
    story.append(p("KR과 동일한 DD Graduated Response를 사용합니다:"))
    story.append(make_table(
        ["DD 수준", "buy_scale", "trim_ratio", "라벨"],
        [
            ["-5%", "70%", "0%", "DD_CAUTION"],
            ["-10%", "50%", "0%", "DD_WARNING"],
            ["-15%", "0% (차단)", "0%", "DD_CRITICAL"],
            ["-20%", "0% (차단)", "20%", "DD_SEVERE"],
            ["-25%", "0% (차단)", "20%", "DD_SAFE_MODE"],
        ],
        col_widths=[2.5*cm, 3*cm, 2.5*cm, 4*cm]
    ))
    story.append(sp(6))

    story.append(h2("8.2 포지션 레벨 가드"))
    story.append(bullet("<b>Stale Price Guard</b>: > 600초 가격 미갱신 → 업데이트 스킵"))
    story.append(bullet("<b>Jump Guard</b>: > 25% (이전 대비), > 30% (HWM 대비) → 스킵"))
    story.append(bullet("<b>Pending Sell Dedup</b>: pending_sell_qty > 0이면 추가 매도 방지"))
    story.append(bullet("<b>Trigger Cooldown</b>: last_sell_order_at 후 60초 이내 재발동 방지"))
    story.append(sp(6))

    story.append(h2("8.3 5-Gate Execution"))
    story.append(p("Section 5.5 참조. 모든 주문은 5개 게이트를 통과해야 실행됩니다."))
    story.append(PageBreak())

    # ═══ 9. ALPACA INTEGRATION ═══
    story.append(h1("9. Alpaca API 통합 (Broker Integration)"))

    story.append(h2("9.1 인증"))
    story.append(code("headers = {\n  'APCA-API-KEY-ID': ALPACA_API_KEY,\n  'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY,\n}"))
    story.append(sp(4))

    story.append(h2("9.2 사용 엔드포인트"))
    story.append(make_table(
        ["엔드포인트", "메서드", "용도"],
        [
            ["/v2/clock", "GET", "시장 상태 + 영업시간"],
            ["/v2/account", "GET", "계좌 요약"],
            ["/v2/positions", "GET", "보유종목 전체"],
            ["/v2/orders", "GET", "미체결 주문"],
            ["/v2/orders", "POST", "주문 제출"],
            ["/v2/orders/{id}", "GET", "주문 상태 확인"],
            ["/v2/stocks/{sym}/quotes/latest", "GET", "최신 호가 (Data API)"],
        ],
        col_widths=[5*cm, 2*cm, 8*cm]
    ))
    story.append(sp(4))

    story.append(h2("9.3 주문 타입"))
    story.append(p("현재: <b>Market Order만 지원</b> (Limit, Bracket, Stop-Loss 미구현)"))
    story.append(sp(4))

    story.append(h2("9.4 Fill Event 구조"))
    story.append(make_table(
        ["필드", "설명"],
        [
            ["symbol", "티커 (e.g., AAPL)"],
            ["side", "BUY / SELL"],
            ["new_fill_qty", "이번 체결 수량"],
            ["avg_price", "Alpaca avg_price"],
            ["order_no", "주문 ID"],
            ["status", "FILLED / PARTIAL"],
        ],
        col_widths=[3.5*cm, 11.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 10. TELEGRAM ═══
    story.append(h1("10. Telegram 알림 (Notifications)"))
    story.append(p("notify/telegram_bot.py에서 [US] 접두사로 알림을 전송합니다."))
    story.append(sp(4))
    story.append(make_table(
        ["함수", "용도"],
        [
            ["send(text, severity)", "범용 알림 (INFO/WARN/CRITICAL)"],
            ["notify_buy(symbol, qty, price)", "매수 체결 알림"],
            ["notify_sell(symbol, qty, price, pnl, pnl_pct)", "매도 체결 알림 (수익률 포함)"],
            ["notify_trail_near(symbol, margin_pct)", "트레일 근접 경고"],
            ["notify_trail_triggered(symbol, price, trigger)", "트레일 발동 알림"],
            ["notify_rebal_complete(n_sell, n_buy)", "리밸런싱 완료 알림"],
            ["notify_batch_complete(n_stocks, n_target)", "배치 스코어링 완료"],
            ["notify_snapshot_mismatch(...)", "스냅샷 불일치 CRITICAL"],
            ["notify_error(msg)", "에러 알림"],
        ],
        col_widths=[5.5*cm, 9.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 11. WEB DASHBOARD ═══
    story.append(h1("11. 웹 대시보드 (Web Dashboard)"))
    story.append(p("FastAPI 기반 대시보드: http://localhost:8081"))
    story.append(sp(4))

    story.append(h2("11.1 API 엔드포인트"))
    story.append(make_table(
        ["메서드", "경로", "설명"],
        [
            ["GET", "/", "메인 대시보드 (HTML)"],
            ["GET", "/api/health", "시장 상태, 연결 정보"],
            ["GET", "/api/db/health", "DB 테이블 통계"],
            ["GET", "/api/account", "Equity, Cash, Buying Power"],
            ["GET", "/api/portfolio", "보유종목 + P&L"],
            ["GET", "/api/orders/open", "미체결 주문"],
            ["GET", "/api/target", "최신 목표 포트폴리오"],
            ["GET", "/api/price/{symbol}", "현재가 조회"],
            ["GET", "/api/regime/current", "레짐 (실제 + 예측 + 섹터)"],
            ["POST", "/api/test/buy", "테스트 매수 (Paper)"],
            ["POST", "/api/test/sell", "테스트 매도 (Paper)"],
            ["GET", "/api/kr/portfolio", "KR Gen05 포트폴리오 프록시"],
        ],
        col_widths=[1.5*cm, 4*cm, 9.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("11.2 Cross-Market 연동"))
    story.append(p("/api/kr/portfolio를 통해 KR Gen05 포트폴리오를 프록시로 조회할 수 있습니다."))
    story.append(PageBreak())

    # ═══ 12. REGIME ═══
    story.append(h1("12. 레짐 예측 (Regime Prediction)"))
    story.append(p("regime/ 디렉토리에서 SPY, VIX, 섹터 ETF 기반으로 시장 레짐을 감지합니다."))
    story.append(sp(4))

    story.append(h2("12.1 데이터 소스"))
    story.append(make_table(
        ["지표", "용도"],
        [
            ["SPY", "시장 추세 (MA200, 수익률)"],
            ["VIX", "변동성 수준"],
            ["QQQ, IWM", "기술주/소형주 상대 강도"],
            ["섹터 ETF", "섹터 로테이션 (XLF, XLK 등)"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(sp(4))

    story.append(h2("12.2 레짐 분류"))
    story.append(make_table(
        ["레짐", "기준"],
        [
            ["BULL", "SPY > MA200 + 복합 스코어 높음"],
            ["SIDE", "중립"],
            ["BEAR", "SPY < MA200 + 복합 스코어 낮음"],
        ],
        col_widths=[3*cm, 12*cm]
    ))
    story.append(sp(4))
    story.append(h2("12.3 Actual vs Predicted"))
    story.append(bullet("<b>actual.py</b>: SPY 실제 변동률 기반 사후 분류"))
    story.append(bullet("<b>predictor.py</b>: 복합 스코어 → 내일 레짐 예측"))
    story.append(PageBreak())

    # ═══ 13. STRATEGY LAB ═══
    story.append(h1("13. Strategy Lab (백테스트)"))
    story.append(p("10개 전략을 5개 그룹으로 나누어 백테스트합니다."))
    story.append(sp(4))

    story.append(h2("13.1 전략 목록"))
    story.append(make_table(
        ["전략", "그룹", "설명"],
        [
            ["momentum_base", "rebal", "순수 12-1 모멘텀"],
            ["lowvol_momentum", "rebal", "Gen4 코어 (LowVol 20%ile + Mom)"],
            ["quality_factor", "rebal", "ROE + Value + Dividend"],
            ["hybrid_qscore", "rebal", "RS+Sector+Quality+Trend+LowVol"],
            ["breakout_trend", "rebal", "60일 고점 돌파, -8% trail"],
            ["mean_reversion", "event", "RSI<30, MA200 필터, 5일 max hold"],
            ["liquidity_signal", "event", "거래량 2x 급증 + 양봉"],
            ["sector_rotation", "macro", "Top 3 섹터 (60일 수익률)"],
            ["vol_regime", "regime", "VIX 적응형"],
            ["russell3000_lowvol", "experimental", "R3000 유니버스 Gen4 코어"],
        ],
        col_widths=[4*cm, 2.5*cm, 8.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("13.2 성과 지표 (12개)"))
    story.append(make_table(
        ["지표", "설명"],
        [
            ["CAGR", "연복합성장률"],
            ["MDD", "최대 낙폭"],
            ["Sharpe", "리스크 조정 수익 (sqrt(252))"],
            ["Calmar", "CAGR / |MDD|"],
            ["Turnover", "연간 회전율"],
            ["Avg Hold Days", "평균 보유 기간"],
            ["Trade Count", "총 거래 수"],
            ["Win Rate", "수익 거래 비율"],
            ["Exposure", "투자 비중"],
            ["Avg Positions", "평균 보유 종목 수"],
            ["Exit Reason Dist", "청산 사유 분포"],
            ["Missing Data Ratio", "데이터 품질"],
        ],
        col_widths=[4*cm, 11*cm]
    ))
    story.append(PageBreak())

    # ═══ 14. DATABASE ═══
    story.append(h1("14. 데이터베이스 (PostgreSQL)"))
    story.append(p("PostgreSQL 'qtron' 데이터베이스에 US 전용 테이블을 사용합니다."))
    story.append(sp(4))

    story.append(h2("14.1 운영 테이블"))
    story.append(make_table(
        ["테이블", "PK", "설명"],
        [
            ["ohlcv_us", "(symbol, date)", "일간 OHLCV (yfinance → upsert)"],
            ["sector_map_us", "(symbol)", "종목 정보 (name, sector, exchange, market_cap)"],
            ["index_us", "(symbol, date)", "인덱스 OHLCV (SPY, QQQ, IWM)"],
            ["target_portfolio_us", "(date, symbol)", "목표 포트폴리오 (rank, snapshot_id)"],
        ],
        col_widths=[4*cm, 3*cm, 8*cm]
    ))
    story.append(sp(6))

    story.append(h2("14.2 연구 테이블"))
    story.append(make_table(
        ["테이블", "PK", "설명"],
        [
            ["ohlcv_us_research", "(symbol, date)", "Lab 전용 OHLCV (universe_tag: R1000, R3000)"],
        ],
        col_widths=[4*cm, 3*cm, 8*cm]
    ))
    story.append(note("연구 테이블은 운영 테이블과 완전 분리됩니다."))
    story.append(PageBreak())

    # ═══ 15. OPERATIONS GUIDE ═══
    story.append(h1("15. 운영 가이드 (Operations Guide)"))

    story.append(h2("15.1 환경 설정"))
    story.append(code("python -m venv .venv\n.venv/Scripts/activate\npip install -r requirements.txt"))
    story.append(sp(2))
    story.append(h3(".env 파일 구성"))
    story.append(make_table(
        ["변수", "설명"],
        [
            ["ALPACA_API_KEY", "Alpaca API 키"],
            ["ALPACA_SECRET_KEY", "Alpaca Secret"],
            ["ALPACA_BASE_URL", "Paper: https://paper-api.alpaca.markets"],
            ["TELEGRAM_BOT_TOKEN", "Telegram 봇 토큰"],
            ["TELEGRAM_CHAT_ID", "Telegram 채팅 ID"],
            ["DB_NAME", "PostgreSQL DB명 (qtron)"],
            ["DB_USER / DB_PASSWORD", "DB 인증"],
            ["DB_HOST / DB_PORT", "DB 호스트 (localhost:5432)"],
        ],
        col_widths=[4.5*cm, 10.5*cm]
    ))
    story.append(sp(6))

    story.append(h2("15.2 일일 워크플로"))
    story.append(h3("장 시작 전"))
    story.append(code("python main.py --test     # 연결 확인"))
    story.append(code("python main.py --batch    # 데이터 업데이트 + 목표 산출"))
    story.append(sp(2))
    story.append(h3("장중"))
    story.append(code("python main.py --live     # 실시간 모니터링 + trail stop"))
    story.append(sp(2))
    story.append(h3("대시보드"))
    story.append(code("python main.py --server   # http://localhost:8081"))
    story.append(sp(6))

    story.append(h2("15.3 실행 명령어 요약"))
    story.append(make_table(
        ["명령어", "설명"],
        [
            ["python main.py --test", "연결 테스트 (Alpaca + DB)"],
            ["python main.py --batch", "일간 배치 (데이터 + 스코어링)"],
            ["python main.py --live", "실시간 매매 모니터링"],
            ["python main.py --server", "FastAPI 대시보드 (port 8081)"],
        ],
        col_widths=[5.5*cm, 9.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 16. KR vs US ═══
    story.append(h1("16. KR vs US 비교"))
    story.append(make_table(
        ["항목", "Q-TRON KR Gen05 1.0", "Q-TRON US 1.0"],
        [
            ["시장", "KOSPI + KOSDAQ", "S&P 500"],
            ["브로커", "키움증권 REST API", "Alpaca REST API"],
            ["Python", "3.9 32-bit", "3.12 64-bit"],
            ["데이터", "pykrx + SQLite", "yfinance + PostgreSQL"],
            ["VOL_PERCENTILE", "하위 30%", "하위 20%"],
            ["비용 (BUY)", "0.115%", "0.05%"],
            ["비용 (SELL)", "0.295%", "0.05%"],
            ["실시간", "WebSocket", "REST 폴링 (1~2초)"],
            ["대시보드", "FastAPI + SSE", "FastAPI + Jinja2"],
            ["유니버스 소스", "pykrx ticker list", "Wikipedia 스크래핑"],
            ["Lab 전략", "9개 (4그룹)", "10개 (5그룹)"],
            ["포트", "8080", "8081"],
            ["레짐 지표", "KOSPI/KOSDAQ/글로벌", "SPY/VIX/섹터 ETF"],
            ["알림", "Telegram + Kakao", "Telegram"],
        ],
        col_widths=[3*cm, 5.5*cm, 6.5*cm]
    ))
    story.append(PageBreak())

    # ═══ 17. EXPANSION ROADMAP ═══
    story.append(h1("17. 확장 로드맵 (Expansion Roadmap)"))

    story.append(h2("17.1 우선순위 높음"))
    story.append(bullet("리밸런싱 자동화 (--live 내 자동 리밸) — 현재 수동 트리거"))
    story.append(bullet("Telegram 명령어 추가 (/status, /portfolio, /rebalance)"))
    story.append(bullet("Live 모드 전환 (Paper → Live Alpaca)"))
    story.append(bullet("백테스트 정밀화 (개별종목 OHLCV, 슬리피지 모델)"))
    story.append(sp(4))

    story.append(h2("17.2 우선순위 보통"))
    story.append(bullet("전략 최적화 (Lab 결과 기반 VOL_PERCENTILE, MOM_SKIP 튜닝)"))
    story.append(bullet("Limit Order 지원 (Market → Limit 전환)"))
    story.append(bullet("Russell 1000/3000 유니버스 확장"))
    story.append(bullet("섹터 제약 (동일 섹터 N개 이하)"))
    story.append(sp(4))

    story.append(h2("17.3 인프라"))
    story.append(bullet("KR + US 통합 대시보드 (Cross-Market)"))
    story.append(bullet("자동 배치 스케줄러 (cron)"))
    story.append(bullet("클라우드 배포 (장시간 운영)"))
    story.append(bullet("PostgreSQL 백업 + 모니터링"))
    story.append(sp(4))

    story.append(h2("17.4 전략 고도화"))
    story.append(bullet("Regime-adaptive position sizing"))
    story.append(bullet("Multi-factor composite (Quality + Momentum + LowVol)"))
    story.append(bullet("옵션 헤지 (tail risk protection)"))
    story.append(bullet("시장 간 상관관계 모니터링 (KR-US correlation)"))

    # ── Build ──
    doc.build(story, onFirstPage=page_hf, onLaterPages=page_hf)
    print(f"PDF generated: {out_path}")
    return out_path


if __name__ == "__main__":
    build()
