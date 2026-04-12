#!/usr/bin/env python3
"""Q-TRON DB Backup & Recovery Plan — PDF"""

import os
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.colors import HexColor, black, white
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
)
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

pdfmetrics.registerFont(TTFont("MG", "C:/Windows/Fonts/malgun.ttf"))
pdfmetrics.registerFont(TTFont("MGB", "C:/Windows/Fonts/malgunbd.ttf"))

C1 = HexColor("#1a237e")
C2 = HexColor("#283593")
CA = HexColor("#0d47a1")
CG = HexColor("#757575")
CT = HexColor("#1a237e")
CR = HexColor("#c62828")
CO = HexColor("#e65100")
CTA = HexColor("#f0f0f8")

def ms(name, fs=10, ld=14, tc=black, al=TA_LEFT, sb=0, sa=4, li=0, b=False):
    return ParagraphStyle(name, fontName="MGB" if b else "MG", fontSize=fs,
                          leading=ld, textColor=tc, alignment=al,
                          spaceBefore=sb, spaceAfter=sa, leftIndent=li, wordWrap='CJK')

S_CT = ms("CT", 28, 36, C1, TA_CENTER, b=True)
S_CS = ms("CS", 14, 20, C2, TA_CENTER)
S_H1 = ms("H1", 18, 24, C1, sb=20, sa=10, b=True)
S_H2 = ms("H2", 14, 19, C2, sb=14, sa=6, b=True)
S_H3 = ms("H3", 12, 16, CA, sb=10, sa=4, b=True)
S_B  = ms("B", 9.5, 14, sa=4, al=TA_JUSTIFY)
S_BL = ms("BL", 9.5, 14, li=15, sa=2)
S_N  = ms("N", 8.5, 12, CO, li=10, sb=4, sa=4)
S_CD = ms("CD", 8, 11, HexColor("#333"), li=10)

def h1(t): return Paragraph(t, S_H1)
def h2(t): return Paragraph(t, S_H2)
def h3(t): return Paragraph(t, S_H3)
def p(t):  return Paragraph(t, S_B)
def bl(t): return Paragraph(f"&bull; {t}", S_BL)
def note(t): return Paragraph(f"<b>NOTE:</b> {t}", S_N)
def warn(t): return Paragraph(f"<b>WARNING:</b> {t}", S_N)
def cd(t): return Paragraph(t.replace("\n","<br/>").replace(" ","&nbsp;"), S_CD)
def sp(h=6): return Spacer(1, h)

def mt(headers, rows, cw=None):
    data = [headers] + rows
    t = Table(data, colWidths=cw, repeatRows=1)
    cmds = [
        ('BACKGROUND',(0,0),(-1,0),CT), ('TEXTCOLOR',(0,0),(-1,0),white),
        ('FONTNAME',(0,0),(-1,0),'MGB'), ('FONTSIZE',(0,0),(-1,0),8.5),
        ('FONTNAME',(0,1),(-1,-1),'MG'), ('FONTSIZE',(0,1),(-1,-1),8),
        ('LEADING',(0,0),(-1,-1),11), ('ALIGN',(0,0),(-1,-1),'LEFT'),
        ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
        ('GRID',(0,0),(-1,-1),0.3,CG),
        ('TOPPADDING',(0,0),(-1,-1),3), ('BOTTOMPADDING',(0,0),(-1,-1),3),
        ('LEFTPADDING',(0,0),(-1,-1),5),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            cmds.append(('BACKGROUND',(0,i),(-1,i),CTA))
    t.setStyle(TableStyle(cmds))
    return t

def hf(canvas, doc):
    canvas.saveState()
    canvas.setFont("MGB", 8); canvas.setFillColor(CG)
    canvas.drawString(2*cm, A4[1]-1.2*cm, "Q-TRON  |  DB Backup & Recovery Plan v1.0")
    canvas.drawRightString(A4[0]-2*cm, A4[1]-1.2*cm, "2026-04-12")
    canvas.setFont("MG", 8)
    canvas.drawCentredString(A4[0]/2, 1.2*cm, f"- {doc.page} -")
    canvas.restoreState()

def build():
    out = os.path.join(os.path.dirname(__file__), "Q-TRON_Backup_Recovery_Plan_v1.0.pdf")
    doc = SimpleDocTemplate(out, pagesize=A4, topMargin=2*cm, bottomMargin=2*cm,
                            leftMargin=2*cm, rightMargin=2*cm)
    s = []

    # Cover
    s.append(Spacer(1,6*cm))
    s.append(Paragraph("Q-TRON", S_CT))
    s.append(sp(10))
    s.append(Paragraph("DB Backup & Recovery Plan", ms("x",20,26,CA,TA_CENTER)))
    s.append(sp(6))
    s.append(Paragraph("KR Gen05 + US 1.0  |  v1.0", S_CS))
    s.append(sp(30))
    s.append(Paragraph("2026-04-12", ms("d",11,14,CG,TA_CENTER)))
    s.append(PageBreak())

    # 1. Overview
    s.append(h1("1. Overview"))
    s.append(p("Q-TRON KR Gen05와 US 1.0은 단일 PostgreSQL 데이터베이스(qtron)를 공유하며, 테이블 접두사(_us)로 구분합니다. 이 문서는 DB 장애 시 데이터 복원 절차와 예방적 백업 전략을 정의합니다."))
    s.append(sp(4))
    s.append(mt(
        ["구분", "항목", "수량"],
        [
            ["PostgreSQL", "KR 테이블", "11개 (ohlcv, sector_map, report_* 6개 등)"],
            ["PostgreSQL", "US 테이블", "7개 (ohlcv_us, sector_map_us 등)"],
            ["JSON State", "KR 상태 파일", "11개 (portfolio/runtime x mode)"],
            ["JSON State", "US 상태 파일", "2개 (portfolio/runtime)"],
            ["SQLite", "보조 DB", "4개 (rest_state, regime, theme, dashboard)"],
            ["Report", "CSV/HTML 리포트", "86개 파일 (~9.5MB)"],
        ],
        cw=[3*cm, 5*cm, 7*cm]
    ))
    s.append(PageBreak())

    # 2. Data Asset Inventory
    s.append(h1("2. Data Asset Inventory"))

    s.append(h2("2.1 PostgreSQL - KR Tables"))
    s.append(mt(
        ["Table", "Criticality", "Description", "RPO"],
        [
            ["ohlcv", "CRITICAL", "KR OHLCV (2,500+ stocks, 260+ days)", "1 day"],
            ["sector_map", "CRITICAL", "Code -> name/sector/market", "1 month"],
            ["target_portfolio", "CRITICAL", "Rebalance targets (date, rank, scores)", "1 day"],
            ["fundamental", "IMPORTANT", "PER, PBR, EPS, market_cap", "1 day"],
            ["kospi_index", "IMPORTANT", "KOSPI daily OHLCV", "1 day"],
            ["report_trades", "IMPORTANT", "Trade execution log", "1 day"],
            ["report_close_log", "IMPORTANT", "Position exit log", "1 day"],
            ["report_equity_log", "IMPORTANT", "Daily equity snapshot", "1 day"],
            ["report_decision_log", "IMPORTANT", "Buy/sell decision audit", "1 day"],
            ["report_reconcile_log", "IMPORTANT", "Broker sync diff log", "1 day"],
            ["report_daily_positions", "IMPORTANT", "Daily holdings snapshot", "1 day"],
        ],
        cw=[3.5*cm, 2.5*cm, 6*cm, 2*cm]
    ))
    s.append(sp(6))

    s.append(h2("2.2 PostgreSQL - US Tables"))
    s.append(mt(
        ["Table", "Criticality", "Description", "RPO"],
        [
            ["ohlcv_us", "CRITICAL", "US OHLCV (S&P 500, 2yr)", "1 day"],
            ["sector_map_us", "CRITICAL", "Symbol -> name/sector/exchange", "1 month"],
            ["target_portfolio_us", "CRITICAL", "Rebalance targets + snapshot_id", "1 day"],
            ["index_us", "IMPORTANT", "SPY/QQQ/IWM daily OHLCV", "1 day"],
            ["trades_us", "IMPORTANT", "Trade log", "1 day"],
            ["equity_history_us", "IMPORTANT", "Daily equity snapshot", "1 day"],
            ["ohlcv_us_research", "RECOVERABLE", "Lab research (R1000/R3000)", "1 week"],
        ],
        cw=[3.5*cm, 2.5*cm, 6*cm, 2*cm]
    ))
    s.append(sp(6))

    s.append(h2("2.3 JSON State Files"))
    s.append(mt(
        ["File", "System", "Criticality", "Update"],
        [
            ["portfolio_state_live.json", "KR", "CRITICAL", "Real-time"],
            ["runtime_state_live.json", "KR", "CRITICAL", "Real-time"],
            ["portfolio_state_us_paper.json", "US", "CRITICAL", "Real-time"],
            ["runtime_state_us_paper.json", "US", "CRITICAL", "Real-time"],
            ["portfolio_state_paper.json", "KR", "IMPORTANT", "Paper session"],
            ["forensic_*.json", "KR", "IMPORTANT", "Incident"],
        ],
        cw=[5*cm, 1.5*cm, 2.5*cm, 3*cm]
    ))
    s.append(sp(6))

    s.append(h2("2.4 SQLite DBs"))
    s.append(mt(
        ["File", "Size", "Criticality", "Description"],
        [
            ["rest_state.db", "9.3 MB", "IMPORTANT", "REST API state mirror + validation log"],
            ["regime.db", "24 KB", "IMPORTANT", "Regime predictions + actuals"],
            ["theme_regime.db", "32 KB", "IMPORTANT", "Theme-based regime"],
            ["dashboard.db", "228 KB", "RECOVERABLE", "Dashboard timeseries (rebuildable)"],
        ],
        cw=[3.5*cm, 2*cm, 2.5*cm, 7*cm]
    ))
    s.append(PageBreak())

    # 3. Backup Strategy
    s.append(h1("3. Backup Strategy"))

    s.append(h2("3.1 Tier 1: Real-Time (5-minute)"))
    s.append(p("가장 중요한 운영 상태 파일을 5분 간격으로 백업합니다."))
    s.append(bl("<b>portfolio_state_live.json</b> → 백업 디렉토리 복사"))
    s.append(bl("<b>runtime_state_live.json</b> → 백업 디렉토리 복사"))
    s.append(bl("<b>portfolio_state_us_paper.json</b> → 백업 디렉토리 복사"))
    s.append(note("StateManager가 이미 .backup 파일을 생성하므로 추가 복사만 필요"))
    s.append(sp(6))

    s.append(h2("3.2 Tier 2: Daily EOD (17:00 KST)"))
    s.append(p("장 마감 후 전체 PostgreSQL 덤프 + 상태 파일 아카이브:"))
    s.append(sp(2))
    s.append(cd("# PostgreSQL full dump\npg_dump -U postgres -d qtron -F c -f backup/qtron_$(date +%Y%m%d).dump\n\n# State files archive\ntar czf backup/state_$(date +%Y%m%d).tar.gz \\\n  kr/state/*.json \\\n  us/state/*.json\n\n# SQLite copy\ncp kr/data/rest_state/rest_state.db backup/\ncp kr/data/regime/regime.db backup/\n\n# Report CSV archive\ntar czf backup/reports_$(date +%Y%m%d).tar.gz \\\n  kr/report/output/*.csv"))
    s.append(sp(6))

    s.append(h2("3.3 Tier 3: Monthly Archive (1st)"))
    s.append(p("월간 전체 아카이브를 외부 저장소에 보관합니다."))
    s.append(bl("PostgreSQL full dump (모든 테이블)"))
    s.append(bl("Report 전체 디렉토리 (HTML/JSON/CSV)"))
    s.append(bl("State 파일 히스토리"))
    s.append(bl("Lab research state"))
    s.append(sp(6))

    s.append(h2("3.4 Retention Policy"))
    s.append(mt(
        ["Tier", "Retention", "Storage"],
        [
            ["Tier 1 (Real-time)", "7 days rolling", "Local backup/"],
            ["Tier 2 (Daily)", "90 days", "Local + External drive"],
            ["Tier 3 (Monthly)", "3 years", "External drive"],
            ["CRITICAL tables", "5 years", "External drive (audit)"],
        ],
        cw=[4*cm, 4*cm, 7*cm]
    ))
    s.append(PageBreak())

    # 4. Recovery Procedures
    s.append(h1("4. Recovery Procedures"))

    s.append(h2("4.1 PostgreSQL Full Recovery"))
    s.append(p("DB 완전 손실 시 최신 daily dump에서 복원:"))
    s.append(cd("# 1. Stop all Q-TRON processes\n# 2. Recreate database\ndropdb -U postgres qtron\ncreatedb -U postgres qtron\n\n# 3. Restore from dump\npg_restore -U postgres -d qtron backup/qtron_YYYYMMDD.dump\n\n# 4. Verify\npsql -U postgres -d qtron -c \"SELECT table_name FROM information_schema.tables WHERE table_schema='public';\"\n\n# 5. Run batch to fill gap (if dump is stale)\npython main.py --batch"))
    s.append(sp(6))

    s.append(h2("4.2 Single Table Recovery"))
    s.append(p("특정 테이블만 손상된 경우:"))
    s.append(cd("# Extract single table from dump\npg_restore -U postgres -d qtron -t ohlcv backup/qtron_YYYYMMDD.dump\n\n# Or restore from CSV (report tables)\npsql -U postgres -d qtron -c \"\\copy report_trades FROM 'backup/trades.csv' CSV HEADER\""))
    s.append(sp(6))

    s.append(h2("4.3 State File Recovery"))
    s.append(p("portfolio_state 손상 시:"))
    s.append(bl("1) .backup 파일 확인 (StateManager 자동 생성)"))
    s.append(bl("2) .backup 없으면 Tier 1 백업에서 복원"))
    s.append(bl("3) 백업도 없으면 <b>--live</b> 시작 → RECON이 broker 기준으로 재구축"))
    s.append(warn("State 파일 삭제 금지. 반드시 백업 후 신규 생성."))
    s.append(sp(6))

    s.append(h2("4.4 SQLite Recovery"))
    s.append(p("SQLite DB 손상 시:"))
    s.append(bl("<b>rest_state.db</b>: 삭제 후 서버 재시작 → 자동 재생성"))
    s.append(bl("<b>regime.db</b>: Tier 2 백업에서 복원 또는 재생성"))
    s.append(bl("<b>dashboard.db</b>: 삭제 후 재시작 (equity_log에서 재구축)"))
    s.append(PageBreak())

    # 5. OHLCV Recovery
    s.append(h1("5. OHLCV Data Recovery"))
    s.append(p("OHLCV는 가장 큰 테이블이지만 외부 소스에서 재수집 가능합니다."))
    s.append(sp(4))

    s.append(h2("5.1 KR OHLCV (pykrx)"))
    s.append(cd("cd kr\npython main.py --batch\n# pykrx에서 전 종목 OHLCV 재수집 (30분~1시간)"))
    s.append(sp(4))

    s.append(h2("5.2 US OHLCV (yfinance)"))
    s.append(cd("cd us\npython main.py --batch\n# yfinance에서 S&P 500 OHLCV 재수집 (10~20분)"))
    s.append(sp(4))

    s.append(h2("5.3 Sector Map"))
    s.append(bl("<b>KR</b>: pykrx + 키움 API에서 재수집 (batch 포함)"))
    s.append(bl("<b>US</b>: yfinance info에서 재수집 (이미 구현, 502종목)"))
    s.append(PageBreak())

    # 6. Disaster Scenarios
    s.append(h1("6. Disaster Scenarios"))

    s.append(h2("6.1 PostgreSQL Service Crash"))
    s.append(mt(
        ["단계", "조치", "예상 시간"],
        [
            ["1", "PostgreSQL 서비스 재시작: net start postgresql-x64-17", "1분"],
            ["2", "Q-TRON 프로세스 재시작", "2분"],
            ["3", "RECON 자동 실행 → 상태 복원", "1분"],
            ["4", "정상 운영 확인", "1분"],
        ],
        cw=[1.5*cm, 9.5*cm, 3*cm]
    ))
    s.append(sp(6))

    s.append(h2("6.2 Database Corruption"))
    s.append(mt(
        ["단계", "조치", "예상 시간"],
        [
            ["1", "PostgreSQL 서비스 중지", "1분"],
            ["2", "최신 daily dump에서 pg_restore", "5~10분"],
            ["3", "--batch 실행 (dump 이후 데이터 보충)", "30분"],
            ["4", "State 파일 확인 (broker RECON으로 보정)", "5분"],
            ["5", "--live 시작 → RECON → 정상 운영", "5분"],
        ],
        cw=[1.5*cm, 9.5*cm, 3*cm]
    ))
    s.append(sp(6))

    s.append(h2("6.3 Disk Failure (전체 손실)"))
    s.append(mt(
        ["단계", "조치", "예상 시간"],
        [
            ["1", "External drive에서 최신 monthly archive 복원", "30분"],
            ["2", "PostgreSQL 설치 + dump 복원", "15분"],
            ["3", "Python venv 재구성", "10분"],
            ["4", "--batch (KR + US) 실행", "1시간"],
            ["5", "Broker RECON으로 포트폴리오 재구축", "5분"],
            ["6", "Report 히스토리는 archive에서 복원", "10분"],
        ],
        cw=[1.5*cm, 9.5*cm, 3*cm]
    ))
    s.append(sp(6))

    s.append(h2("6.4 Portfolio State 손실 (DB 정상)"))
    s.append(p("가장 흔한 시나리오. Broker가 truth이므로 복원 가능합니다."))
    s.append(bl("1) --live 시작"))
    s.append(bl("2) was_dirty_exit() = True 감지"))
    s.append(bl("3) RECON → FORCE_SYNC → broker holdings에서 재구축"))
    s.append(bl("4) 포트폴리오 복원 완료 (5분 이내)"))
    s.append(note("Broker = Truth 원칙에 의해 포트폴리오는 항상 broker에서 복원 가능"))
    s.append(PageBreak())

    # 7. Automation
    s.append(h1("7. Backup Automation (구현 예정)"))

    s.append(h2("7.1 Daily Backup Script"))
    s.append(cd("#!/bin/bash\n# qtron_daily_backup.sh\nBACKUP_DIR=C:/Q-TRON-32_ARCHIVE/backup\nDATE=$(date +%Y%m%d)\n\n# PostgreSQL dump\npg_dump -U postgres -d qtron -F c -f $BACKUP_DIR/qtron_$DATE.dump\n\n# State files\ncp kr/state/*.json $BACKUP_DIR/state_kr/\ncp us/state/*.json $BACKUP_DIR/state_us/\n\n# SQLite\ncp kr/data/rest_state/rest_state.db $BACKUP_DIR/sqlite/\ncp kr/data/regime/*.db $BACKUP_DIR/sqlite/\n\n# Cleanup old backups (90 days)\nfind $BACKUP_DIR -name '*.dump' -mtime +90 -delete\n\necho \"[BACKUP] $DATE completed\""))
    s.append(sp(6))

    s.append(h2("7.2 Tray Integration"))
    s.append(p("tray_server.py의 auto-batch 스케줄러와 동일 구조로 daily backup을 추가할 수 있습니다."))
    s.append(bl("17:00 KST: EOD 완료 후 자동 백업"))
    s.append(bl("결과 로그: [BACKUP_OK] / [BACKUP_FAIL]"))
    s.append(bl("실패 시 Telegram 알림"))
    s.append(PageBreak())

    # 8. Security
    s.append(h1("8. Security Notes"))
    s.append(warn("현재 PostgreSQL 자격증명이 소스 코드에 하드코딩되어 있습니다."))
    s.append(sp(4))
    s.append(h2("8.1 현재 상태"))
    s.append(bl("DB: qtron / User: postgres / Password: 하드코딩"))
    s.append(bl("Host: localhost (외부 접근 불가)"))
    s.append(sp(4))
    s.append(h2("8.2 개선 방향"))
    s.append(bl("자격증명을 .env 파일로 이관 (US는 이미 일부 적용)"))
    s.append(bl("백업 파일 암호화 (7z 또는 gpg)"))
    s.append(bl("외부 저장소 전송 시 암호화된 채널 사용"))

    # Build
    doc.build(s, onFirstPage=hf, onLaterPages=hf)
    print(f"PDF: {out}")
    return out

if __name__ == "__main__":
    build()
