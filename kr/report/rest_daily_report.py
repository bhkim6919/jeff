# -*- coding: utf-8 -*-
"""
rest_daily_report.py — REST 전용 EOD 일일 리포트
==================================================
SQLite + API 기반. 모든 섹션에 source_ts 명시.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("gen4.report.rest_daily")

REPORT_DIR = Path(__file__).resolve().parent / "output"


def generate_eod_report(
    portfolio: Optional[Dict] = None,
    dd_guard: Optional[Dict] = None,
    trail_stops: Optional[Dict] = None,
    recon: Optional[Dict] = None,
    regime_actual: Optional[Dict] = None,
    regime_predict: Optional[Dict] = None,
    rebalance: Optional[Dict] = None,
) -> Optional[Path]:
    """Generate REST EOD HTML report. Returns path to file."""
    eod_ts = time.time()
    eod_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    today_str = date.today().strftime("%Y-%m-%d")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = REPORT_DIR / f"rest_daily_{today_str.replace('-', '')}.html"

    # ── Build HTML ──
    sections = []

    # Header
    sections.append(f"""
    <div style="text-align:center;padding:20px 0;border-bottom:1px solid #333">
        <h1 style="color:#e6edf3;margin:0">Q-TRON REST Daily Report</h1>
        <p style="color:#8b949e;margin:4px 0">{today_str} | snapshot {eod_str}</p>
    </div>
    """)

    # Regime
    if regime_actual:
        scores = regime_actual.get("scores", {})
        sections.append(f"""
        <div class="section">
            <h2>레짐</h2>
            <table>
                <tr><td>오늘</td><td><b style="color:#F04452">{regime_actual.get('actual_label','--')}</b> (점수: {scores.get('total','--')})</td></tr>
                <tr><td>KOSPI</td><td>{regime_actual.get('kospi_change',0)*100:+.2f}% | breadth {regime_actual.get('breadth_ratio',0)*100:.0f}%</td></tr>
                <tr><td>점수 분해</td><td>ret={scores.get('ret_score',0)} br={scores.get('breadth_score',0)} flow={scores.get('flow_score',0)} stress={scores.get('stress_penalty',0)}</td></tr>
            </table>
        </div>
        """)
    if regime_predict:
        sections.append(f"""
        <div class="section">
            <h2>내일 예측</h2>
            <table>
                <tr><td>예측</td><td><b>{regime_predict.get('predicted_label','--')}</b> (점수: {regime_predict.get('composite_score',0):.3f})</td></tr>
                <tr><td>데이터</td><td>{regime_predict.get('available_weight',0)*100:.0f}% [{regime_predict.get('confidence_flag','')}]</td></tr>
            </table>
        </div>
        """)

    # Portfolio
    if portfolio:
        sections.append(f"""
        <div class="section">
            <h2>포트폴리오</h2>
            <table>
                <tr><td>총자산</td><td>{portfolio.get('total_asset',0):,.0f}원</td></tr>
                <tr><td>평가손익</td><td>{portfolio.get('pnl_pct',0):+.2f}%  ({portfolio.get('total_pnl',0):+,.0f}원)</td></tr>
                <tr><td>현금</td><td>{portfolio.get('cash',0):,.0f}원</td></tr>
                <tr><td>보유종목</td><td>{portfolio.get('holdings_count',0)}종목</td></tr>
            </table>
            <p class="source">source: kt00018</p>
        </div>
        """)

    # Trail Stop
    if trail_stops:
        danger_list = [s for s in trail_stops.get("stops", [])
                       if s.get("risk_zone") in ("DANGER", "WARNING") or s.get("triggered")]
        if danger_list:
            rows = ""
            for s in danger_list:
                margin = ((s["current_price"] / s["trail_price"]) - 1) * 100 if s.get("trail_price") else 0
                rows += f"<tr><td>{s['code']}</td><td>{s.get('risk_zone','')}</td><td>{margin:.1f}%</td></tr>"
            sections.append(f"""
            <div class="section">
                <h2>Trail Stop 위험 종목</h2>
                <table><tr><th>종목</th><th>구간</th><th>margin</th></tr>{rows}</table>
                <p class="source">source: portfolio_state_live.json</p>
            </div>
            """)

    # DD
    if dd_guard:
        sections.append(f"""
        <div class="section">
            <h2>DD 상태</h2>
            <table>
                <tr><td>일간 DD</td><td>{(dd_guard.get('daily_dd') or 0)*100:+.2f}%</td></tr>
                <tr><td>월간 DD</td><td>{(dd_guard.get('monthly_dd') or 0)*100:+.2f}%</td></tr>
                <tr><td>BUY STATUS</td><td>{dd_guard.get('buy_permission','--')}</td></tr>
            </table>
        </div>
        """)

    # Recon
    if recon:
        status = "비신뢰 ⚠️" if recon.get("unreliable") else "정상 ✅"
        sections.append(f"""
        <div class="section">
            <h2>시스템</h2>
            <table>
                <tr><td>RECON</td><td>{status}</td></tr>
                <tr><td>마지막 실행</td><td>{recon.get('last_run','--')}</td></tr>
                <tr><td>stale</td><td>{'⚠️ ' + str(recon.get('age_sec',0)) + '초' if recon.get('stale') else '정상'}</td></tr>
            </table>
        </div>
        """)

    # Rebalance
    if rebalance:
        sections.append(f"""
        <div class="section">
            <h2>리밸런싱</h2>
            <p>마지막: {rebalance.get('last','')} | D-? (cycle {rebalance.get('cycle',21)}일)</p>
        </div>
        """)

    # Assemble HTML
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Q-TRON REST Daily {today_str}</title>
<style>
    body {{ background:#0d1117; color:#e6edf3; font-family:system-ui,-apple-system,sans-serif; max-width:700px; margin:0 auto; padding:20px; }}
    .section {{ background:#161b22; border:1px solid #30363d; border-radius:10px; padding:16px; margin:12px 0; }}
    h2 {{ color:#58a6ff; font-size:15px; margin:0 0 10px; }}
    table {{ width:100%; border-collapse:collapse; }}
    td, th {{ padding:6px 8px; text-align:left; font-size:13px; border-bottom:1px solid #21262d; }}
    th {{ color:#8b949e; }}
    .source {{ color:#484f58; font-size:10px; margin-top:8px; }}
    b {{ color:#F04452; }}
</style></head><body>
{''.join(sections)}
<p style="text-align:center;color:#484f58;font-size:11px;margin-top:20px">
    Generated at {eod_str} | READ-ONLY analytics
</p>
</body></html>"""

    out_path.write_text(html, encoding="utf-8")
    logger.info(f"[REST Report] Generated: {out_path}")
    return out_path
