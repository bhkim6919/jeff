"""
bt_reporter.py
==============
백테스트 결과를 HTML 리포트로 저장.

포함 내용:
  - 핵심 지표 요약 (CAGR, MDD, Sharpe, 승률 등)
  - 월별 수익률 히트맵 (HTML 테이블)
  - 자산 곡선 (SVG 인라인 차트)
  - 거래 내역 테이블 (최근 50건)
  - Train / Test 구간 분리 표시

사용:
  from backtest.bt_reporter import BtReporter
  reporter = BtReporter(output_dir="backtest/results")
  path = reporter.save(result, label="base")
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from backtest.bt_engine import BtResult


class BtReporter:

    def __init__(self, output_dir: str = "backtest/results"):
        self._out = Path(output_dir)
        self._out.mkdir(parents=True, exist_ok=True)

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def save(self, result: BtResult, label: str = "") -> str:
        """HTML 리포트 저장 후 파일 경로 반환."""
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"bt_report_{label}_{ts}.html" if label else f"bt_report_{ts}.html"
        path     = self._out / filename

        html = self._build_html(result, label)
        path.write_text(html, encoding="utf-8")
        print(f"[BtReporter] 리포트 저장 → {path}")
        return str(path)

    def print_summary(self, result: BtResult, label: str = ""):
        """콘솔에 요약 출력."""
        m = result.metrics
        tag = f"[{label}] " if label else ""
        print("=" * 60)
        print(f"{tag}백테스트 결과 요약")
        print(f"  기간:      {result.config.start} ~ {result.config.end}")
        print(f"  초기자본:  {m.get('initial_cash', 0):>14,.0f}원")
        print(f"  최종자산:  {m.get('final_equity', 0):>14,.0f}원")
        print(f"  총수익률:  {m.get('total_return', 0)*100:>+.2f}%")
        print(f"  CAGR:      {m.get('cagr', 0)*100:>+.2f}%")
        print(f"  MDD:       {m.get('mdd', 0)*100:>.2f}%")
        print(f"  Sharpe:    {m.get('sharpe', 0):>.3f}")
        print(f"  거래횟수:  {m.get('n_trades', 0):>5}회")
        print(f"  승률:      {m.get('win_rate', 0)*100:>.1f}%")
        print(f"  평균손익:  {m.get('avg_pnl', 0):>+,.0f}원/거래")
        print("=" * 60)

    # ── HTML 빌더 ─────────────────────────────────────────────────────────────

    def _build_html(self, result: BtResult, label: str) -> str:
        m   = result.metrics
        cfg = result.config

        metrics_html  = self._metrics_html(m, cfg)
        chart_html    = self._equity_chart_html(result.equity_curve, cfg)
        monthly_html  = self._monthly_heatmap_html(result.monthly_returns)
        trades_html   = self._trades_html(result.trades)

        title = f"Q-TRON 백테스트 리포트{' — ' + label if label else ''}"

        return f"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<title>{title}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:'Malgun Gothic',sans-serif;background:#f0f2f5;color:#111;padding:20px;}}
h1{{color:#1e3a5f;margin-bottom:4px;}}
h2{{color:#1e3a5f;font-size:1.1rem;margin:20px 0 10px;border-left:4px solid #3b82f6;padding-left:10px;}}
.subtitle{{color:#6b7280;font-size:.9rem;margin-bottom:20px;}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:20px;}}
.card{{background:#fff;border-radius:8px;padding:16px;box-shadow:0 1px 4px rgba(0,0,0,.08);}}
.card .label{{font-size:.78rem;color:#6b7280;margin-bottom:4px;}}
.card .value{{font-size:1.4rem;font-weight:700;}}
.card .value.pos{{color:#16a34a;}}
.card .value.neg{{color:#dc2626;}}
.card .value.neu{{color:#1e3a5f;}}
section{{background:#fff;border-radius:8px;padding:20px;margin-bottom:16px;
         box-shadow:0 1px 4px rgba(0,0,0,.08);}}
table{{width:100%;border-collapse:collapse;font-size:.85rem;}}
th{{background:#f8fafc;border-bottom:2px solid #e2e8f0;padding:8px 10px;text-align:center;}}
td{{border-bottom:1px solid #f1f5f9;padding:6px 10px;text-align:right;}}
td:first-child{{text-align:center;}}
tr:hover td{{background:#f8fafc;}}
.hm-pos{{background:#dcfce7;color:#166534;}}
.hm-neg{{background:#fee2e2;color:#991b1b;}}
.hm-zero{{background:#f1f5f9;color:#64748b;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:9px;font-size:.78rem;font-weight:600;}}
.badge-tp{{background:#dcfce7;color:#166534;}}
.badge-sl{{background:#fee2e2;color:#991b1b;}}
.badge-ma{{background:#fef9c3;color:#854d0e;}}
.badge-eod{{background:#e0e7ff;color:#3730a3;}}
</style>
</head>
<body>
<h1>{title}</h1>
<p class="subtitle">
  기간: {cfg.start} ~ {cfg.end} &nbsp;|&nbsp;
  슬리피지: {cfg.slippage*100:.2f}% &nbsp;|&nbsp;
  수수료: {cfg.commission*100:.4f}% (편도) &nbsp;|&nbsp;
  Train: ~{cfg.train_end} / Test: {cfg.test_start}~
</p>

{metrics_html}

<section>
  <h2>📈 자산 곡선</h2>
  {chart_html}
</section>

<section>
  <h2>📅 월별 수익률</h2>
  {monthly_html}
</section>

<section>
  <h2>📋 거래 내역 (최근 50건)</h2>
  {trades_html}
</section>

</body>
</html>"""

    # ── 지표 카드 ─────────────────────────────────────────────────────────────

    def _metrics_html(self, m: dict, cfg) -> str:
        def card(label, value, cls="neu"):
            return f'<div class="card"><div class="label">{label}</div><div class="value {cls}">{value}</div></div>'

        total_ret = m.get("total_return", 0)
        cagr      = m.get("cagr", 0)
        mdd       = m.get("mdd", 0)
        sharpe    = m.get("sharpe", 0)

        cards = [
            card("총 수익률",   f"{total_ret*100:+.2f}%",
                 "pos" if total_ret >= 0 else "neg"),
            card("CAGR",        f"{cagr*100:+.2f}%",
                 "pos" if cagr >= 0 else "neg"),
            card("최대 낙폭(MDD)", f"{mdd*100:.2f}%",
                 "neg" if mdd < -0.1 else "neu"),
            card("Sharpe",      f"{sharpe:.3f}",
                 "pos" if sharpe >= 1 else ("neu" if sharpe >= 0 else "neg")),
            card("승률",        f"{m.get('win_rate',0)*100:.1f}%",   "neu"),
            card("총 거래",     f"{m.get('n_trades',0)}회",          "neu"),
            card("평균 손익",   f"{m.get('avg_pnl',0):+,.0f}원",
                 "pos" if m.get("avg_pnl",0) >= 0 else "neg"),
            card("최종 자산",   f"{m.get('final_equity',0)/1e6:.2f}M원", "neu"),
        ]
        return '<div class="grid">' + "".join(cards) + "</div>"

    # ── 자산 곡선 SVG ─────────────────────────────────────────────────────────

    def _equity_chart_html(self, eq_df: pd.DataFrame, cfg) -> str:
        if eq_df.empty or "equity" not in eq_df.columns:
            return "<p>데이터 없음</p>"

        W, H   = 900, 260
        PAD    = 50
        series = eq_df["equity"].tolist()
        n      = len(series)
        if n < 2:
            return "<p>데이터 부족</p>"

        min_v, max_v = min(series), max(series)
        span = max(max_v - min_v, 1)

        def px(i, v):
            x = PAD + (i / (n - 1)) * (W - PAD * 2)
            y = H - PAD - ((v - min_v) / span) * (H - PAD * 2)
            return x, y

        pts = " ".join(f"{px(i,v)[0]:.1f},{px(i,v)[1]:.1f}" for i, v in enumerate(series))

        # Train/Test 구분선
        train_end_dt = cfg.train_end  # YYYYMMDD
        vline_html   = ""
        if "date" in eq_df.columns:
            dates = eq_df["date"].astype(str).str.replace("-", "").tolist()
            try:
                idx = next(i for i, d in enumerate(dates) if d >= train_end_dt)
                vx  = px(idx, series[idx])[0]
                vline_html = (
                    f'<line x1="{vx}" y1="{PAD}" x2="{vx}" y2="{H-PAD}" '
                    f'stroke="#f59e0b" stroke-width="2" stroke-dasharray="6,3"/>'
                    f'<text x="{vx+4}" y="{PAD+14}" font-size="11" fill="#b45309">Test 시작</text>'
                )
            except StopIteration:
                pass

        init_y = px(0, series[0])[1]
        svg = f"""<svg viewBox="0 0 {W} {H}" style="width:100%;max-width:{W}px;">
  <line x1="{PAD}" y1="{init_y:.1f}" x2="{W-PAD}" y2="{init_y:.1f}"
        stroke="#9ca3af" stroke-width="1" stroke-dasharray="4,3"/>
  <polyline points="{pts}" fill="none" stroke="#3b82f6" stroke-width="2"/>
  {vline_html}
  <text x="{PAD}" y="{H-8}" font-size="11" fill="#6b7280">{eq_df['date'].iloc[0] if 'date' in eq_df.columns else ''}</text>
  <text x="{W-PAD}" y="{H-8}" font-size="11" fill="#6b7280" text-anchor="end">{eq_df['date'].iloc[-1] if 'date' in eq_df.columns else ''}</text>
  <text x="{PAD-4}" y="{PAD}" font-size="11" fill="#6b7280" text-anchor="end">{max_v/1e6:.1f}M</text>
  <text x="{PAD-4}" y="{H-PAD}" font-size="11" fill="#6b7280" text-anchor="end">{min_v/1e6:.1f}M</text>
</svg>"""
        return svg

    # ── 월별 히트맵 ────────────────────────────────────────────────────────────

    def _monthly_heatmap_html(self, monthly: pd.DataFrame) -> str:
        if monthly.empty:
            return "<p>데이터 없음</p>"

        years  = sorted(monthly["year"].unique())
        months = list(range(1, 13))
        month_labels = ["1월","2월","3월","4월","5월","6월",
                        "7월","8월","9월","10월","11월","12월"]

        rows = ["<table><tr><th>연도</th>" +
                "".join(f"<th>{m}</th>" for m in month_labels) +
                "<th>연간</th></tr>"]

        for year in years:
            yr_data = monthly[monthly["year"] == year]
            cells   = [f"<td>{year}</td>"]
            annual  = 1.0
            for month in months:
                row = yr_data[yr_data["month"] == month]
                if row.empty:
                    cells.append('<td class="hm-zero">—</td>')
                else:
                    r = float(row["return"].iloc[0])
                    annual *= (1 + r)
                    cls  = "hm-pos" if r > 0 else ("hm-neg" if r < 0 else "hm-zero")
                    cells.append(f'<td class="{cls}">{r*100:+.1f}%</td>')
            annual_ret = annual - 1
            cls_a = "hm-pos" if annual_ret > 0 else "hm-neg"
            cells.append(f'<td class="{cls_a}"><b>{annual_ret*100:+.1f}%</b></td>')
            rows.append("<tr>" + "".join(cells) + "</tr>")

        rows.append("</table>")
        return "\n".join(rows)

    # ── 거래 내역 ─────────────────────────────────────────────────────────────

    def _trades_html(self, trades: pd.DataFrame) -> str:
        if trades.empty:
            return "<p>거래 없음</p>"

        sells = trades[trades["side"] == "SELL"].tail(50)
        if sells.empty:
            return "<p>청산 거래 없음</p>"

        badge_map = {"TP": "badge-tp", "SL": "badge-sl",
                     "MA20": "badge-ma", "EOD": "badge-eod"}

        rows = ["""<table>
<tr><th>날짜</th><th>종목</th><th>청산유형</th><th>수량</th>
<th>체결가</th><th>손익</th><th>수익률</th></tr>"""]

        for _, row in sells.iterrows():
            pnl  = float(row.get("pnl", 0))
            qty  = int(row.get("quantity", 0))
            price= float(row.get("price", 0))
            cost_base = qty * float(row.get("raw_price", price))
            ret  = pnl / cost_base * 100 if cost_base > 0 else 0
            ctype= str(row.get("close_type", ""))
            badge_cls = badge_map.get(ctype, "")
            pnl_cls   = "pos" if pnl >= 0 else "neg"

            rows.append(f"""<tr>
<td>{row.get('date','')}</td>
<td>{row.get('code','')}</td>
<td><span class="badge {badge_cls}">{ctype}</span></td>
<td>{qty:,}</td>
<td>{price:,.0f}</td>
<td style="color:{'#16a34a' if pnl>=0 else '#dc2626'}">{pnl:+,.0f}원</td>
<td style="color:{'#16a34a' if ret>=0 else '#dc2626'}">{ret:+.2f}%</td>
</tr>""")

        rows.append("</table>")
        return "\n".join(rows)
