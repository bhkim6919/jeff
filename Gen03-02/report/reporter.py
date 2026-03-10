"""
Reporter + TradeLogger
======================
Gen3 리포트 생성 및 거래 로그 기록.

로그 파일 위치: data/logs/
  - trades.csv        : 체결 내역
  - close_log.csv     : 청산 내역 (TP/SL/MA20/MAX_HOLD)
  - equity_log.csv    : 일별 자산 추이
  - daily_log.csv     : 일별 레짐/체결 요약
  - report_*.html     : HTML 리포트
"""

import csv
import os
import webbrowser
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from config import Gen3Config
from data.name_lookup import get_name


# ── TradeLogger ───────────────────────────────────────────────────────────────

class TradeLogger:
    """체결 내역 + 청산 내역 CSV 기록."""

    def __init__(self, config: Gen3Config):
        self.log_dir = config.abs_path("data/logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.trades_csv    = self.log_dir / "trades.csv"
        self.close_csv     = self.log_dir / "close_log.csv"

    def log_all(self, results: list, positioned: List[dict] = None) -> None:
        pos_map = {p["code"]: p for p in (positioned or [])}
        exists  = self.trades_csv.exists()
        with open(self.trades_csv, "a", newline="", encoding="utf-8-sig") as f:
            fields = ["date", "code", "side", "quantity", "exec_price",
                      "slippage_pct", "rejected", "reject_reason", "qscore"]
            w = csv.DictWriter(f, fieldnames=fields)
            if not exists:
                w.writeheader()
            for r in results:
                plan = pos_map.get(getattr(r, "code", ""), {})
                w.writerow({
                    "date":          datetime.now().strftime("%Y-%m-%d"),
                    "code":          getattr(r, "code", ""),
                    "side":          getattr(r, "side", ""),
                    "quantity":      getattr(r, "quantity", 0),
                    "exec_price":    getattr(r, "exec_price", 0),
                    "slippage_pct":  f"{getattr(r, 'slippage_pct', 0):.4f}",
                    "rejected":      getattr(r, "rejected", False),
                    "reject_reason": getattr(r, "reject_reason", ""),
                    "qscore":        plan.get("qscore", ""),
                })

    def log_close(self, code: str, close_type: str,
                  entry_price: float, close_price: float, pnl: float) -> None:
        exists = self.close_csv.exists()
        with open(self.close_csv, "a", newline="", encoding="utf-8-sig") as f:
            fields = ["date", "code", "close_type", "entry_price", "close_price", "pnl"]
            w = csv.DictWriter(f, fieldnames=fields)
            if not exists:
                w.writeheader()
            w.writerow({
                "date":        datetime.now().strftime("%Y-%m-%d"),
                "code":        code,
                "close_type":  close_type,
                "entry_price": int(entry_price),
                "close_price": int(close_price),
                "pnl":         int(pnl),
            })


# ── Reporter ──────────────────────────────────────────────────────────────────

PERIOD_LABEL = {
    "weekly": "주간", "monthly": "월간", "quarterly": "분기",
    "semiannual": "반기", "annual": "연간",
}


class Reporter:
    """Gen3 리포트 생성기."""

    def __init__(self, config: Gen3Config):
        self.log_dir    = config.abs_path("data/logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.equity_csv = self.log_dir / "equity_log.csv"
        self.daily_csv  = self.log_dir / "daily_log.csv"

    # ── 공개 API ─────────────────────────────────────────────────────────────

    def report_daily(
        self,
        results:    list,
        positioned: List[dict],
        regime:     Any,
        signals:    List[dict],
        portfolio:  Dict[str, Any],
        open_browser: bool = False,
    ) -> None:
        now = datetime.now()
        self._log_equity(portfolio, now)
        self._log_daily(regime, signals, results, now)

        stats = self._calc_daily_stats(results)
        html  = self._build_daily_html(results, regime, signals, portfolio, stats, now)
        path  = self._save_html(html, f"daily_{now.strftime('%Y%m%d_%H%M%S')}")

        self._print_daily(regime, signals, results, portfolio, stats, now)
        if open_browser:
            self._open(path)

    def auto_period_reports(self, open_browser: bool = False) -> None:
        today     = date.today()
        generated = []
        if today.weekday() == 4:
            self.report_period("weekly", open_browser)
            generated.append("주간")
        if self._is_month_end(today):
            self.report_period("monthly", open_browser)
            generated.append("월간")
        if generated:
            print(f"[Reporter] 자동 생성: {', '.join(generated)} 리포트")

    def report_period(self, period: str, open_browser: bool = False) -> None:
        now        = datetime.now()
        start, end = self._period_range(period, now)
        label      = PERIOD_LABEL.get(period, period)
        equities   = self._load_equities(start, end)
        stats      = self._calc_period_stats(equities)
        html       = self._build_period_html(label, start, end, stats, now)
        path       = self._save_html(html, f"{period}_{now.strftime('%Y%m%d_%H%M%S')}")
        self._print_period(label, start, end, stats)
        if open_browser:
            self._open(path)

    # ── 로그 기록 ─────────────────────────────────────────────────────────────

    def _log_equity(self, portfolio: dict, now: datetime) -> None:
        exists = self.equity_csv.exists()
        with open(self.equity_csv, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["timestamp", "total", "cash", "exposure"])
            if not exists:
                w.writeheader()
            total    = str(portfolio.get("총평가금액", "0원")).replace(",", "").replace("원", "")
            cash     = str(portfolio.get("현금", "0원")).replace(",", "").replace("원", "")
            exposure = str(portfolio.get("총노출도", "0%")).replace("%", "")
            w.writerow({"timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                        "total": total, "cash": cash, "exposure": exposure})

    def _log_daily(self, regime: Any, signals: list, results: list, now: datetime) -> None:
        exists   = self.daily_csv.exists()
        accepted = sum(1 for r in results if not getattr(r, "rejected", False))
        with open(self.daily_csv, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["date", "regime", "signals", "accepted"])
            if not exists:
                w.writeheader()
            w.writerow({
                "date":     now.strftime("%Y-%m-%d"),
                "regime":   getattr(regime, "value", str(regime)),
                "signals":  len(signals),
                "accepted": accepted,
            })

    # ── 통계 ─────────────────────────────────────────────────────────────────

    def _calc_daily_stats(self, results: list) -> dict:
        total    = len(results)
        accepted = sum(1 for r in results if not getattr(r, "rejected", False))
        return {"체결건수": accepted, "거부건수": total - accepted, "전체시도": total}

    def _calc_period_stats(self, equities: list) -> dict:
        exposure_vals = []
        for row in equities:
            try:
                exposure_vals.append(float(row.get("exposure", 0)))
            except Exception:
                pass
        avg_exp = sum(exposure_vals) / len(exposure_vals) if exposure_vals else 0.0
        return {"관측일수": len(equities), "평균노출도(%)": round(avg_exp, 2)}

    # ── HTML 빌더 ─────────────────────────────────────────────────────────────

    @staticmethod
    def _rs_badge(v: float) -> str:
        """RS 퍼센타일을 색상 스팬으로 변환."""
        if v >= 0.90:   color = "#22c55e"   # 초록 (상위 10%)
        elif v >= 0.70: color = "#84cc16"   # 연초록
        elif v >= 0.50: color = "#f59e0b"   # 주황
        else:           color = "#ef4444"   # 빨강
        return f'<span style="color:{color};font-weight:600">{v:.0%}</span>'

    def _sig_row(self, s: dict) -> str:
        """signals 항목 1개를 HTML <tr>로 변환 — 선정 근거 포함."""
        code   = s.get("code", "")
        entry  = s.get("entry", 0)
        tp     = s.get("tp", 0)
        sl     = s.get("sl", 0)
        stage  = s.get("stage", "B")
        qscore = s.get("qscore", 0.0)
        rs     = s.get("rs_composite", qscore)

        # Stage 배지
        sc = "#f59e0b" if stage == "A" else "#3b82f6"
        stage_html = (
            f'<span style="background:{sc};color:#fff;padding:1px 6px;'
            f'border-radius:3px;font-size:9px;font-weight:700">{stage}</span>'
        )

        # 모멘텀 (rs20/rs60/rs120)
        m20  = self._rs_badge(s.get("rs20_rank",  0.0))
        m60  = self._rs_badge(s.get("rs60_rank",  0.0))
        m120 = self._rs_badge(s.get("rs120_rank", 0.0))
        momentum = f'<span style="font-size:10px">{m20}&nbsp;/&nbsp;{m60}&nbsp;/&nbsp;{m120}</span>'

        # 선정근거 배지
        tags = []
        if s.get("is_52w_high"):
            tags.append('<span style="background:#7c3aed;color:#fff;'
                        'padding:1px 4px;border-radius:2px;font-size:9px">52주신고</span>')
        if s.get("above_ma20"):
            tags.append('<span style="background:#059669;color:#fff;'
                        'padding:1px 4px;border-radius:2px;font-size:9px">MA20위</span>')
        if s.get("signal_entry"):
            tags.append('<span style="background:#0891b2;color:#fff;'
                        'padding:1px 4px;border-radius:2px;font-size:9px">진입신호</span>')
        tags_html = "&nbsp;".join(tags) if tags else '<span style="color:#6b7280">-</span>'

        # RR 비율
        risk   = entry - sl
        reward = tp - entry
        rr     = reward / risk if risk > 0 else 0.0
        rr_color = "#22c55e" if rr >= 2.0 else ("#f59e0b" if rr >= 1.5 else "#ef4444")
        rr_html = f'<span style="color:{rr_color};font-weight:600">{rr:.1f}x</span>'

        return (
            f'<tr>'
            f'<td>{get_name(code)}</td>'
            f'<td style="color:#9ca3af;font-size:10px">{code}</td>'
            f'<td>{stage_html}</td>'
            f'<td style="font-weight:600">{qscore:.4f}</td>'
            f'<td>{self._rs_badge(rs)}</td>'
            f'<td style="white-space:nowrap">{momentum}</td>'
            f'<td style="white-space:nowrap">{tags_html}</td>'
            f'<td>{rr_html}</td>'
            f'<td>{entry:,}</td>'
            f'<td>{tp:,}</td>'
            f'<td>{sl:,}</td>'
            f'<td>{s.get("sector","")}</td>'
            f'</tr>'
        )

    def _build_daily_html(
        self, results: list, regime: Any, signals: list,
        portfolio: dict, stats: dict, now: datetime,
    ) -> str:
        regime_str = getattr(regime, "value", str(regime))
        color_map  = {"BULL": "#2ecc71", "BEAR": "#e74c3c", "SIDEWAYS": "#f39c12"}
        color      = color_map.get(regime_str, "#888")

        trade_rows = ""
        for r in results:
            rejected = getattr(r, "rejected", False)
            bg       = "#1c0a0a" if rejected else "#0a1c0a"
            code     = getattr(r, "code", "")
            trade_rows += (
                f'<tr style="background:{bg}">'
                f'<td>{get_name(code)}</td>'
                f'<td style="color:#9ca3af;font-size:10px">{code}</td>'
                f'<td>{"거부" if rejected else "체결"}</td>'
                f'<td>{getattr(r, "quantity", 0)}</td>'
                f'<td>{getattr(r, "exec_price", 0):,.0f}</td>'
                f'<td>{getattr(r, "slippage_pct", 0):.3%}</td>'
                f'<td>{getattr(r, "reject_reason", "")}</td>'
                "</tr>"
            )

        sig_rows = ""
        for s in signals[:20]:
            sig_rows += self._sig_row(s)

        port_rows = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in portfolio.items()
        )
        stat_rows = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in stats.items()
        )

        return self._wrap_html(
            title       = f"Q-TRON Gen3 일일 리포트 {now.strftime('%Y-%m-%d')}",
            period_badge= "일일",
            period_color= "#3498db",
            subtitle    = f"{now.strftime('%Y-%m-%d %H:%M')} | 레짐: {regime_str} | 신호: {len(signals)}개",
            market_color= color,
            body        = f"""
            <h2>📈 당일 요약</h2>
            <table><tr><th>항목</th><th>값</th></tr>{stat_rows}</table>

            <h2>📋 오늘의 신호 (signals.csv 상위 20개)</h2>
            <table>
              <tr>
                <th>종목명</th><th>코드</th><th>Stage</th>
                <th>Q-Score</th><th>RS%</th><th>모멘텀(20/60/120)</th>
                <th>선정근거</th><th>RR</th>
                <th>진입가</th><th>TP</th><th>SL</th><th>섹터</th>
              </tr>
              {sig_rows}
            </table>

            <h2>✅ 체결 내역</h2>
            <table>
              <tr><th>종목명</th><th>코드</th><th>구분</th><th>수량</th><th>체결가</th><th>슬리피지</th><th>사유</th></tr>
              {trade_rows}
            </table>

            <h2>💼 포트폴리오</h2>
            <table><tr><th>항목</th><th>값</th></tr>{port_rows}</table>
            """,
        )

    def _build_period_html(
        self, label: str, start: date, end: date, stats: dict, now: datetime
    ) -> str:
        period_str = f"{start} ~ {end}"
        stat_rows  = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in stats.items()
        )
        return self._wrap_html(
            title       = f"Q-TRON Gen3 {label} 리포트 ({period_str})",
            period_badge= label,
            period_color= "#6366f1",
            subtitle    = f"생성: {now.strftime('%Y-%m-%d %H:%M')}",
            market_color= "#6366f1",
            body        = f"""
            <h2>📊 {label} 요약 ({period_str})</h2>
            <table><tr><th>항목</th><th>값</th></tr>{stat_rows}</table>
            """,
        )

    def _wrap_html(self, title, period_badge, period_color, subtitle, market_color, body) -> str:
        return f"""<!doctype html>
<html lang="ko">
<head><meta charset="utf-8"/><title>{title}</title>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<style>
body{{font-family:'Malgun Gothic',sans-serif;background:#0f172a;color:#e5e7eb;margin:0;}}
.wrap{{max-width:960px;margin:0 auto;padding:16px;}}
.header{{padding:10px 4px 14px;border-bottom:1px solid #1f2937;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;
        border:1px solid {period_color};color:{period_color};margin-right:8px;}}
.title{{font-size:18px;font-weight:700;}}
.subtitle{{font-size:12px;color:#9ca3af;margin-top:4px;color:{market_color};}}
h2{{font-size:14px;margin:16px 0 8px;}}
table{{width:100%;border-collapse:collapse;font-size:11px;}}
th,td{{padding:4px 6px;border-bottom:1px solid #111827;}}
th{{text-align:left;color:#9ca3af;font-weight:500;}}
tr:hover{{background:#020617;}}
</style></head>
<body><div class="wrap">
<div class="header">
  <div class="title"><span class="badge">{period_badge}</span>{title}</div>
  <div class="subtitle">{subtitle}</div>
</div>
{body}
</div></body></html>"""

    # ── 유틸 ─────────────────────────────────────────────────────────────────

    def _save_html(self, html: str, name: str) -> str:
        path = self.log_dir / f"report_{name}.html"
        path.write_text(html, encoding="utf-8")
        print(f"[Reporter] HTML → {path}")
        return str(path)

    def _open(self, path: str) -> None:
        try:
            webbrowser.open(f"file://{os.path.abspath(path)}")
        except Exception as e:
            print(f"[Reporter] 브라우저 오픈 실패: {e}")

    def _print_daily(self, regime, signals, results, portfolio, stats, now) -> None:
        print(f"\n{'='*60}")
        print(f"  Q-TRON Gen3 일일 리포트  {now.strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")
        print(f"[레짐] {getattr(regime,'value',regime)}  |  신호: {len(signals)}개")
        print("\n[당일 요약]")
        for k, v in stats.items():
            print(f"  {k}: {v}")
        print("\n[포트폴리오]")
        for k, v in portfolio.items():
            print(f"  {k}: {v}")
        print(f"{'='*60}\n")

    def _print_period(self, label, start, end, stats) -> None:
        print(f"\n{'='*60}")
        print(f"  Q-TRON Gen3 {label} 리포트  ({start} ~ {end})")
        print(f"{'='*60}")
        for k, v in stats.items():
            print(f"  {k}: {v}")
        print(f"{'='*60}\n")

    def _load_equities(self, start: date, end: date) -> list:
        if not self.equity_csv.exists():
            return []
        rows = []
        with open(self.equity_csv, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S").date()
                    if start <= ts <= end:
                        rows.append(row)
                except Exception:
                    pass
        return rows

    def _period_range(self, period: str, now: datetime) -> Tuple[date, date]:
        end = now.date()
        if period == "weekly":
            start = end - timedelta(days=6)
        elif period == "monthly":
            start = end.replace(day=1)
        elif period == "quarterly":
            month = ((end.month - 1) // 3) * 3 + 1
            start = date(end.year, month, 1)
        elif period == "semiannual":
            start = date(end.year, 1 if end.month <= 6 else 7, 1)
        elif period == "annual":
            start = date(end.year, 1, 1)
        else:
            start = end
        return start, end

    def _is_month_end(self, d: date) -> bool:
        return (d + timedelta(days=1)).month != d.month
