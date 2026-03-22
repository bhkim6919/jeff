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
        self.mode = "MOCK" if getattr(config, "paper_trading", True) else "LIVE"

    def log_all(self, results: list, positioned: List[dict] = None) -> None:
        pos_map = {p["code"]: p for p in (positioned or [])}
        exists  = self.trades_csv.exists()
        with open(self.trades_csv, "a", newline="", encoding="utf-8-sig") as f:
            fields = ["date", "code", "side", "quantity", "raw_fill_qty",
                      "decision_price", "fill_price", "exec_price",
                      "slippage_pct", "rejected", "reject_reason", "qscore", "mode"]
            w = csv.DictWriter(f, fieldnames=fields)
            if not exists:
                w.writeheader()
            for r in results:
                plan = pos_map.get(getattr(r, "code", ""), {})
                # v7.9: applied_qty 우선, 없으면 quantity 폴백
                _applied = getattr(r, "applied_qty", 0) or getattr(r, "quantity", 0)
                _raw     = getattr(r, "raw_fill_qty", 0) or getattr(r, "quantity", 0)
                w.writerow({
                    "date":          datetime.now().strftime("%Y-%m-%d"),
                    "code":          getattr(r, "code", ""),
                    "side":          getattr(r, "side", ""),
                    "quantity":      _applied,
                    "raw_fill_qty":  _raw,
                    "decision_price": getattr(r, "decision_price", 0),
                    "fill_price":    getattr(r, "fill_price", 0),
                    "exec_price":    getattr(r, "exec_price", 0),
                    "slippage_pct":  f"{getattr(r, 'slippage_pct', 0):.4f}",
                    "rejected":      getattr(r, "rejected", False),
                    "reject_reason": getattr(r, "reject_reason", ""),
                    "qscore":        plan.get("qscore", ""),
                    "mode":          self.mode,
                })

    def log_close(self, code: str, close_type: str,
                  entry_price: float, close_price: float,
                  quantity: int = 0,
                  pnl_gross: float = 0.0,
                  fee_buy: float = 0.0, fee_sell: float = 0.0,
                  tax: float = 0.0, total_cost: float = 0.0,
                  pnl_net: float = 0.0,
                  # v7.7: price/qty source 분리
                  decision_price: float = 0.0,
                  fill_price: float = 0.0,
                  qty_before: int = 0,
                  qty_after: int = 0,
                  # backward compat
                  pnl: float = 0.0) -> None:
        # backward compat: pnl만 넘어온 경우 (이전 호출 방식)
        if pnl != 0.0 and pnl_gross == 0.0:
            pnl_gross = pnl
            pnl_net   = pnl
        exists = self.close_csv.exists()
        with open(self.close_csv, "a", newline="", encoding="utf-8-sig") as f:
            fields = ["date", "code", "close_type", "quantity",
                      "entry_price", "close_price",
                      "decision_price", "fill_price",
                      "qty_before", "qty_after",
                      "pnl_gross", "fee_buy", "fee_sell", "tax",
                      "total_cost", "pnl_net", "mode"]
            w = csv.DictWriter(f, fieldnames=fields)
            if not exists:
                w.writeheader()
            w.writerow({
                "date":        datetime.now().strftime("%Y-%m-%d"),
                "code":        code,
                "close_type":  close_type,
                "quantity":    quantity,
                "entry_price": int(entry_price),
                "close_price": int(close_price),
                "decision_price": int(decision_price),
                "fill_price":  int(fill_price),
                "qty_before":  qty_before,
                "qty_after":   qty_after,
                "pnl_gross":   int(pnl_gross),
                "fee_buy":     int(fee_buy),
                "fee_sell":    int(fee_sell),
                "tax":         int(tax),
                "total_cost":  int(total_cost),
                "pnl_net":     int(pnl_net),
                "mode":        self.mode,
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
        self.close_csv  = self.log_dir / "close_log.csv"
        self.daily_csv  = self.log_dir / "daily_log.csv"
        self.mode = "MOCK" if getattr(config, "paper_trading", True) else "LIVE"

    # ── 공개 API ─────────────────────────────────────────────────────────────

    def report_daily(
        self,
        results:    list,
        positioned: List[dict],
        regime:     Any,
        signals:    List[dict],
        portfolio:  Dict[str, Any],
        open_browser: bool = False,
        restricted_positions: Optional[List[dict]] = None,
    ) -> None:
        now = datetime.now()
        self._log_equity(portfolio, now)
        self._log_daily(regime, signals, results, now)

        rp = restricted_positions or []
        stats = self._calc_daily_stats(results)
        html  = self._build_daily_html(results, regime, signals, portfolio, stats, now, rp)
        path  = self._save_html(html, f"daily_{now.strftime('%Y%m%d_%H%M%S')}")

        self._print_daily(regime, signals, results, portfolio, stats, now, rp)
        if open_browser:
            self._open(path)

    def auto_period_reports(self, open_browser: bool = False) -> None:
        today     = date.today()
        generated = []
        # 주간: 매주 금요일
        if today.weekday() == 4:
            self.report_period("weekly", open_browser)
            generated.append("주간")
        # 월간: 매월 말일
        if self._is_month_end(today):
            self.report_period("monthly", open_browser)
            generated.append("월간")
        # 분기: 3/6/9/12월 말일
        if self._is_month_end(today) and today.month in (3, 6, 9, 12):
            self.report_period("quarterly", open_browser)
            generated.append("분기")
        # 반기: 6/12월 말일
        if self._is_month_end(today) and today.month in (6, 12):
            self.report_period("semiannual", open_browser)
            generated.append("반기")
        # 연간: 12월 말일
        if self._is_month_end(today) and today.month == 12:
            self.report_period("annual", open_browser)
            generated.append("연간")
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
            w = csv.DictWriter(f, fieldnames=["timestamp", "total", "cash", "exposure", "mode"])
            if not exists:
                w.writeheader()
            total    = str(portfolio.get("총평가금액", "0원")).replace(",", "").replace("원", "")
            cash     = str(portfolio.get("현금", "0원")).replace(",", "").replace("원", "")
            exposure = str(portfolio.get("총노출도", "0%")).replace("%", "")
            w.writerow({"timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                        "total": total, "cash": cash, "exposure": exposure,
                        "mode": self.mode})

    def _log_daily(self, regime: Any, signals: list, results: list, now: datetime) -> None:
        """날짜 기준 upsert: 같은 날짜 재실행해도 1일 1행 보장."""
        fields = ["date", "regime", "signals", "accepted", "mode"]
        today_str = now.strftime("%Y-%m-%d")
        accepted = sum(1 for r in results if not getattr(r, "rejected", False))
        new_row = {
            "date":     today_str,
            "regime":   getattr(regime, "value", str(regime)),
            "signals":  len(signals),
            "accepted": accepted,
            "mode":     self.mode,
        }

        # 기존 CSV 읽기 → 오늘 날짜 행 제거 → 새 행 추가 → 전체 재작성
        existing_rows = []
        if self.daily_csv.exists():
            try:
                with open(self.daily_csv, encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        if row.get("date") != today_str:
                            existing_rows.append(row)
            except Exception:
                pass

        existing_rows.append(new_row)
        with open(self.daily_csv, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(existing_rows)

    # ── 통계 ─────────────────────────────────────────────────────────────────

    def _calc_daily_stats(self, results: list) -> dict:
        total    = len(results)
        accepted = sum(1 for r in results if not getattr(r, "rejected", False))

        # 비용 합산 (청산 결과에 포함된 비용 메타)
        total_fee_buy  = sum(getattr(r, "fee_buy",    0) for r in results)
        total_fee_sell = sum(getattr(r, "fee_sell",   0) for r in results)
        total_tax      = sum(getattr(r, "tax",        0) for r in results)
        total_cost     = sum(getattr(r, "total_cost", 0) for r in results)
        total_pnl_gross = sum(getattr(r, "pnl_gross", 0) for r in results
                              if hasattr(r, "pnl_gross"))
        total_pnl_net   = sum(getattr(r, "pnl",       0) for r in results
                              if hasattr(r, "close_type"))

        return {
            "체결건수": accepted,
            "거부건수": total - accepted,
            "전체시도": total,
            "매수수수료": f"{total_fee_buy:,.0f}원",
            "매도수수료": f"{total_fee_sell:,.0f}원",
            "매도세금":   f"{total_tax:,.0f}원",
            "총부대비용": f"{total_cost:,.0f}원",
            "총손익(세전)": f"{total_pnl_gross:+,.0f}원",
            "총손익(세후)": f"{total_pnl_net:+,.0f}원",
        }

    def _calc_period_stats(self, equities: list) -> dict:
        if not equities:
            return {"관측일수": 0, "데이터 없음": "-"}

        totals = []
        exposure_vals = []
        cash_vals = []
        for row in equities:
            try:
                totals.append(float(row.get("total", 0)))
            except Exception:
                pass
            try:
                exposure_vals.append(float(row.get("exposure", 0)))
            except Exception:
                pass
            try:
                cash_vals.append(float(row.get("cash", 0)))
            except Exception:
                pass

        avg_exp = sum(exposure_vals) / len(exposure_vals) if exposure_vals else 0.0
        start_equity = totals[0] if totals else 0
        end_equity   = totals[-1] if totals else 0
        peak_equity  = max(totals) if totals else 0
        trough_equity = min(totals) if totals else 0

        # 기간 수익률
        period_return = ((end_equity / start_equity) - 1) * 100 if start_equity else 0.0
        # MDD
        running_peak = 0.0
        max_dd = 0.0
        for t in totals:
            if t > running_peak:
                running_peak = t
            dd = ((t - running_peak) / running_peak) * 100 if running_peak else 0.0
            if dd < max_dd:
                max_dd = dd

        stats = {
            "관측일수":        len(equities),
            "시작 자산":       f"{start_equity:,.0f}원",
            "종료 자산":       f"{end_equity:,.0f}원",
            "기간 수익률":     f"{period_return:+.2f}%",
            "최고 자산":       f"{peak_equity:,.0f}원",
            "최저 자산":       f"{trough_equity:,.0f}원",
            "MDD":            f"{max_dd:.2f}%",
            "평균 노출도":     f"{avg_exp:.1f}%",
        }
        if cash_vals:
            avg_cash = sum(cash_vals) / len(cash_vals)
            stats["평균 현금"] = f"{avg_cash:,.0f}원"
        return stats

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
        restricted_positions: List[dict] = None,
    ) -> str:
        regime_str = getattr(regime, "value", str(regime))
        color_map  = {"BULL": "#2ecc71", "BEAR": "#e74c3c", "SIDEWAYS": "#f39c12"}
        color      = color_map.get(regime_str, "#888")

        trade_rows = ""
        for r in results:
            rejected = getattr(r, "rejected", False)
            bg       = "#1c0a0a" if rejected else "#0a1c0a"
            code     = getattr(r, "code", "")
            cost     = getattr(r, "total_cost", 0)
            pnl_net  = getattr(r, "pnl", 0) if hasattr(r, "close_type") else 0
            pnl_color = "#22c55e" if pnl_net > 0 else ("#ef4444" if pnl_net < 0 else "#9ca3af")
            # v7.7: decision/fill price, qty_before/after
            dp = getattr(r, "decision_price", 0)
            fp = getattr(r, "fill_price", 0)
            qb = getattr(r, "qty_before", 0)
            qa = getattr(r, "qty_after", 0)
            trade_rows += (
                f'<tr style="background:{bg}">'
                f'<td>{get_name(code)}</td>'
                f'<td style="color:#9ca3af;font-size:10px">{code}</td>'
                f'<td>{"거부" if rejected else "체결"}</td>'
                f'<td>{getattr(r, "quantity", 0)}</td>'
                f'<td>{dp:,.0f}</td>'
                f'<td>{fp:,.0f}</td>'
                f'<td>{getattr(r, "slippage_pct", 0):.3%}</td>'
                f'<td>{qb}→{qa}</td>'
                f'<td style="color:#f59e0b">{cost:,.0f}</td>'
                f'<td style="color:{pnl_color};font-weight:600">'
                f'{pnl_net:+,.0f}</td>'
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

        # v7.6: 청산 실패 / 제약 종목 섹션
        rp = restricted_positions or []
        restricted_html = ""
        if rp:
            rp_rows = ""
            for rp_item in rp:
                conf = rp_item.get("qty_confidence", "?")
                conf_color = "#ef4444" if conf != "HIGH" else "#22c55e"
                rp_rows += (
                    f'<tr style="background:#1c0a0a">'
                    f'<td>{get_name(rp_item.get("code",""))}</td>'
                    f'<td style="color:#9ca3af;font-size:10px">{rp_item.get("code","")}</td>'
                    f'<td>{rp_item.get("hold_qty", 0)}</td>'
                    f'<td style="color:#f59e0b;font-weight:600">{rp_item.get("sellable_qty", "?")}</td>'
                    f'<td style="color:{conf_color}">{conf}</td>'
                    f'<td>{rp_item.get("reason", "")}</td>'
                    f'</tr>'
                )
            restricted_html = f"""
            <h2>&#9888; 청산 실패 / 제약 종목</h2>
            <table>
              <tr><th>종목명</th><th>코드</th><th>보유수량</th><th>매도가능</th><th>신뢰도</th><th>사유</th></tr>
              {rp_rows}
            </table>
            """

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
              <tr><th>종목명</th><th>코드</th><th>구분</th><th>수량</th><th>판단가</th><th>체결가</th><th>슬리피지</th><th>수량변화</th><th>부대비용</th><th>세후손익</th><th>사유</th></tr>
              {trade_rows}
            </table>

            {restricted_html}

            <h2>💼 포트폴리오</h2>
            <table><tr><th>항목</th><th>값</th></tr>{port_rows}</table>

            <h2>🔍 운영 품질</h2>
            <table>
              <tr><th>항목</th><th>상태</th></tr>
              <tr><td>qty mismatch</td><td>{self._ops_badge(rp, 'qty')}</td></tr>
              <tr><td>fill price anomaly</td><td>{self._ops_badge_slip(results)}</td></tr>
              <tr><td>partial fill/close</td><td>{self._ops_badge_partial(results)}</td></tr>
              <tr><td>restricted positions</td><td>{self._ops_badge_restricted(rp)}</td></tr>
              <tr><td>data source</td><td><b>{self.mode}</b></td></tr>
            </table>
            """,
        )

    # ── v7.7 운영 품질 배지 헬퍼 ───────────────────────────────────────────

    @staticmethod
    def _ops_badge(rp: list, kind: str) -> str:
        if not rp:
            return '<span style="color:#22c55e">정상</span>'
        mismatch = [r for r in rp if r.get("reason") == "POSITION_MISMATCH"]
        if mismatch:
            return f'<span style="color:#ef4444;font-weight:600">심각 ({len(mismatch)}건)</span>'
        return f'<span style="color:#f59e0b">주의 ({len(rp)}건)</span>'

    @staticmethod
    def _ops_badge_slip(results: list) -> str:
        anomalies = [r for r in results
                     if not getattr(r, "rejected", False)
                     and abs(getattr(r, "slippage_pct", 0)) > 0.05]
        if not anomalies:
            return '<span style="color:#22c55e">정상</span>'
        return (f'<span style="color:#ef4444;font-weight:600">'
                f'이상 ({len(anomalies)}건, >5%)</span>')

    @staticmethod
    def _ops_badge_partial(results: list) -> str:
        partials = [r for r in results
                    if getattr(r, "close_status", "") == "PARTIAL_CLOSED"]
        if not partials:
            return '<span style="color:#22c55e">정상</span>'
        return (f'<span style="color:#f59e0b;font-weight:600">'
                f'{len(partials)}건 PARTIAL</span>')

    @staticmethod
    def _ops_badge_restricted(rp: list) -> str:
        if not rp:
            return '<span style="color:#22c55e">없음</span>'
        return (f'<span style="color:#ef4444;font-weight:600">'
                f'{len(rp)}건</span>')

    def _build_period_html(
        self, label: str, start: date, end: date, stats: dict, now: datetime
    ) -> str:
        period_str = f"{start} ~ {end}"
        stat_rows  = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in stats.items()
        )
        # v7.7: 청산 사유 분포 (close_log.csv)
        exit_html = self._build_exit_distribution(start, end)
        return self._wrap_html(
            title       = f"Q-TRON Gen3 {label} 리포트 ({period_str})",
            period_badge= label,
            period_color= "#6366f1",
            subtitle    = f"생성: {now.strftime('%Y-%m-%d %H:%M')}",
            market_color= "#6366f1",
            body        = f"""
            <h2>📊 {label} 요약 ({period_str})</h2>
            <table><tr><th>항목</th><th>값</th></tr>{stat_rows}</table>
            {exit_html}
            """,
        )

    def _build_exit_distribution(self, start: date, end: date) -> str:
        """close_log.csv에서 기간 내 청산 사유 분포 및 손익 기여도 산출."""
        if not self.close_csv.exists():
            return ""
        try:
            rows = []
            with open(self.close_csv, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    d = row.get("date", "")
                    if start.isoformat() <= d <= end.isoformat():
                        rows.append(row)
            if not rows:
                return ""
            # Aggregate by close_type
            from collections import defaultdict
            agg = defaultdict(lambda: {"count": 0, "pnl": 0, "wins": 0})
            total_trades = len(rows)
            total_pnl = 0
            total_wins = 0
            for row in rows:
                ct = row.get("close_type", "UNKNOWN")
                pnl = int(row.get("pnl_net", 0))
                agg[ct]["count"] += 1
                agg[ct]["pnl"] += pnl
                total_pnl += pnl
                if pnl > 0:
                    agg[ct]["wins"] += 1
                    total_wins += 1

            exit_rows = ""
            for ct, v in sorted(agg.items(), key=lambda x: -x[1]["count"]):
                wr = v["wins"] / v["count"] * 100 if v["count"] else 0
                pnl_c = "#22c55e" if v["pnl"] > 0 else "#ef4444"
                exit_rows += (
                    f'<tr><td>{ct}</td><td>{v["count"]}</td>'
                    f'<td>{wr:.0f}%</td>'
                    f'<td style="color:{pnl_c};font-weight:600">{v["pnl"]:+,}원</td></tr>'
                )
            total_wr = total_wins / total_trades * 100 if total_trades else 0
            total_c = "#22c55e" if total_pnl > 0 else "#ef4444"
            exit_rows += (
                f'<tr style="border-top:2px solid #334155"><td><b>합계</b></td>'
                f'<td><b>{total_trades}</b></td>'
                f'<td><b>{total_wr:.0f}%</b></td>'
                f'<td style="color:{total_c};font-weight:700"><b>{total_pnl:+,}원</b></td></tr>'
            )
            return f"""
            <h2>📉 청산 사유 분포</h2>
            <table>
              <tr><th>사유</th><th>건수</th><th>승률</th><th>손익 기여</th></tr>
              {exit_rows}
            </table>
            """
        except Exception:
            return ""

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

    def _print_daily(self, regime, signals, results, portfolio, stats, now,
                      restricted_positions: list = None) -> None:
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
        # 비용 요약이 있으면 별도 출력
        cost_keys = ["매수수수료", "매도수수료", "매도세금", "총부대비용", "총손익(세전)", "총손익(세후)"]
        cost_items = {k: v for k, v in stats.items() if k in cost_keys}
        if cost_items and any(v != "0원" and v != "+0원" for v in cost_items.values()):
            print("\n[부대비용]")
            for k, v in cost_items.items():
                print(f"  {k}: {v}")
        # v7.6: 청산 실패 / 제약 종목
        rp = restricted_positions or []
        if rp:
            print(f"\n[청산 실패/제약 종목] {len(rp)}건")
            for rp_item in rp:
                code = rp_item.get("code", "")
                print(f"  {get_name(code)}({code})  "
                      f"hold={rp_item.get('hold_qty', '?')}  "
                      f"sellable={rp_item.get('sellable_qty', '?')}  "
                      f"confidence={rp_item.get('qty_confidence', '?')}  "
                      f"reason={rp_item.get('reason', '')}")
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
