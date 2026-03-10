"""
Q-TRON Gen2 Stage7 Reporter

- 일일 / 기간별(주간, 월간 등) 리포트 생성
- Early Entry 테마 선반영 이유 자동 정리
- summary_daily.csv 로 요약 로그 기록

주의:
- public API 시그니처는 기존과 동일하게 유지:
    - report_daily(...)
    - auto_period_reports(...)
    - report_period(...)
"""

import os
import csv
import json
import glob
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple, Optional

# -----------------------------------------------------------------------------
# 경로 설정
# -----------------------------------------------------------------------------

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOG_DIR = os.path.join(BASE_DIR, "logs")

TRADES_CSV = os.path.join(LOG_DIR, "trades.csv")
EQUITY_CSV = os.path.join(LOG_DIR, "equity_log.csv")
DAILY_CSV = os.path.join(LOG_DIR, "daily_log.csv")
SUMMARY_DAILY_CSV = os.path.join(LOG_DIR, "summary_daily.csv")
FILTER_CSV = os.path.join(LOG_DIR, "filter_log.csv")

EARLY_DIR = os.path.join(BASE_DIR, "data", "early_signals")

PERIOD_LABEL = {
    "daily": "일일",
    "weekly": "주간",
    "monthly": "월간",
    "quarterly": "분기",
    "semiannual": "반기",
    "annual": "연간",
}


class Reporter:
    """
    Stage7에서 사용하는 리포트 생성기

    public API:
        - report_daily(...)
        - auto_period_reports(...)
        - report_period(period, ...)

    나머지는 모두 내부 헬퍼.
    """

    def __init__(self, log_dir: str = LOG_DIR):
        self.log_dir = log_dir
        os.makedirs(self.log_dir, exist_ok=True)

    # =========================================================================
    # 공개 API
    # =========================================================================

    def report_daily(
            self,
            results: List[Any],
            positioned: List[Any],
            market_state: Any,
            candidates: List[Any],
            scored: List[Dict[str, Any]],
            portfolio_summary: Dict[str, Any],
            open_browser: bool = False,
    ) -> None:
        """
        일일 리포트 생성 + 로그 기록

        results: Stage5 체결 결과 리스트
        positioned: 기존 포지션
        market_state: Enum-like (value: "BULL"/"BEAR"/"SIDEWAYS")
        candidates: Stage2 후보 종목 리스트
        scored: Q-Score 계산 결과 리스트(dict)
        portfolio_summary: 포트폴리오 요약 dict
        """
        now = datetime.now()

        # 1) 로그 기록
        self._log_equity(portfolio_summary, now)
        self._log_daily(market_state, candidates, results, now)

        # Early 신호는 한 번만 읽어서 summary에도 같이 기록
        early_data = self._load_early_signal(now.strftime("%Y-%m-%d"))
        self._log_summary_daily(market_state, now, early_data)

        # ✅ 테마 비교 로그 (Q-TRON 테마 vs HTS(0213) 업종 Top1/2)
        self._log_theme_comparison(now, early_data)

        # 2) 통계 계산
        stats = self._calc_daily_stats(results, positioned)

        # 3) HTML 생성
        html = self._build_daily_html(
            results, scored, market_state, candidates, portfolio_summary, stats, now, early_data
        )
        path = self._save_html(html, f"daily_{now.strftime('%Y%m%d_%H%M%S')}")

        # 4) 콘솔 요약 출력
        self._print_daily(results, scored, market_state, candidates, portfolio_summary, stats, now)

        # 5) 브라우저 오픈 옵션
        if open_browser:
            self._open(path)

        # 6) 필요 시 자동 period 리포트 생성
        today = now.date()
        generated: List[str] = []

        if today.weekday() == 4:  # 금요일
            self.report_period("weekly", open_browser)
            generated.append("주간")

        if self._is_last_trading_day_of_month(today):
            self.report_period("monthly", open_browser)
            generated.append("월간")

        if today.month in (3, 6, 9, 12) and self._is_last_trading_day_of_month(today):
            self.report_period("quarterly", open_browser)
            generated.append("분기")

        if today.month in (6, 12) and self._is_last_trading_day_of_month(today):
            self.report_period("semiannual", open_browser)
            generated.append("반기")

        if today.month == 12 and self._is_last_trading_day_of_month(today):
            self.report_period("annual", open_browser)
            generated.append("연간")

        if generated:
            print(f"[Reporter] 자동 생성: {', '.join(generated)} 리포트")

    def auto_period_reports(self, open_browser: bool = False) -> None:
        """
        오늘이 특정 기준일이면 자동으로 기간 리포트 생성
        - 금요일: 주간
        - 월말: 월간
        - 분기말: 분기
        - 반기말: 반기
        - 연말: 연간
        """
        today = date.today()
        generated: List[str] = []

        if today.weekday() == 4:  # 금요일
            self.report_period("weekly", open_browser)
            generated.append("주간")

        if self._is_last_trading_day_of_month(today):
            self.report_period("monthly", open_browser)
            generated.append("월간")

        if today.month in (3, 6, 9, 12) and self._is_last_trading_day_of_month(today):
            self.report_period("quarterly", open_browser)
            generated.append("분기")

        if today.month in (6, 12) and self._is_last_trading_day_of_month(today):
            self.report_period("semiannual", open_browser)
            generated.append("반기")

        if today.month == 12 and self._is_last_trading_day_of_month(today):
            self.report_period("annual", open_browser)
            generated.append("연간")

        if generated:
            print(f"[Reporter] 자동 생성: {', '.join(generated)} 리포트")

    def report_period(self, period: str, open_browser: bool = False) -> None:
        """
        기간 리포트 생성
        period: "weekly", "monthly", "quarterly", "semiannual", "annual"
        """
        now = datetime.now()
        start, end = self._period_range(period, now)
        label = PERIOD_LABEL.get(period, period)

        trades = self._load_trades(start, end)
        equities = self._load_equities(start, end)
        daily_logs = self._load_daily_logs(start, end)

        stats = self._calc_period_stats(trades, equities, daily_logs)

        if period == "monthly":
            filter_logs = self._load_filter_logs(start, end)
            detail = self._calc_monthly_detail(trades, equities, daily_logs, filter_logs)
            html = self._build_monthly_html(label, start, end, trades, equities, stats, detail, now)
        else:
            html = self._build_period_html(label, start, end, trades, equities, stats, now)

        path = self._save_html(html, f"{period}_{now.strftime('%Y%m%d_%H%M%S')}")
        self._print_period(label, start, end, stats)

        if open_browser:
            self._open(path)

    # =========================================================================
    # 로그 기록
    # =========================================================================

    def _log_equity(self, portfolio_summary: Dict[str, Any], now: datetime) -> None:
        exists = os.path.exists(EQUITY_CSV)
        with open(EQUITY_CSV, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=["timestamp", "total", "cash", "exposure"])
            if not exists:
                w.writeheader()

            total = str(portfolio_summary.get("총평가금액", "0원")).replace(",", "").replace("원", "")
            cash = str(portfolio_summary.get("현금", "0원")).replace(",", "").replace("원", "")
            exp = str(portfolio_summary.get("총노출도", "0.0%")).replace("%", "")

            w.writerow(
                {
                    "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "total": total,
                    "cash": cash,
                    "exposure": exp,
                }
            )

    def _log_daily(
        self,
        market_state: Any,
        candidates: List[Any],
        results: List[Any],
        now: datetime,
    ) -> None:
        """
        일별 시장상태 / 후보 수 / 체결 수 기록
        - 월간/기간 리포트에서 컨텍스트로 사용
        """
        exists = os.path.exists(DAILY_CSV)
        accepted = len([r for r in results if not getattr(r, "rejected", False)])
        with open(DAILY_CSV, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(
                f, fieldnames=["date", "market_state", "candidates", "accepted"]
            )
            if not exists:
                w.writeheader()

            w.writerow(
                {
                    "date": now.strftime("%Y-%m-%d"),
                    "market_state": getattr(market_state, "value", str(market_state)),
                    "candidates": len(candidates),
                    "accepted": accepted,
                }
            )

    def _log_summary_daily(
        self,
        market_state: Any,
        now: datetime,
        early_data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        summary_daily.csv 에 일자별 요약 로그 기록
        - date
        - market_state
        - early_sectors (세미콜론 연결 문자열)
        """
        exists = os.path.exists(SUMMARY_DAILY_CSV)
        with open(SUMMARY_DAILY_CSV, "a", newline="", encoding="utf-8-sig") as f:
            fieldnames = ["date", "market_state", "early_sectors"]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            if not exists:
                w.writeheader()

            sectors = ""
            if early_data:
                sectors = ";".join(early_data.get("active_sectors", []))

            w.writerow(
                {
                    "date": now.strftime("%Y-%m-%d"),
                    "market_state": getattr(market_state, "value", str(market_state)),
                    "early_sectors": sectors,
                }
            )

    def _log_theme_comparison(
        self,
        now: datetime,
        early_data: Optional[Dict[str, Any]],
    ) -> None:
        """
        Early Entry 테마 vs HTS(0213 기준) 업종 1,2위 비교 로그

        - logs/theme_log.csv 에 일자별 기록
        - TR 실패 / 모듈 미구현 시에는 조용히 스킵하고,
          Q-TRON 코어/전략/리포트 생성에는 전혀 영향 주지 않음.
        """
        # Early 데이터가 없으면 아무 것도 하지 않음
        if not early_data:
            return


        active = early_data.get("active_sectors", [])
        if not active:
            return

        # Q-TRON 테마 Top1, Top2 (필요 시 정렬/스코어링 로직 보강 가능)
        q_top1 = active[0]
        q_top2 = active[1] if len(active) > 1 else ""

        # HTS(0213) 기준 업종 Top1, Top2 조회
        try:
            try:
                # 이 모듈과 함수는 나중에 구현해도 됨.
                # (없으면 ImportError가 나고, 아래 except에서 스킵)
                from kiwoom_api_wrapper import fetch_sector_snapshot, rank_hts_top2  # type: ignore
            except ImportError:
                print("[Reporter] kiwoom_api_wrapper 모듈 없음 → 테마 비교 스킵")
                return

            df = fetch_sector_snapshot()
            hts_top1, hts_top2 = rank_hts_top2(df)
        except Exception as e:  # 외부 API 예외 방어
            print(f"[Reporter] 테마 비교용 TR 조회 실패: {e}")
            return

        # 일치 여부 플래그 계산
        match = "none"
        q_set = {q_top1, q_top2} - {""}
        h_set = {hts_top1, hts_top2} - {""}
        inter = q_set & h_set
        if len(inter) == 2:
            match = "both"
        elif len(inter) == 1:
            match = "1of2"

        # CSV 로그 기록
        theme_log_csv = os.path.join(self.log_dir, "theme_log.csv")
        exists = os.path.exists(theme_log_csv)
        with open(theme_log_csv, "a", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(
                    [
                        "date",
                        "time",
                        "q_top1",
                        "q_top2",
                        "hts_top1",
                        "hts_top2",
                        "match",
                    ]
                )
            w.writerow(
                [
                    now.strftime("%Y-%m-%d"),
                    now.strftime("%H:%M:%S"),
                    q_top1,
                    q_top2,
                    hts_top1,
                    hts_top2,
                    match,
                ]
            )

    # =========================================================================
    # 데이터 로드
    # =========================================================================

    def _load_trades(self, start: date, end: date) -> List[Dict[str, Any]]:
        if not os.path.exists(TRADES_CSV):
            return []
        rows: List[Dict[str, Any]] = []
        with open(TRADES_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    d = datetime.strptime(row["date"], "%Y-%m-%d").date()
                    if start <= d <= end:
                        rows.append(row)
                except Exception:
                    continue
        return rows

    def _load_equities(self, start: date, end: date) -> List[Dict[str, Any]]:
        if not os.path.exists(EQUITY_CSV):
            return []
        rows: List[Dict[str, Any]] = []
        with open(EQUITY_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S").date()
                    if start <= ts <= end:
                        rows.append(row)
                except Exception:
                    continue
        return rows

    def _load_daily_logs(self, start: date, end: date) -> List[Dict[str, Any]]:
        if not os.path.exists(DAILY_CSV):
            return []
        rows: List[Dict[str, Any]] = []
        with open(DAILY_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    d = datetime.strptime(row["date"], "%Y-%m-%d").date()
                    if start <= d <= end:
                        rows.append(row)
                except Exception:
                    continue
        return rows

    def _load_filter_logs(self, start: date, end: date) -> List[Dict[str, Any]]:
        if not os.path.exists(FILTER_CSV):
            return []
        rows: List[Dict[str, Any]] = []
        with open(FILTER_CSV, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                try:
                    d = datetime.strptime(row["date"], "%Y-%m-%d").date()
                    if start <= d <= end:
                        rows.append(row)
                except Exception:
                    continue
        return rows

    # =========================================================================
    # Early Entry 신호 / 테마 섹션
    # =========================================================================

    def _load_early_signal(self, date_str: str) -> Optional[Dict[str, Any]]:
        """
        data/early_signals/early_signal_YYYY-MM-DD.json 읽기
        없으면 None 반환
        """
        if not os.path.isdir(EARLY_DIR):
            return None

        path = os.path.join(EARLY_DIR, f"early_signal_{date_str}.json")
        if not os.path.exists(path):
            return None

        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"[Reporter] Early 신호 JSON 읽기 실패: {e}")
            return None

    def _build_early_section_html(
        self,
        now: datetime,
        market_state: Any,
        early_data: Optional[Dict[str, Any]],
    ) -> str:
        """
        Early Entry 활성 섹터에 대해:
          - 어떤 테마(섹터)를 선반영 했는지
          - 어떤 조건(수치) 때문에 그렇게 판단했는지
        를 나열하는 HTML 섹션.
        """
        if not early_data:
            return (
                "<h2>🚀 Early Entry 선반영 테마</h2>"
                "<p>오늘은 Early Entry 활성 섹터가 없습니다.</p>"
            )

        active_sectors = early_data.get("active_sectors", [])
        details = early_data.get("details", {})

        if not active_sectors:
            return (
                "<h2>🚀 Early Entry 선반영 테마</h2>"
                "<p>오늘은 Early Entry 활성 섹터가 없습니다.</p>"
            )

        blocks: List[str] = []
        for sector in active_sectors:
            d = details.get(sector, {})
            cond = d.get("conditions_met", 0)
            breadth = d.get("breadth_jump", 0.0) * 100.0
            vol = d.get("vol_ratio", 0.0) * 100.0
            nh = d.get("new_high_ratio", 0.0) * 100.0

            block = f"""
            <div style="margin:10px 0 14px;padding:8px 10px;border-radius:8px;
                        border:1px solid #d0d7ff;background:#f7f8ff;">
              <strong>■ {sector} (Early Entry 활성)</strong><br>
              <span style="font-size:0.9em;color:#555;">
                - Breadth Jump: {breadth:.1f}% (기준 ≥ 20%p)<br>
                - Volume Surge: {vol:.1f}% (기준 ≥ 130%)<br>
                - New High Ratio: {nh:.1f}% (기준 ≥ 7%)<br>
                → 3개 조건 중 <b>{cond}</b>개 충족
              </span><br><br>
              <span style="font-size:0.9em;color:#444;">
                위 조건을 충족하여 기존 Gen2 신호보다 평균 6.1일 정도
                빠르게 {sector} 테마를 선반영하도록 설계되었습니다.
                (장 마감 후 확정 데이터 기준 · 레짐: {getattr(market_state, 'value', market_state)})
              </span>
            </div>
            """
            blocks.append(block)

        return "<h2>🚀 Early Entry 선반영 테마</h2>" + "".join(blocks)

    # =========================================================================
    # 통계 계산
    # =========================================================================

    def _calc_daily_stats(
        self,
        results: List[Any],
        positioned: List[Any],
    ) -> Dict[str, Any]:
        """
        당일 요약 통계(체결 건수, 거부 건수 등)
        실제 전략 성과는 기간 리포트에서 주로 다룸.
        """
        total_trades = len(results)
        accepted = len([r for r in results if not getattr(r, "rejected", False)])
        rejected = total_trades - accepted
        pos_count = len(positioned)

        return {
            "체결건수": str(accepted),
            "거부건수": str(rejected),
            "전체시도": str(total_trades),
            "보유종목수": str(pos_count),
        }

    def _calc_period_stats(
        self,
        trades: List[Dict[str, Any]],
        equities: List[Dict[str, Any]],
        daily_logs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        기간 성과 요약 (아주 단순 버전)
        - 누적 실현손익 합계
        - 거래 횟수
        - 평균 일간 노출도 등
        """
        trade_count = len(trades)
        realized = 0.0
        for row in trades:
            try:
                realized += float(str(row.get("pnl", "0")).replace(",", ""))
            except Exception:
                continue

        exposure_vals: List[float] = []
        for row in equities:
            try:
                exposure_vals.append(float(row.get("exposure", "0")))
            except Exception:
                continue
        avg_exposure = sum(exposure_vals) / len(exposure_vals) if exposure_vals else 0.0

        return {
            "거래횟수": trade_count,
            "실현손익합계": realized,
            "평균노출도(%)": round(avg_exposure, 2),
            "거래일수": len(daily_logs),
        }

    def _calc_monthly_detail(
        self,
        trades: List[Dict[str, Any]],
        equities: List[Dict[str, Any]],
        daily_logs: List[Dict[str, Any]],
        filter_logs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        월간 리포트용 상세 통계 (간단 버전)
        필요 시 세부 항목 확장 가능.
        """
        # 예시: 승률, 평균 수익/손실
        gains: List[float] = []
        losses: List[float] = []
        for row in trades:
            try:
                pnl = float(str(row.get("pnl", "0")).replace(",", ""))
            except Exception:
                continue
            if pnl > 0:
                gains.append(pnl)
            elif pnl < 0:
                losses.append(pnl)

        win = len(gains)
        loss = len(losses)
        total = win + loss
        win_rate = (win / total * 100) if total > 0 else 0.0
        avg_gain = sum(gains) / len(gains) if gains else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0

        return {
            "승률": round(win_rate, 2),
            "평균수익": round(avg_gain, 2),
            "평균손실": round(avg_loss, 2),
            "거래횟수": total,
            "필터로그건수": len(filter_logs),
        }

    # =========================================================================
    # HTML 빌더
    # =========================================================================

    def _wrap_html(
        self,
        title: str,
        period_badge: str,
        period_color: str,
        subtitle: str,
        market_color: str,
        body: str,
    ) -> str:
        """
        공통 HTML 레이아웃 래퍼
        """
        return f"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<title>{title}</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
body {{
  font-family:'Malgun Gothic','Apple SD Gothic Neo',sans-serif;
  background:#0f172a;
  color:#e5e7eb;
  margin:0;
}}
.wrap {{
  max-width:900px;
  margin:0 auto;
  padding:16px;
}}
.header {{
  padding:10px 4px 14px;
  border-bottom:1px solid #1f2937;
}}
.badge {{
  display:inline-block;
  padding:2px 8px;
  border-radius:999px;
  font-size:11px;
  border:1px solid {period_color};
  color:{period_color};
  margin-right:8px;
}}
.title {{
  font-size:18px;
  font-weight:700;
}}
.subtitle {{
  font-size:12px;
  color:#9ca3af;
  margin-top:4px;
}}
h2 {{
  font-size:15px;
  margin:16px 0 8px;
}}
table {{
  width:100%;
  border-collapse:collapse;
  font-size:11px;
}}
th,td {{
  padding:4px 6px;
  border-bottom:1px solid #111827;
}}
th {{
  text-align:left;
  color:#9ca3af;
  font-weight:500;
}}
tr:hover {{
  background:#020617;
}}
</style>
</head>
<body>
<div class="wrap">
  <div class="header">
    <div class="title">
      <span class="badge">{period_badge}</span>{title}
    </div>
    <div class="subtitle" style="color:{market_color};">{subtitle}</div>
  </div>
  {body}
</div>
</body>
</html>
"""

    def _build_daily_html(
        self,
        results: List[Any],
        scored: List[Dict[str, Any]],
        market_state: Any,
        candidates: List[Any],
        portfolio: Dict[str, Any],
        stats: Dict[str, Any],
        now: datetime,
        early_data: Optional[Dict[str, Any]],
    ) -> str:
        """
        일일 리포트 HTML 생성
        - 상단: Early Entry 선반영 테마 + 이유
        - 당일 요약, Q-Score, 체결 내역, 포트폴리오
        """
        sc = {"BULL": "#2ecc71", "BEAR": "#e74c3c", "SIDEWAYS": "#f39c12"}
        color = sc.get(getattr(market_state, "value", str(market_state)), "#888")

        # Early 섹션
        early_section_html = self._build_early_section_html(now, market_state, early_data)

        # 체결 내역 테이블
        trade_rows = ""
        for r in results:
            rejected = getattr(r, "rejected", False)
            bg = "#fff0f0" if rejected else "#f0fff0"
            code = getattr(r, "code", "")
            name = getattr(r, "name", "")
            side = "거부" if rejected else "체결"
            qty = getattr(r, "quantity", 0)
            price = getattr(r, "exec_price", 0)
            slip = getattr(r, "slippage_pct", 0.0)
            reason = getattr(r, "reject_reason", "")
            trade_rows += (
                f'<tr style="background:{bg}">'
                f"<td>{code}</td>"
                f"<td>{name}</td>"
                f"<td>{side}</td>"
                f"<td>{qty}</td>"
                f"<td>{price:,.0f}</td>"
                f"<td>{slip:.3%}</td>"
                f"<td>{reason}</td>"
                "</tr>"
            )

        # Q-Score 테이블
        score_rows = ""
        for s in scored:
            bd = s.get("breakdown", {})
            score_rows += (
                "<tr>"
                f"<td><b>{s.get('code','')}</b></td>"
                f"<td>{s.get('q_score',0):.1f}</td>"
                f"<td>{bd.get('technical',0):.2f}</td>"
                f"<td>{bd.get('demand',0):.2f}</td>"
                f"<td>{bd.get('price',0):.2f}</td>"
                f"<td>{bd.get('alpha',0):.2f}</td>"
                "</tr>"
            )

        port_rows = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in portfolio.items()
        )
        stat_rows = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in stats.items()
        )

        return self._wrap_html(
            title=f"Q-TRON 일일 리포트 {now.strftime('%Y-%m-%d')}",
            period_badge="일일",
            period_color="#3498db",
            subtitle=(
                f"{now.strftime('%Y-%m-%d %H:%M')} | "
                f"시장: {getattr(market_state, 'value', market_state)} | "
                f"후보: {len(candidates)}종목"
            ),
            market_color=color,
            body=f"""
            {early_section_html}

            <h2>📈 당일 요약</h2>
            <table>
              <tr><th>항목</th><th>값</th></tr>
              {stat_rows}
            </table>

            <h2>🏆 Q-Score 순위</h2>
            <table>
              <tr>
                <th>종목</th><th>Q-Score</th>
                <th>Technical</th><th>Demand</th><th>Price</th><th>Alpha</th>
              </tr>
              {score_rows}
            </table>

            <h2>✅ 체결 내역</h2>
            <table>
              <tr>
                <th>코드</th><th>종목</th><th>구분</th><th>수량</th>
                <th>체결가</th><th>슬리피지</th><th>사유</th>
              </tr>
              {trade_rows}
            </table>

            <h2>💼 포트폴리오</h2>
            <table>
              <tr><th>항목</th><th>값</th></tr>
              {port_rows}
            </table>
            """,
        )

    def _build_period_html(
        self,
        label: str,
        start: date,
        end: date,
        trades: List[Dict[str, Any]],
        equities: List[Dict[str, Any]],
        stats: Dict[str, Any],
        now: datetime,
    ) -> str:
        """
        주간/분기/연간 등 공통 기간 리포트 (간단 버전)
        """
        stat_rows = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in stats.items()
        )
        period_str = f"{start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}"

        return self._wrap_html(
            title=f"Q-TRON {label} 리포트 ({period_str})",
            period_badge=label,
            period_color="#6366f1",
            subtitle=f"생성 시각: {now.strftime('%Y-%m-%d %H:%M')}",
            market_color="#6366f1",
            body=f"""
            <h2>📊 기간 요약 ({period_str})</h2>
            <table>
              <tr><th>항목</th><th>값</th></tr>
              {stat_rows}
            </table>
            """,
        )

    def _build_monthly_html(
        self,
        label: str,
        start: date,
        end: date,
        trades: List[Dict[str, Any]],
        equities: List[Dict[str, Any]],
        stats: Dict[str, Any],
        detail: Dict[str, Any],
        now: datetime,
    ) -> str:
        """
        월간 리포트 전용 HTML (조금 더 상세)
        """
        period_str = f"{start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}"
        stat_rows = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in stats.items()
        )
        det_rows = "".join(
            f"<tr><td>{k}</td><td><b>{v}</b></td></tr>" for k, v in detail.items()
        )

        return self._wrap_html(
            title=f"Q-TRON {label} 리포트 ({period_str})",
            period_badge=label,
            period_color="#10b981",
            subtitle=f"생성 시각: {now.strftime('%Y-%m-%d %H:%M')}",
            market_color="#10b981",
            body=f"""
            <h2>📊 월간 요약</h2>
            <table>
              <tr><th>항목</th><th>값</th></tr>
              {stat_rows}
            </table>

            <h2>🔍 월간 상세 지표</h2>
            <table>
              <tr><th>항목</th><th>값</th></tr>
              {det_rows}
            </table>
            """,
        )

    # =========================================================================
    # 유틸
    # =========================================================================

    def _save_html(self, html: str, name: str) -> str:
        os.makedirs(self.log_dir, exist_ok=True)
        path = os.path.join(self.log_dir, f"report_{name}.html")
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"[Reporter] HTML 저장 → {path}")
        return path

    def _open(self, path: str) -> None:
        """
        OS 기본 브라우저로 HTML 열기
        """
        try:
            import webbrowser

            webbrowser.open(f"file://{os.path.abspath(path)}")
        except Exception as e:
            print(f"[Reporter] 브라우저 오픈 실패: {e}")

    def _print_daily(
        self,
        results: List[Any],
        scored: List[Dict[str, Any]],
        market_state: Any,
        candidates: List[Any],
        portfolio: Dict[str, Any],
        stats: Dict[str, Any],
        now: datetime,
    ) -> None:
        """
        콘솔용 간단 요약 출력
        """
        print("\n============================================================")
        print(f"  Q-TRON 일일 리포트  {now.strftime('%Y-%m-%d %H:%M')}")
        print("============================================================\n")
        print(f"[시장] {getattr(market_state, 'value', market_state)}  |  후보: {len(candidates)}종목")
        print("\n[당일 요약]")
        for k, v in stats.items():
            print(f"  {k}: {v}")
        print("\n[포트폴리오 요약]")
        for k, v in portfolio.items():
            print(f"  {k}: {v}")
        print("============================================================\n")

    def _print_period(
        self,
        label: str,
        start: date,
        end: date,
        stats: Dict[str, Any],
    ) -> None:
        period_str = f"{start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}"
        print("\n============================================================")
        print(f"  Q-TRON {label} 리포트  ({period_str})")
        print("============================================================\n")
        for k, v in stats.items():
            print(f"  {k}: {v}")
        print("============================================================\n")

    # ---- 기간 계산 / 월말 판단 ---------------------------------------------

    def _period_range(self, period: str, now: datetime) -> Tuple[date, date]:
        """
        기간 코드별 날짜 범위 계산
        """
        end = now.date()
        if period == "weekly":
            start = end - timedelta(days=6)
        elif period == "monthly":
            start = end.replace(day=1)
        elif period == "quarterly":
            month = ((end.month - 1) // 3) * 3 + 1
            start = date(end.year, month, 1)
        elif period == "semiannual":
            month = 1 if end.month <= 6 else 7
            start = date(end.year, month, 1)
        elif period == "annual":
            start = date(end.year, 1, 1)
        else:  # fallback
            start = end
        return start, end

    def _is_last_trading_day_of_month(self, d: date) -> bool:
        """
        단순 월말 판단 (주말 제외 X, 필요 시 거래일 캘린더와 연동 가능)
        """
        next_day = d + timedelta(days=1)
        return next_day.month != d.month