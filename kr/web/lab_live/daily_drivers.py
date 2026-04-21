"""
daily_drivers.py — Lab Live per-lane daily analysis drivers
============================================================
Reconstructed after pre-commit stash incident (2026-04-20) wiped this untracked file.
Contract derived from call sites in `web/app.py:/api/lab/live/meta` and
`web/static/lab_live.js:renderExpandedPanel`.

Output schema expected by lab_live.js:
    {
      "ok": bool,
      "market":  {kospi_close, kospi_day_pct, kospi_series, regime_hint},
      "strategy":{day_pct, cumul_pct, delta_vs_kospi, equity_series},
      "top_contributors": [{code, name, daily_pct, daily_amount, is_new_today, weight_pct}],
      "sectors": [{sector, weight, count}]
    }
Baseline for `daily_pct` follows the UI tag:
    - is_new_today=True → entry_price  (신규)
    - else              → 전일 종가    (보유)
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────
# KOSPI series
# ────────────────────────────────────────────────────────────
def build_kospi_series(db, trade_date: str, window: int = 30) -> List[Dict[str, Any]]:
    """Return [{date, close}] for last `window` KOSPI sessions up to trade_date."""
    try:
        df = db.get_kospi_index()
        if df is None or df.empty:
            return []
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        cutoff = pd.to_datetime(trade_date, errors="coerce")
        if cutoff is not None and not pd.isna(cutoff):
            df = df[df["date"] <= cutoff]
        df = df.sort_values("date").tail(int(window))
        out: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            out.append({
                "date": str(r["date"].date()),
                "close": float(r["close"]) if pd.notna(r["close"]) else None,
            })
        return out
    except Exception as e:
        logger.warning("[DRIVERS] build_kospi_series failed: %s", e)
        return []


# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────
def _safe_pct(numer: Optional[float], denom: Optional[float]) -> Optional[float]:
    try:
        if numer is None or denom is None or denom == 0:
            return None
        return float((numer / denom - 1.0) * 100.0)
    except Exception:
        return None


def _regime_hint_from_kospi(kospi_series: List[Dict[str, Any]]) -> str:
    if not kospi_series or len(kospi_series) < 2:
        return "-"
    try:
        closes = [k["close"] for k in kospi_series if k.get("close") is not None]
        if len(closes) < 2:
            return "-"
        first, last = closes[0], closes[-1]
        chg = (last / first - 1.0) * 100.0
        n = len(closes)
        if chg > 2.0:
            return f"상승세 ({chg:+.1f}%, {n}D)"
        if chg < -2.0:
            return f"하락세 ({chg:+.1f}%, {n}D)"
        return f"횡보 ({chg:+.1f}%, {n}D)"
    except Exception:
        return "-"


def _prev_close(code: str, db_provider, trade_date: str) -> Optional[float]:
    """Close of the session immediately before `trade_date` for `code` (DB)."""
    try:
        df = db_provider.get_ohlcv(code, end=trade_date)
        if df is None or df.empty:
            return None
        # If last row is exactly trade_date, use row[-2]; else row[-1] is already prev.
        last_date = pd.to_datetime(df.iloc[-1]["date"], errors="coerce")
        cutoff = pd.to_datetime(trade_date, errors="coerce")
        if cutoff is not None and last_date is not None and last_date.date() == cutoff.date():
            if len(df) >= 2:
                return float(df.iloc[-2]["close"])
            return None
        return float(df.iloc[-1]["close"])
    except Exception:
        return None


# ────────────────────────────────────────────────────────────
# Main builder
# ────────────────────────────────────────────────────────────
def build_drivers_for_lane(
    lane,
    sname: str,
    trade_date: str,
    sector_map: Dict[str, Any],
    initial_cash: float,
    kospi_series: List[Dict[str, Any]],
    db_provider,
    window: int = 30,
) -> Dict[str, Any]:
    """Build the per-lane daily-analysis payload consumed by lab_live.js."""
    try:
        positions = getattr(lane, "positions", {}) or {}
        equity_history = getattr(lane, "equity_history", []) or []
        cash = float(getattr(lane, "cash", 0) or 0)
        init_cash = float(initial_cash) if initial_cash else 100_000_000.0

        # ── Market block ──────────────────────────────────────
        kospi_close: Optional[float] = None
        kospi_day_pct: Optional[float] = None
        if kospi_series:
            k_last = kospi_series[-1]
            kospi_close = k_last.get("close")
            if len(kospi_series) >= 2:
                k_prev = kospi_series[-2]
                kospi_day_pct = _safe_pct(k_last.get("close"), k_prev.get("close"))
        regime_hint = _regime_hint_from_kospi(kospi_series)

        # ── Strategy block ────────────────────────────────────
        total_equity = cash + sum(
            (int(getattr(p, "qty", 0) or 0) * float(getattr(p, "current_price", 0) or 0))
            for p in positions.values()
        )

        eq_rows = equity_history[-int(window):] if window else list(equity_history)
        equity_series = [
            {
                "date": str(r.get("date") or ""),
                "return_pct": (
                    (float(r.get("equity", 0) or 0) / init_cash - 1.0) * 100.0
                    if init_cash else 0.0
                ),
            }
            for r in eq_rows
            if r and r.get("date")
        ]

        day_pct: Optional[float] = None
        if len(equity_history) >= 2:
            cur_eq = float(equity_history[-1].get("equity") or 0)
            prev_eq = float(equity_history[-2].get("equity") or 0)
            day_pct = _safe_pct(cur_eq, prev_eq)
        elif equity_history:
            cur_eq = float(equity_history[-1].get("equity") or 0)
            day_pct = _safe_pct(cur_eq, init_cash)

        cumul_pct = _safe_pct(total_equity, init_cash)

        delta_vs_kospi: Optional[float] = None
        if cumul_pct is not None and kospi_series and len(kospi_series) >= 2:
            k_first = kospi_series[0].get("close")
            k_last_c = kospi_series[-1].get("close")
            kospi_cum = _safe_pct(k_last_c, k_first) if (k_first and k_last_c) else None
            if kospi_cum is not None:
                delta_vs_kospi = cumul_pct - kospi_cum

        # ── Contributors ──────────────────────────────────────
        contributors: List[Dict[str, Any]] = []
        for code, pos in positions.items():
            qty = int(getattr(pos, "qty", 0) or 0)
            cur = float(getattr(pos, "current_price", 0) or 0)
            entry = float(getattr(pos, "entry_price", 0) or 0)
            entry_date = str(getattr(pos, "entry_date", "") or "")
            name = str(getattr(pos, "name", "") or "")
            is_new_today = (entry_date == str(trade_date))

            if is_new_today:
                baseline = entry
            else:
                baseline = _prev_close(code, db_provider, str(trade_date))
                if baseline is None or baseline == 0:
                    baseline = entry  # fallback

            daily_pct = _safe_pct(cur, baseline) if (cur and baseline) else None
            daily_amount: Optional[float] = None
            if baseline and qty and cur:
                daily_amount = float((cur - baseline) * qty)

            mkt_value = qty * cur
            weight_pct = (mkt_value / total_equity * 100.0) if total_equity > 0 else 0.0

            contributors.append({
                "code": code,
                "name": name,
                "daily_pct": daily_pct,
                "daily_amount": daily_amount,
                "is_new_today": is_new_today,
                "weight_pct": weight_pct,
            })

        contributors.sort(
            key=lambda c: abs(c.get("daily_amount") or 0),
            reverse=True,
        )
        top_contributors = contributors[:10]

        # ── Sectors ───────────────────────────────────────────
        sector_agg: Dict[str, Dict[str, float]] = {}
        for code, pos in positions.items():
            qty = int(getattr(pos, "qty", 0) or 0)
            cur = float(getattr(pos, "current_price", 0) or 0)
            mkt_value = qty * cur
            info = sector_map.get(code) if isinstance(sector_map, dict) else None
            sector = (info or {}).get("sector") or "기타"
            agg = sector_agg.setdefault(sector, {"value": 0.0, "count": 0})
            agg["value"] += mkt_value
            agg["count"] += 1

        sectors: List[Dict[str, Any]] = []
        for s, agg in sector_agg.items():
            weight = (agg["value"] / total_equity * 100.0) if total_equity > 0 else 0.0
            sectors.append({
                "sector": s,
                "weight": weight,
                "count": int(agg["count"]),
            })
        sectors.sort(key=lambda s: s["weight"], reverse=True)

        return {
            "ok": True,
            "market": {
                "kospi_close": kospi_close,
                "kospi_day_pct": kospi_day_pct,
                "kospi_series": kospi_series,
                "regime_hint": regime_hint,
            },
            "strategy": {
                "day_pct": day_pct,
                "cumul_pct": cumul_pct,
                "delta_vs_kospi": delta_vs_kospi,
                "equity_series": equity_series,
            },
            "top_contributors": top_contributors,
            "sectors": sectors,
        }
    except Exception as e:
        logger.warning("[DRIVERS] build_drivers_for_lane(%s) failed: %s", sname, e)
        return {"ok": False, "error": str(e)}
