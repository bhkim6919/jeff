"""
meta_collector.py -- Gen5 Meta Layer Phase 0: US EOD metric collection
=======================================================================
run_eod() 완료 후 호출. runtime truth만 사용 (provider 재조회 금지).
Observer-only.
"""
from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

import numpy as np
import pandas as pd

from lab import meta_db

logger = logging.getLogger("lab.meta")


def collect_meta_us(
    eod_date: str,
    close_dict: Dict[str, float],
    full_close_dict: Dict[str, pd.Series],
    ohlcv_dict: Dict[str, pd.DataFrame],
    strategies: Dict[str, dict],
    runtime_states: Dict[str, Any],
    spy_series: Optional[pd.Series],
    sector_map: Dict[str, dict],
    snapshot_version: str = "",
    run_meta: Optional[Dict] = None,
    universe_count: int = 0,
) -> None:
    """Collect and store all US meta metrics from runtime snapshot."""
    _sv = snapshot_version or (run_meta or {}).get("snapshot_version", "")

    try:
        data_snapshot_id = _compute_snapshot_id(eod_date, close_dict, spy_series)

        # 1. Market context
        mc = _compute_market_context(
            eod_date, close_dict, full_close_dict,
            spy_series, sector_map, data_snapshot_id,
        )
        meta_db.save_market_context(mc)

        # 2. Strategy daily + exposure + risk
        sd_rows = []
        se_rows = []
        sr_rows = []
        for sname, scfg in sorted(strategies.items()):
            state = runtime_states.get(sname)
            if not state:
                continue

            if "version" not in scfg:
                raise ValueError(f"[META] Missing version for strategy: {sname}")
            version = f"{sname}_{scfg['version']}"

            sd = _compute_strategy_daily(
                eod_date, sname, version, state, close_dict, full_close_dict,
            )
            sd_rows.append(sd)

            se = _compute_strategy_exposure(
                eod_date, sname, state, close_dict, sector_map,
            )
            se_rows.append(se)

            sr = _compute_strategy_risk_us(eod_date, sname, state, _sv)
            sr_rows.append(sr)

        meta_db.save_strategy_daily(sd_rows)
        meta_db.save_strategy_exposure(se_rows)
        meta_db.save_strategy_risk_daily(sr_rows)

        # 3. Run quality
        meta_db.save_run_quality({
            "trade_date": eod_date,
            "snapshot_version": _sv,
            "market": "US",
            "sync_status": (run_meta or {}).get("sync_status", "?"),
            "selected_source": (run_meta or {}).get("selected_source", "Alpaca"),
            "data_snapshot_id": data_snapshot_id,
            "degraded_flag": (run_meta or {}).get("degraded_flag", 0),
            "ohlc_invariant_warn_count": (run_meta or {}).get("ohlc_invariant_warn_count", 0),
        })

        # 4. Universe snapshot
        meta_db.save_universe_snapshot({
            "trade_date": eod_date,
            "snapshot_version": _sv,
            "universe_count_raw": universe_count,
            "universe_count_filtered": len(close_dict),
            "tradable_count": len(close_dict),
        })

        # 5. Recommendation log (EOD 직후 확실히 저장)
        try:
            from lab.meta_summary import build_daily_summary_us
            build_daily_summary_us(eod_date)
        except Exception as e:
            logger.warning(f"[META] recommendation_log save failed (non-fatal): {e}")

        # 6. Verify
        _sanity_check(mc, sd_rows)
        verification = meta_db.verify_row_counts(eod_date, len(strategies))
        if verification["missing_strategies"]:
            logger.warning(f"[META_VERIFY] Missing: {verification['missing_strategies']}")
        else:
            logger.info(
                f"[META_OK] {eod_date}: mc=1, sd={verification['strategy_daily']}, "
                f"se={verification['exposure_daily']}, "
                f"rq=1, sr={len(sr_rows)}, usd=1, rec=1"
            )

    except Exception as e:
        logger.error(f"[META] Collection failed: {e}", exc_info=True)


def _compute_snapshot_id(
    eod_date: str,
    close_dict: Dict[str, float],
    spy_series: Optional[pd.Series],
) -> str:
    """Reproducibility key from runtime data."""
    parts = [eod_date, str(len(close_dict))]
    if spy_series is not None and len(spy_series) > 0:
        parts.append(str(spy_series.index[-1]))
        parts.append(f"{spy_series.iloc[-1]:.2f}")
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ── Market Context ────────────────────────────────────────────

def _compute_market_context(
    eod_date: str,
    close_dict: Dict[str, float],
    full_close_dict: Dict[str, pd.Series],
    spy_series: Optional[pd.Series],
    sector_map: Dict[str, dict],
    data_snapshot_id: str,
) -> dict:
    # index_return (SPY)
    index_return = None
    if spy_series is not None and len(spy_series) >= 2:
        prev = float(spy_series.iloc[-2])
        curr = float(spy_series.iloc[-1])
        if prev > 0:
            index_return = round((curr - prev) / prev, 6)

    # adv_ratio: fraction of stocks with positive return
    adv_ratio = None
    adv_count = 0
    total_count = 0
    for sym, series in full_close_dict.items():
        if len(series) < 2:
            continue
        prev = float(series.iloc[-2])
        curr = close_dict.get(sym, 0)
        if prev <= 0 or curr <= 0:
            continue
        total_count += 1
        if curr > prev:
            adv_count += 1
    if total_count > 10:
        adv_ratio = round(adv_count / total_count, 4)

    # sector_dispersion: std of sector avg returns
    sector_dispersion = None
    if sector_map and total_count > 10:
        sector_returns = defaultdict(list)
        for sym, series in full_close_dict.items():
            if len(series) < 2:
                continue
            prev = float(series.iloc[-2])
            curr = close_dict.get(sym, 0)
            if prev <= 0 or curr <= 0:
                continue
            ret = (curr - prev) / prev
            info = sector_map.get(sym, {})
            sector = info.get("sector", "Other") if isinstance(info, dict) else "Other"
            sector_returns[sector].append(ret)
        if len(sector_returns) >= 3:
            sector_avgs = [np.mean(v) for v in sector_returns.values() if len(v) >= 2]
            if len(sector_avgs) >= 3:
                sector_dispersion = round(float(np.std(sector_avgs)), 6)

    # breakout_ratio: fraction with close >= 60-day high
    breakout_ratio = None
    br_count = 0
    br_total = 0
    for sym, series in full_close_dict.items():
        if len(series) < 60:
            continue
        high_60 = float(series.iloc[-61:-1].max())  # past 60 days excluding today
        curr = close_dict.get(sym, 0)
        if high_60 <= 0 or curr <= 0:
            continue
        br_total += 1
        if curr >= high_60:
            br_count += 1
    if br_total > 10:
        breakout_ratio = round(br_count / br_total, 4)

    return {
        "trade_date": eod_date,
        "index_return": index_return,
        "adv_ratio": adv_ratio,
        "sector_dispersion": sector_dispersion,
        "breakout_ratio": breakout_ratio,
        "data_snapshot_id": data_snapshot_id,
    }


# ── Strategy Daily ────────────────────────────────────────────

def _compute_strategy_daily(
    eod_date: str,
    sname: str,
    version: str,
    state: Any,
    close_dict: Dict[str, float],
    full_close_dict: Dict[str, pd.Series],
) -> dict:
    # State can be dict (from ForwardStrategyState.to_dict())
    if isinstance(state, dict):
        positions = state.get("positions", {})
        cash = state.get("cash", 100_000)
        equity_history = state.get("equity_history", [])
    else:
        positions = getattr(state, "positions", {})
        cash = getattr(state, "cash", 100_000)
        equity_history = getattr(state, "equity_history", [])

    # Current equity
    pos_value = 0.0
    for sym, pos in positions.items():
        price = close_dict.get(sym, 0)
        _q = pos.get("quantity") if isinstance(pos, dict) else getattr(pos, "quantity", None)
        qty = _q if _q is not None else (pos.get("qty", 0) if isinstance(pos, dict) else getattr(pos, "qty", 0))
        if price > 0 and qty > 0:
            pos_value += qty * price
    equity = cash + pos_value

    # Previous equity
    initial_cash = 100_000
    if len(equity_history) >= 2:
        prev_eq = equity_history[-2]
        prev_equity = prev_eq[1] if isinstance(prev_eq, (list, tuple)) else prev_eq.get("equity", initial_cash)
    else:
        prev_equity = initial_cash

    daily_return = round((equity - prev_equity) / prev_equity, 6) if prev_equity > 0 else None

    # ── Data quality gate (KR/US 동일 기준: _OUTLIER_THRESHOLD=0.5) ──
    _OUTLIER_THRESHOLD = 0.5
    if daily_return is not None and abs(daily_return) > _OUTLIER_THRESHOLD:
        logger.warning(
            f"[META_RETURN_DEBUG] {eod_date} {sname}: OUTLIER daily_return={daily_return} "
            f"equity={equity:.2f} prev={prev_equity:.2f} → set None"
        )
        daily_return = None

    if len(positions) > 0 and pos_value < 1e-8:
        logger.warning(
            f"[META_RETURN_DEBUG] {eod_date} {sname}: SNAPSHOT_MISMATCH "
            f"position_count={len(positions)} pos_value={pos_value:.2f}"
        )

    cumul_return = round((equity - initial_cash) / initial_cash, 6) if initial_cash > 0 else None

    # Win/loss
    win_count = 0
    loss_count = 0
    for sym in positions:
        series = full_close_dict.get(sym)
        if series is None or len(series) < 2:
            continue
        prev_c = float(series.iloc[-2])
        curr_c = close_dict.get(sym, 0)
        if prev_c <= 0 or curr_c <= 0:
            continue
        if curr_c > prev_c:
            win_count += 1
        elif curr_c < prev_c:
            loss_count += 1

    # Turnover: approximate from positions entered today
    turnover = None
    if prev_equity > 0:
        trade_amount = 0.0
        for sym, pos in positions.items():
            entry_date = pos.get("entry_date") if isinstance(pos, dict) else getattr(pos, "entry_date", "")
            if entry_date == eod_date:
                _q = pos.get("quantity") if isinstance(pos, dict) else getattr(pos, "quantity", None)
                qty = _q if _q is not None else (pos.get("qty", 0) if isinstance(pos, dict) else getattr(pos, "qty", 0))
                price = close_dict.get(sym, 0)
                trade_amount += qty * price
        # Exits are not tracked in final state, so this is BUY-side only
        turnover = round(trade_amount / prev_equity, 6)

    cash_ratio = round(cash / equity, 4) if equity > 0 else None
    gross_exposure = round(pos_value / equity, 4) if equity > 0 else None

    # DQ_EQUIV: pos_value==0 ↔ gross_exposure==0 동치 검증 로그
    pv_zero = abs(pos_value) < 1e-8
    ge_zero = gross_exposure is None or abs(gross_exposure) < 1e-8
    if pv_zero != ge_zero:
        logger.warning(
            f"[META_DQ_EQUIV] {eod_date} {sname}: pv_zero={pv_zero} ge_zero={ge_zero} "
            f"pos_value={pos_value:.4f} gross_exposure={gross_exposure} equity={equity:.2f}"
        )

    logger.debug(
        f"[META_RETURN_DEBUG] {eod_date} {sname}: dr={daily_return} cr={cumul_return} "
        f"pc={len(positions)} pv={pos_value:.2f} ge={gross_exposure} "
        f"cash={cash:.2f} equity={equity:.2f}"
    )

    return {
        "trade_date": eod_date,
        "strategy": sname,
        "strategy_version": version,
        "daily_return": daily_return,
        "cumul_return": cumul_return,
        "position_count": len(positions),
        "win_count": win_count,
        "loss_count": loss_count,
        "turnover": turnover,
        "cash_ratio": cash_ratio,
        "gross_exposure": gross_exposure,
    }


# ── Strategy Exposure ────────────────────────────────────────

def _compute_strategy_exposure(
    eod_date: str,
    sname: str,
    state: Any,
    close_dict: Dict[str, float],
    sector_map: Dict[str, dict],
) -> dict:
    if isinstance(state, dict):
        positions = state.get("positions", {})
        cash = state.get("cash", 100_000)
    else:
        positions = getattr(state, "positions", {})
        cash = getattr(state, "cash", 100_000)

    equity = cash
    weights = {}
    for sym, pos in positions.items():
        _q = pos.get("quantity") if isinstance(pos, dict) else getattr(pos, "quantity", None)
        qty = _q if _q is not None else (pos.get("qty", 0) if isinstance(pos, dict) else getattr(pos, "qty", 0))
        price = close_dict.get(sym, 0)
        val = qty * price
        equity += val
        weights[sym] = val

    if equity > 0:
        weights = {k: v / equity for k, v in weights.items()}

    sorted_w = sorted(weights.values(), reverse=True)
    top1_weight = round(sorted_w[0], 4) if sorted_w else None
    top5_weight = round(sum(sorted_w[:5]), 4) if len(sorted_w) >= 5 else (
        round(sum(sorted_w), 4) if sorted_w else None
    )

    sector_top1 = None
    sector_top1_weight = None
    sector_disp = None
    if weights and sector_map:
        sector_weights = defaultdict(float)
        for sym, w in weights.items():
            info = sector_map.get(sym, {})
            sector = info.get("sector", "Other") if isinstance(info, dict) else "Other"
            sector_weights[sector] += w
        if sector_weights:
            top_sec = max(sector_weights, key=sector_weights.get)
            sector_top1 = top_sec
            sector_top1_weight = round(sector_weights[top_sec], 4)
            if len(sector_weights) >= 2:
                sector_disp = round(float(np.std(list(sector_weights.values()))), 6)

    return {
        "trade_date": eod_date,
        "strategy": sname,
        "top1_weight": top1_weight,
        "top5_weight": top5_weight,
        "sector_top1": sector_top1,
        "sector_top1_weight": sector_top1_weight,
        "sector_dispersion": sector_disp,
    }


# ── Strategy Risk (US) ────────────────────────────────────────

def _compute_strategy_risk_us(
    eod_date: str,
    sname: str,
    state: Any,
    snapshot_version: str = "",
) -> dict:
    """Compute rolling risk metrics from equity_history. NULL if insufficient data."""
    if isinstance(state, dict):
        eh = state.get("equity_history", [])
    else:
        eh = getattr(state, "equity_history", [])

    equities = []
    for e in eh:
        if isinstance(e, (list, tuple)):
            equities.append(float(e[1]) if len(e) > 1 else 0)
        elif isinstance(e, dict):
            equities.append(e.get("equity", 0))
        else:
            equities.append(float(e) if e else 0)

    n = len(equities)
    result = {
        "trade_date": eod_date,
        "strategy": sname,
        "snapshot_version": snapshot_version,
        "daily_mdd": None,
        "rolling_5d_return": None,
        "rolling_20d_return": None,
        "rolling_20d_mdd": None,
        "realized_vol_20d": None,
        "hit_rate_20d": None,
        "avg_hold_days": None,
        "slippage_bps_est": None,
        "cost_bps_est": None,
    }

    if n < 2:
        return result

    returns = []
    for i in range(1, n):
        if equities[i - 1] > 0:
            returns.append((equities[i] - equities[i - 1]) / equities[i - 1])
        else:
            returns.append(0)

    if len(returns) >= 5:
        r5 = returns[-5:]
        cum5 = 1
        for r in r5:
            cum5 *= (1 + r)
        result["rolling_5d_return"] = round(cum5 - 1, 6)

    if len(returns) >= 20:
        r20 = returns[-20:]
        cum20 = 1
        for r in r20:
            cum20 *= (1 + r)
        result["rolling_20d_return"] = round(cum20 - 1, 6)

        peak = equities[-21] if len(equities) > 20 else equities[0]
        max_dd = 0
        for eq in equities[-20:]:
            if eq > peak:
                peak = eq
            dd = (eq - peak) / peak if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd
        result["rolling_20d_mdd"] = round(max_dd, 6)
        result["realized_vol_20d"] = round(float(np.std(r20)) * (252 ** 0.5), 6)
        wins = sum(1 for r in r20 if r > 0)
        result["hit_rate_20d"] = round(wins / 20, 4)
    elif len(returns) >= 2:
        result["realized_vol_20d"] = round(float(np.std(returns)) * (252 ** 0.5), 6)
        wins = sum(1 for r in returns if r > 0)
        result["hit_rate_20d"] = round(wins / len(returns), 4)

    if n >= 2:
        result["daily_mdd"] = round(min(0, returns[-1]), 6)

    # avg_hold_days
    positions = state.get("positions", {}) if isinstance(state, dict) else getattr(state, "positions", {})
    hold_days = []
    for sym, pos in positions.items():
        ed = pos.get("entry_date") if isinstance(pos, dict) else getattr(pos, "entry_date", "")
        if ed:
            try:
                hold = (pd.Timestamp(eod_date) - pd.Timestamp(ed)).days
                hold_days.append(hold)
            except Exception:
                pass
    if hold_days:
        result["avg_hold_days"] = round(np.mean(hold_days), 1)

    return result


# ── Sanity ────────────────────────────────────────────────────

def _sanity_check(mc: dict, sd_rows: list) -> None:
    adv = mc.get("adv_ratio")
    if adv is not None and (adv < 0 or adv > 1):
        logger.warning(f"[META_SANITY] adv_ratio={adv} out of [0,1]")
    br = mc.get("breakout_ratio")
    if br is not None and br > 0.3:
        logger.warning(f"[META_SANITY] breakout_ratio={br} > 30%")
    for row in sd_rows:
        t = row.get("turnover")
        if t is not None and t > 2.0:
            logger.warning(f"[META_SANITY] {row['strategy']} turnover={t} > 200%")
