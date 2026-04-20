"""
meta_collector.py -- Gen5 Meta Layer Phase 0: EOD metric collection
====================================================================
run_daily() 완료 후 호출. 동일 EOD snapshot 기준으로 모든 값 계산.
Observer-only — 추천/비중조절 금지.

Truth source: close/high/vol matrix (DB에서 로드된 동일 snapshot).
외부 JSON 의존 없음 (regime_score만 file 참조, 없으면 NULL).
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, FrozenSet, List, Optional

import numpy as np
import pandas as pd

from web.lab_live import meta_db

logger = logging.getLogger("lab.meta")


def collect_meta(
    today_date: str,
    today_idx: int,
    close: pd.DataFrame,
    high: pd.DataFrame,
    vol: pd.DataFrame,
    universe: FrozenSet[str],
    sector_map: dict,
    index_series: pd.Series,
    fundamental: Optional[pd.DataFrame],
    lanes: dict,
    new_trades: list,
    data_snapshot_id: str,
    config: Any,
    run_meta: Optional[Dict] = None,
    universe_raw_count: int = 0,
    universe_filtered_count: int = 0,
    missing_data_count: int = 0,
) -> None:
    """Collect and store all meta metrics for today's EOD.

    run_meta: engine._run_meta dict containing snapshot_version, sync info.
    """
    _sv = (run_meta or {}).get("snapshot_version", "")

    try:
        # 1. Market context
        mc = _compute_market_context(
            today_date, today_idx, close, high, vol,
            universe, sector_map, index_series, data_snapshot_id,
        )
        meta_db.save_market_context(mc)

        # 2. Strategy daily + exposure + risk
        sd_rows = []
        se_rows = []
        sr_rows = []
        for sname, lane in sorted(lanes.items()):
            sd = _compute_strategy_daily(
                today_date, today_idx, sname, lane,
                close, new_trades, config,
            )
            sd_rows.append(sd)

            se = _compute_strategy_exposure(
                today_date, sname, lane, close, today_idx,
                sector_map, fundamental,
            )
            se_rows.append(se)

            sr = _compute_strategy_risk(today_date, sname, lane, config, _sv)
            sr_rows.append(sr)

        meta_db.save_strategy_daily(sd_rows)
        meta_db.save_strategy_exposure(se_rows)
        meta_db.save_strategy_risk_daily(sr_rows)

        # 3. Run quality
        if run_meta:
            meta_db.save_run_quality({
                "trade_date": today_date,
                "snapshot_version": _sv,
                "market": "KR",
                "sync_status": run_meta.get("sync_status", "?"),
                "synced_count": run_meta.get("sync_completeness"),
                "failed_count": run_meta.get("sync_failed_count"),
                "expected_count": None,
                "completeness_ratio": run_meta.get("sync_completeness"),
                "selected_source": run_meta.get("selected_source"),
                "csv_last_date": run_meta.get("csv_last_date"),
                "db_last_date": run_meta.get("db_last_date"),
                "data_snapshot_id": data_snapshot_id,
                "degraded_flag": 0,
                "run_id": run_meta.get("matrix_hash", ""),
            })

        # 4. Universe snapshot
        meta_db.save_universe_snapshot({
            "trade_date": today_date,
            "snapshot_version": _sv,
            "universe_count_raw": universe_raw_count or len(universe),
            "universe_count_filtered": universe_filtered_count or len(universe),
            "missing_data_count": missing_data_count,
            "tradable_count": len(universe),
        })

        # 5. Recommendation log (EOD 직후 확실히 저장 — UI 호출 의존 제거)
        try:
            from web.lab_live.meta_summary import build_daily_summary
            build_daily_summary(today_date)  # 내부에서 save_recommendation_log 호출
        except Exception as e:
            logger.warning(f"[META] recommendation_log save failed (non-fatal): {e}")

        # 6. Sanity checks + verification
        _sanity_check(mc, sd_rows)
        verification = meta_db.verify_row_counts(today_date, len(lanes))
        if verification["missing_strategies"]:
            logger.warning(
                f"[META_VERIFY] Missing: {verification['missing_strategies']}"
            )
        else:
            logger.info(
                f"[META_OK] {today_date}: mc=1, sd={verification['strategy_daily']}, "
                f"se={verification['exposure_daily']}, "
                f"rq=1, sr={len(sr_rows)}, usd=1, rec=1"
            )

    except Exception as e:
        logger.error(f"[META] Collection failed: {e}", exc_info=True)


# ── Market Context ────────────────────────────────────────────

def _compute_market_context(
    today_date: str,
    today_idx: int,
    close: pd.DataFrame,
    high: pd.DataFrame,
    vol: pd.DataFrame,
    universe: FrozenSet[str],
    sector_map: dict,
    index_series: pd.Series,
    data_snapshot_id: str,
) -> dict:
    """Compute market-level features from the same EOD snapshot."""
    univ_codes = [c for c in universe if c in close.columns]

    # kospi_return
    kospi_return = None
    if today_idx >= 1 and len(index_series) > today_idx:
        prev = float(index_series.iloc[today_idx - 1])
        curr = float(index_series.iloc[today_idx])
        if prev > 0:
            kospi_return = round((curr - prev) / prev, 6)

    # adv_ratio: universe 내 상승 종목 비율
    adv_ratio = None
    if today_idx >= 1 and univ_codes:
        prev_close = close[univ_codes].iloc[today_idx - 1]
        curr_close = close[univ_codes].iloc[today_idx]
        valid = (prev_close > 0) & (curr_close > 0)
        if valid.sum() > 0:
            returns = (curr_close[valid] - prev_close[valid]) / prev_close[valid]
            adv_ratio = round(float((returns > 0).sum()) / len(returns), 4)

    # small_vs_large: 시총 하위50% 평균수익 - 상위50% 평균수익
    # close 기반 proxy: 주가 수준으로 대체 (fundamental 없을 때)
    small_vs_large = None
    if today_idx >= 1 and univ_codes:
        prev_close = close[univ_codes].iloc[today_idx - 1]
        curr_close = close[univ_codes].iloc[today_idx]
        valid = (prev_close > 0) & (curr_close > 0)
        if valid.sum() > 10:
            returns = (curr_close[valid] - prev_close[valid]) / prev_close[valid]
            prices = prev_close[valid]
            median_price = prices.median()
            small_ret = returns[prices <= median_price].mean()
            large_ret = returns[prices > median_price].mean()
            if not (np.isnan(small_ret) or np.isnan(large_ret)):
                small_vs_large = round(float(small_ret - large_ret), 6)

    # sector_dispersion: 섹터별 평균 수익률의 표준편차
    sector_dispersion = None
    if today_idx >= 1 and univ_codes and sector_map:
        prev_close = close[univ_codes].iloc[today_idx - 1]
        curr_close = close[univ_codes].iloc[today_idx]
        valid = (prev_close > 0) & (curr_close > 0)
        if valid.sum() > 0:
            returns = (curr_close[valid] - prev_close[valid]) / prev_close[valid]
            sector_returns = defaultdict(list)
            for code in returns.index:
                info = sector_map.get(code, {})
                sector = info.get("sector", "기타") if isinstance(info, dict) else "기타"
                sector_returns[sector].append(float(returns[code]))
            if len(sector_returns) >= 3:
                sector_avgs = [np.mean(v) for v in sector_returns.values() if len(v) >= 2]
                if len(sector_avgs) >= 3:
                    sector_dispersion = round(float(np.std(sector_avgs)), 6)

    # breakout_ratio: 60일 신고가 돌파 비율
    breakout_ratio = None
    if today_idx >= 60 and univ_codes:
        window = close[univ_codes].iloc[today_idx - 60:today_idx]  # 과거 60일 (오늘 제외)
        high_60 = window.max()
        today_close = close[univ_codes].iloc[today_idx]
        valid = (high_60 > 0) & (today_close > 0)
        if valid.sum() > 0:
            breakouts = (today_close[valid] >= high_60[valid]).sum()
            breakout_ratio = round(float(breakouts) / valid.sum(), 4)

    # regime: from file (best effort, NULL if missing)
    regime_score = None
    regime_label = None
    regime_path = Path(__file__).resolve().parent.parent.parent / "data" / "regime" / "latest.json"
    if regime_path.exists():
        try:
            regime_data = json.loads(regime_path.read_text(encoding="utf-8"))
            if regime_data.get("feature_date") == today_date:
                regime_score = regime_data.get("composite_score")
                regime_label = regime_data.get("predicted_label")
        except Exception:
            pass

    return {
        "trade_date": today_date,
        "kospi_return": kospi_return,
        "adv_ratio": adv_ratio,
        "small_vs_large": small_vs_large,
        "sector_dispersion": sector_dispersion,
        "breakout_ratio": breakout_ratio,
        "regime_score": regime_score,
        "regime_label": regime_label,
        "data_snapshot_id": data_snapshot_id,
    }


# ── Strategy Daily ────────────────────────────────────────────

def _compute_strategy_daily(
    today_date: str,
    today_idx: int,
    sname: str,
    lane: Any,
    close: pd.DataFrame,
    new_trades: list,
    config: Any,
) -> dict:
    """Compute daily performance metrics for one strategy."""
    from lab.lab_config import STRATEGY_CONFIGS

    scfg = STRATEGY_CONFIGS.get(sname)
    if not scfg or not hasattr(scfg, 'version'):
        raise ValueError(f"[META] Missing version for strategy: {sname}")
    version = f"{sname}_{scfg.version}"

    # Current equity
    equity = lane.cash
    pos_value = 0.0
    for tk, pos in lane.positions.items():
        pos_value += pos.qty * pos.current_price
    equity += pos_value

    # Previous equity
    initial_cash = float(config.initial_cash)
    if len(lane.equity_history) >= 2:
        prev_equity = lane.equity_history[-2]["equity"]
    else:
        prev_equity = initial_cash

    # daily_return
    daily_return = None
    if prev_equity > 0:
        daily_return = round((equity - prev_equity) / prev_equity, 6)

    # ── Data quality gate (KR/US 동일 기준: _OUTLIER_THRESHOLD=0.5) ──
    _OUTLIER_THRESHOLD = 0.5
    if daily_return is not None and abs(daily_return) > _OUTLIER_THRESHOLD:
        logger.warning(
            f"[META_RETURN_DEBUG] {today_date} {sname}: OUTLIER daily_return={daily_return} "
            f"equity={equity:.2f} prev={prev_equity:.2f} → set None"
        )
        daily_return = None

    # Snapshot mismatch: pos_value==0 이면 gross_exposure==0 (일반적 동치,
    # 초기 운영 구간에서 DQ_EQUIV 로그로 검증)
    if len(lane.positions) > 0 and pos_value < 1e-8:
        logger.warning(
            f"[META_RETURN_DEBUG] {today_date} {sname}: SNAPSHOT_MISMATCH "
            f"position_count={len(lane.positions)} pos_value={pos_value:.2f}"
        )

    # cumul_return
    cumul_return = None
    if initial_cash > 0:
        cumul_return = round((equity - initial_cash) / initial_cash, 6)

    # win/loss counts: 종목별 당일 가격 변동 기준
    win_count = 0
    loss_count = 0
    if today_idx >= 1:
        for tk, pos in lane.positions.items():
            if tk not in close.columns:
                continue
            prev_c = float(close[tk].iloc[today_idx - 1])
            curr_c = float(close[tk].iloc[today_idx])
            if prev_c <= 0 or curr_c <= 0:
                continue
            if curr_c > prev_c:
                win_count += 1
            elif curr_c < prev_c:
                loss_count += 1

    # turnover: BUY + SELL 매매 금액 / prev_equity
    turnover = None
    if prev_equity > 0:
        strategy_trades = [t for t in new_trades if t.get("strategy") == sname]
        trade_amount = sum(
            abs(t.get("qty", 0) * t.get("exit_price", 0))
            for t in strategy_trades
        )
        # pending fills (오늘 체결된 매수 = 어제 pending → 오늘 open fill)
        # pending fill 금액은 lane.positions에서 entry_date == today인 종목
        for tk, pos in lane.positions.items():
            if pos.entry_date == today_date:
                trade_amount += pos.qty * pos.entry_price
        turnover = round(trade_amount / prev_equity, 6)

    # cash_ratio, gross_exposure
    cash_ratio = round(lane.cash / equity, 4) if equity > 0 else None
    gross_exposure = round(pos_value / equity, 4) if equity > 0 else None

    # DQ_EQUIV: pos_value==0 ↔ gross_exposure==0 동치 검증 로그
    pv_zero = abs(pos_value) < 1e-8
    ge_zero = gross_exposure is None or abs(gross_exposure) < 1e-8
    if pv_zero != ge_zero:
        logger.warning(
            f"[META_DQ_EQUIV] {today_date} {sname}: pv_zero={pv_zero} ge_zero={ge_zero} "
            f"pos_value={pos_value:.4f} gross_exposure={gross_exposure} equity={equity:.2f}"
        )

    logger.debug(
        f"[META_RETURN_DEBUG] {today_date} {sname}: dr={daily_return} cr={cumul_return} "
        f"pc={len(lane.positions)} pv={pos_value:.2f} ge={gross_exposure} "
        f"cash={lane.cash:.2f} equity={equity:.2f}"
    )

    return {
        "trade_date": today_date,
        "strategy": sname,
        "strategy_version": version,
        "daily_return": daily_return,
        "cumul_return": cumul_return,
        "position_count": len(lane.positions),
        "win_count": win_count,
        "loss_count": loss_count,
        "turnover": turnover,
        "cash_ratio": cash_ratio,
        "gross_exposure": gross_exposure,
    }


# ── Strategy Exposure ────────────────────────────────────────

def _compute_strategy_exposure(
    today_date: str,
    sname: str,
    lane: Any,
    close: pd.DataFrame,
    today_idx: int,
    sector_map: dict,
    fundamental: Optional[pd.DataFrame],
) -> dict:
    """Compute exposure/concentration metrics for one strategy."""
    equity = lane.cash
    weights = {}
    for tk, pos in lane.positions.items():
        val = pos.qty * pos.current_price
        equity += val  # re-sum to avoid float drift from caller
        weights[tk] = val

    # Normalize to portfolio weight
    if equity > 0:
        weights = {k: v / equity for k, v in weights.items()}

    # top1_weight, top5_weight
    sorted_w = sorted(weights.values(), reverse=True)
    top1_weight = round(sorted_w[0], 4) if sorted_w else None
    top5_weight = round(sum(sorted_w[:5]), 4) if len(sorted_w) >= 5 else (
        round(sum(sorted_w), 4) if sorted_w else None
    )

    # sector concentration
    sector_top1 = None
    sector_top1_weight = None
    sector_disp = None
    if weights and sector_map:
        sector_weights = defaultdict(float)
        for code, w in weights.items():
            info = sector_map.get(code, {})
            sector = info.get("sector", "기타") if isinstance(info, dict) else "기타"
            sector_weights[sector] += w
        if sector_weights:
            top_sector = max(sector_weights, key=sector_weights.get)
            sector_top1 = top_sector
            sector_top1_weight = round(sector_weights[top_sector], 4)
            if len(sector_weights) >= 2:
                sector_disp = round(float(np.std(list(sector_weights.values()))), 6)

    # avg_market_cap: from fundamental data
    avg_market_cap = None
    if fundamental is not None and not fundamental.empty and "market_cap" in fundamental.columns:
        codes_held = list(weights.keys())
        if codes_held:
            fund_indexed = fundamental.set_index("code") if "code" in fundamental.columns else fundamental
            caps = []
            for code in codes_held:
                if code in fund_indexed.index:
                    mc = fund_indexed.loc[code, "market_cap"]
                    if isinstance(mc, pd.Series):
                        mc = mc.iloc[0]
                    if pd.notna(mc) and mc > 0:
                        caps.append(float(mc))
            if caps:
                avg_market_cap = round(np.mean(caps), 0)

    return {
        "trade_date": today_date,
        "strategy": sname,
        "avg_market_cap": avg_market_cap,
        "top1_weight": top1_weight,
        "top5_weight": top5_weight,
        "sector_top1": sector_top1,
        "sector_top1_weight": sector_top1_weight,
        "sector_dispersion": sector_disp,
    }


# ── Strategy Risk ────────────────────────────────────────────

def _compute_strategy_risk(
    today_date: str,
    sname: str,
    lane: Any,
    config: Any,
    snapshot_version: str = "",
) -> dict:
    """Compute rolling risk metrics from equity_history. NULL허용: 데이터 부족 시."""
    eh = lane.equity_history  # list of {"equity": float, "date": str} or similar
    initial_cash = float(config.initial_cash)

    # Extract equity series
    equities = []
    for e in eh:
        if isinstance(e, dict):
            equities.append(e.get("equity", 0))
        elif isinstance(e, (list, tuple)):
            equities.append(e[1] if len(e) > 1 else 0)
        else:
            equities.append(float(e) if e else 0)

    n = len(equities)
    result = {
        "trade_date": today_date,
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
        "cost_bps_est": round((config.buy_cost + config.sell_cost) * 10000, 1),
    }

    if n < 2:
        return result

    # daily returns
    returns = []
    for i in range(1, n):
        if equities[i - 1] > 0:
            returns.append((equities[i] - equities[i - 1]) / equities[i - 1])
        else:
            returns.append(0)

    # rolling_5d_return
    if len(returns) >= 5:
        r5 = returns[-5:]
        cum5 = 1
        for r in r5:
            cum5 *= (1 + r)
        result["rolling_5d_return"] = round(cum5 - 1, 6)

    # rolling_20d_return + mdd + vol + hit_rate
    if len(returns) >= 20:
        r20 = returns[-20:]
        cum20 = 1
        for r in r20:
            cum20 *= (1 + r)
        result["rolling_20d_return"] = round(cum20 - 1, 6)

        # MDD over 20d
        peak = equities[-21] if len(equities) > 20 else equities[0]
        max_dd = 0
        for eq in equities[-20:]:
            if eq > peak:
                peak = eq
            dd = (eq - peak) / peak if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd
        result["rolling_20d_mdd"] = round(max_dd, 6)

        # realized vol
        result["realized_vol_20d"] = round(float(np.std(r20)) * (252 ** 0.5), 6)

        # hit rate
        wins = sum(1 for r in r20 if r > 0)
        result["hit_rate_20d"] = round(wins / 20, 4)
    elif len(returns) >= 5:
        # partial: vol/hit_rate with available data
        result["realized_vol_20d"] = round(float(np.std(returns)) * (252 ** 0.5), 6)
        wins = sum(1 for r in returns if r > 0)
        result["hit_rate_20d"] = round(wins / len(returns), 4)

    # daily_mdd (today only)
    if n >= 2:
        result["daily_mdd"] = round(min(0, returns[-1]), 6)

    # avg_hold_days: from current positions
    hold_days = []
    for tk, pos in lane.positions.items():
        if hasattr(pos, 'entry_date') and pos.entry_date:
            try:
                entry = pd.Timestamp(pos.entry_date)
                hold = (pd.Timestamp(today_date) - entry).days
                hold_days.append(hold)
            except Exception:
                pass
    if hold_days:
        result["avg_hold_days"] = round(np.mean(hold_days), 1)

    return result


# ── Sanity Checks ─────────────────────────────────────────────

def _sanity_check(mc: dict, sd_rows: list) -> None:
    """Log warnings for out-of-range values."""
    adv = mc.get("adv_ratio")
    if adv is not None and (adv < 0 or adv > 1):
        logger.warning(f"[META_SANITY] adv_ratio={adv} out of [0,1]")

    sd = mc.get("sector_dispersion")
    if sd is not None and sd > 0.5:
        logger.warning(f"[META_SANITY] sector_dispersion={sd} unusually high")

    br = mc.get("breakout_ratio")
    if br is not None and br > 0.3:
        logger.warning(f"[META_SANITY] breakout_ratio={br} > 30%")

    for row in sd_rows:
        t = row.get("turnover")
        if t is not None and t > 2.0:
            logger.warning(
                f"[META_SANITY] {row['strategy']} turnover={t} > 200%"
            )
