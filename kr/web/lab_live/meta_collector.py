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
) -> None:
    """Collect and store all meta metrics for today's EOD."""
    try:
        # 1. Market context
        mc = _compute_market_context(
            today_date, today_idx, close, high, vol,
            universe, sector_map, index_series, data_snapshot_id,
        )
        meta_db.save_market_context(mc)

        # 2. Strategy daily + exposure
        sd_rows = []
        se_rows = []
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

        meta_db.save_strategy_daily(sd_rows)
        meta_db.save_strategy_exposure(se_rows)

        # 3. Sanity checks + verification
        _sanity_check(mc, sd_rows)
        verification = meta_db.verify_row_counts(today_date, len(lanes))
        if verification["missing_strategies"]:
            logger.warning(
                f"[META_VERIFY] Missing: {verification['missing_strategies']}"
            )
        else:
            logger.info(
                f"[META_OK] {today_date}: mc=1, sd={verification['strategy_daily']}, "
                f"se={verification['exposure_daily']}"
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
