# -*- coding: utf-8 -*-
"""
lab_simulator.py -- Lab Swing Simulation Engine
=================================================
Virtual portfolio simulation with 3 strategies on ranking stocks.
NO real orders -- simulation only.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen4.rest.lab")

# ── Default Parameters ────────────────────────────────────────

DEFAULT_PARAMS: Dict[str, Any] = {
    "ranking_source": "실시간순위",    # 1 (ka00198 키움빅데이터, 장중)
    # "등락률"=ka10027(장후도 가능), "과거CSV"=swing/ranking/*.csv(백테스트)
    "top_n": 20,                      # 2
    "entry_threshold": 3.0,           # 3  (%)
    "exit_target_a": 1.0,             # 4  (%)
    "stop_loss_a": -0.5,              # 5  (%)
    "exit_target_b": 2.0,             # 6  (%)
    "stop_loss_b": -1.0,              # 7  (%)
    "exit_target_c": 1.5,             # 8  (%)
    "trail_max_c": 6.0,               # 9  (%)
    "max_positions": 5,               # 10
    "position_size_pct": 20.0,        # 11 (%)
    "price_min": 5000,                # 12
}

PARAM_RANGES: Dict[str, Dict[str, Any]] = {
    "ranking_source":   {"type": "select", "options": ["실시간순위", "등락률", "거래량", "거래대금", "과거CSV"]},
    "top_n":            {"type": "range", "min": 5, "max": 50, "step": 1},
    "entry_threshold":  {"type": "range", "min": 1.0, "max": 10.0, "step": 0.5, "unit": "%"},
    "exit_target_a":    {"type": "range", "min": 0.5, "max": 5.0, "step": 0.1, "unit": "%"},
    "stop_loss_a":      {"type": "range", "min": -3.0, "max": -0.3, "step": 0.1, "unit": "%"},
    "exit_target_b":    {"type": "range", "min": 1.0, "max": 10.0, "step": 0.5, "unit": "%"},
    "stop_loss_b":      {"type": "range", "min": -5.0, "max": -0.5, "step": 0.1, "unit": "%"},
    "exit_target_c":    {"type": "range", "min": 1.0, "max": 10.0, "step": 0.5, "unit": "%"},
    "trail_max_c":      {"type": "range", "min": 3.0, "max": 10.0, "step": 0.5, "unit": "%"},
    "max_positions":    {"type": "range", "min": 1, "max": 20, "step": 1},
    "position_size_pct": {"type": "range", "min": 5.0, "max": 50.0, "step": 5.0, "unit": "%"},
    "price_min":        {"type": "range", "min": 1000, "max": 50000, "step": 1000, "unit": "원"},
}

INITIAL_CASH = 10_000_000  # 1천만원


# ── Ranking Fetcher ───────────────────────────────────────────

# Kiwoom REST API — ka10027/ka10030/ka10032 deprecated (1504 error).
# All ranking via ka00198 + qry_tp: 1=실시간, 2=거래량, 3=거래대금, 5=등락률
_RANKING_API_MAP = {
    "실시간순위": ("ka00198", "/api/dostk/stkinfo", "1"),
    "등락률":    ("ka00198", "/api/dostk/stkinfo", "5"),
    "거래량":    ("ka00198", "/api/dostk/stkinfo", "2"),
    "거래대금":  ("ka00198", "/api/dostk/stkinfo", "3"),
}


def fetch_ranking(provider, source: str = "등락률", top_n: int = 20) -> List[Dict]:
    """
    Fetch ranking stocks from Kiwoom REST API.

    Returns list of dicts:
        [{"code": "005930", "name": "삼성전자", "price": 72000,
          "change_pct": 3.5, "volume": 12345678, "rank": 1}, ...]
    """
    entry = _RANKING_API_MAP.get(source, _RANKING_API_MAP["실시간순위"])
    api_id, path, qry_tp = entry

    body = {"qry_tp": qry_tp}

    try:
        resp = provider._request(api_id, path, body, related_code="LAB")
    except Exception as e:
        logger.error(f"[LAB] Ranking fetch failed: {e}")
        return _fallback_ranking(top_n)

    if not resp or resp.get("return_code") not in (0, None):
        logger.warning(f"[LAB] Ranking API returned: {resp.get('return_msg', 'unknown')}")
        return _fallback_ranking(top_n)

    output = resp.get("item_inq_rank", resp.get("output", []))
    if not output:
        return _fallback_ranking(top_n)

    results = []
    for i, item in enumerate(output[:top_n]):
        try:
            code = str(item.get("stk_cd", "")).strip()
            name = item.get("stk_nm", "").strip()
            price = abs(int(
                str(item.get("past_curr_prc", "0")).replace(",", "").replace("+", "") or "0"
            ))
            change_pct = float(item.get("base_comp_chgr", "0") or "0")
            volume = 0  # ka00198 doesn't return volume

            if name and price > 0:
                results.append({
                    "code": code.zfill(6) if code else "",
                    "name": name,
                    "price": price,
                    "change_pct": round(change_pct, 2),
                    "volume": volume,
                    "rank": i + 1,
                })
        except (ValueError, TypeError) as e:
            logger.debug(f"[LAB] Ranking parse skip: {e}")
            continue

    return results[:top_n]


def load_csv_ranking(date_str: str = "", top_n: int = 20) -> List[Dict]:
    """Load ranking from kr-legacy/data/swing/ranking/*.csv (과거 백테스트용)."""
    import csv
    from pathlib import Path

    ranking_dir = Path(__file__).resolve().parent.parent.parent / "kr-legacy" / "data" / "swing" / "ranking"

    if not date_str:
        # Find latest CSV
        csvs = sorted(ranking_dir.glob("*.csv"), reverse=True)
        if not csvs:
            return _fallback_ranking(top_n)
        csv_file = csvs[0]
        date_str = csv_file.stem
    else:
        csv_file = ranking_dir / f"{date_str}.csv"

    if not csv_file.exists():
        logger.warning(f"[LAB] CSV not found: {csv_file}")
        return _fallback_ranking(top_n)

    # Read last snapshot (most recent time)
    rows = []
    try:
        with open(csv_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except Exception as e:
        logger.error(f"[LAB] CSV read error: {e}")
        return _fallback_ranking(top_n)

    if not rows:
        return _fallback_ranking(top_n)

    # Get latest snapshot_time
    last_time = rows[-1].get("snapshot_time", "")
    latest = [r for r in rows if r.get("snapshot_time") == last_time]

    results = []
    for r in latest[:top_n]:
        try:
            results.append({
                "code": r.get("code", "").zfill(6),
                "name": r.get("name", ""),
                "price": int(float(r.get("price", 0))),
                "change_pct": round(float(r.get("change_pct", 0)), 2),
                "volume": 0,
                "rank": int(r.get("rank", 0)),
            })
        except (ValueError, TypeError):
            continue

    logger.info(f"[LAB] CSV loaded: {csv_file.name} time={last_time} count={len(results)}")
    return results


def available_csv_dates() -> List[str]:
    """Return list of available CSV dates for Lab selector."""
    from pathlib import Path
    ranking_dir = Path(__file__).resolve().parent.parent.parent / "kr-legacy" / "data" / "swing" / "ranking"
    if not ranking_dir.exists():
        return []
    return sorted([f.stem for f in ranking_dir.glob("*.csv")], reverse=True)


def _fallback_ranking(top_n: int) -> List[Dict]:
    """Generate demo ranking data when API is unavailable."""
    import random
    demo_stocks = [
        ("005930", "삼성전자", 72000), ("000660", "SK하이닉스", 185000),
        ("035420", "NAVER", 210000), ("005380", "현대차", 248000),
        ("006400", "삼성SDI", 385000), ("051910", "LG화학", 320000),
        ("035720", "카카오", 42000), ("028260", "삼성물산", 135000),
        ("003670", "포스코퓨처엠", 210000), ("012330", "현대모비스", 225000),
        ("066570", "LG전자", 95000), ("055550", "신한지주", 52000),
        ("017670", "SK텔레콤", 58000), ("105560", "KB금융", 82000),
        ("096770", "SK이노베이션", 105000), ("032830", "삼성생명", 85000),
        ("034730", "SK", 175000), ("003550", "LG", 78000),
        ("015760", "한국전력", 22000), ("009150", "삼성전기", 145000),
    ]
    results = []
    for i, (code, name, base_price) in enumerate(demo_stocks[:top_n]):
        pct = round(random.uniform(1.0, 8.0), 2)
        price = int(base_price * (1 + pct / 100))
        results.append({
            "code": code, "name": name, "price": price,
            "change_pct": pct, "volume": random.randint(100000, 5000000),
            "rank": i + 1,
        })
    results.sort(key=lambda x: x["change_pct"], reverse=True)
    for i, r in enumerate(results):
        r["rank"] = i + 1
    return results


# ── Strategy Simulation ──────────────────────────────────────

@dataclass
class VirtualTrade:
    code: str
    name: str
    strategy: str
    side: str            # "BUY" or "SELL"
    price: int
    qty: int
    pnl: float = 0.0
    pnl_pct: float = 0.0
    reason: str = ""     # "TP", "SL", "TRAIL", "TIME", "HOLD"
    timestamp: str = ""


@dataclass
class VirtualPosition:
    code: str
    name: str
    entry_price: int
    qty: int
    current_price: int = 0
    high_price: int = 0   # for trailing stop
    days_held: int = 0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0


@dataclass
class StrategyResult:
    name: str
    label: str
    trades: List[Dict] = field(default_factory=list)
    positions: List[Dict] = field(default_factory=list)
    total_pnl: float = 0.0
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0
    cash: float = INITIAL_CASH
    total_value: float = INITIAL_CASH


def _simulate_strategy_a(
    ranking: List[Dict], params: Dict[str, Any]
) -> StrategyResult:
    """Strategy A: Conservative -- TP/SL with 1-day max hold."""
    result = StrategyResult(name="A", label="Conservative")
    tp = params.get("exit_target_a", 1.0) / 100
    sl = params.get("stop_loss_a", -0.5) / 100
    max_pos = params.get("max_positions", 5)
    size_pct = params.get("position_size_pct", 20.0) / 100
    price_min = params.get("price_min", 5000)
    entry_thresh = params.get("entry_threshold", 3.0)

    cash = float(INITIAL_CASH)
    positions: List[VirtualPosition] = []
    trades: List[VirtualTrade] = []

    # Entry: buy top stocks that pass filter
    eligible = [s for s in ranking if s["change_pct"] >= entry_thresh and s["price"] >= price_min]

    for stock in eligible[:max_pos]:
        alloc = INITIAL_CASH * size_pct
        if cash < alloc * 0.5:
            break
        buy_amount = min(alloc, cash)
        qty = int(buy_amount / stock["price"])
        if qty <= 0:
            continue
        cost = qty * stock["price"]
        cash -= cost
        positions.append(VirtualPosition(
            code=stock["code"], name=stock["name"],
            entry_price=stock["price"], qty=qty,
            current_price=stock["price"], high_price=stock["price"],
        ))
        trades.append(VirtualTrade(
            code=stock["code"], name=stock["name"], strategy="A",
            side="BUY", price=stock["price"], qty=qty,
            timestamp=datetime.now().strftime("%H:%M:%S"),
        ))

    # Simulate exit: use current price vs entry for TP/SL check
    closed_positions = []
    for pos in positions:
        # Simulate small price movement based on ranking momentum
        stock_data = next((s for s in ranking if s["code"] == pos.code), None)
        if stock_data:
            # Use current change as proxy for intraday movement
            change = stock_data["change_pct"] / 100
            # Simulated exit price: entry * (1 + partial change)
            import random
            sim_change = random.uniform(-abs(sl), change * 0.8)
            exit_price = int(pos.entry_price * (1 + sim_change))
        else:
            exit_price = pos.current_price

        pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
        reason = "HOLD"

        if pnl_pct >= tp:
            reason = "TP"
            exit_price = int(pos.entry_price * (1 + tp))
            pnl_pct = tp
        elif pnl_pct <= sl:
            reason = "SL"
            exit_price = int(pos.entry_price * (1 + sl))
            pnl_pct = sl
        else:
            reason = "TIME"  # max 1 day hold

        pnl = (exit_price - pos.entry_price) * pos.qty
        cash += exit_price * pos.qty

        trades.append(VirtualTrade(
            code=pos.code, name=pos.name, strategy="A",
            side="SELL", price=exit_price, qty=pos.qty,
            pnl=pnl, pnl_pct=round(pnl_pct * 100, 2), reason=reason,
            timestamp=datetime.now().strftime("%H:%M:%S"),
        ))
        closed_positions.append((pnl, pnl_pct))

    wins = sum(1 for p, _ in closed_positions if p > 0)
    losses = sum(1 for p, _ in closed_positions if p <= 0)
    total_pnl = sum(p for p, _ in closed_positions)

    result.trades = [asdict(t) for t in trades]
    result.total_pnl = round(total_pnl)
    result.win_count = wins
    result.loss_count = losses
    result.win_rate = round(wins / max(wins + losses, 1) * 100, 1)
    result.cash = round(cash)
    result.total_value = round(cash)
    return result


def _simulate_strategy_b(
    ranking: List[Dict], params: Dict[str, Any]
) -> StrategyResult:
    """Strategy B: Aggressive -- wider TP/SL, more positions, 3-day hold."""
    result = StrategyResult(name="B", label="Aggressive")
    tp = params.get("exit_target_b", 2.0) / 100
    sl = params.get("stop_loss_b", -1.0) / 100
    max_pos = min(params.get("max_positions", 5) * 2, 20)  # double positions
    size_pct = params.get("position_size_pct", 20.0) / 100 * 0.5  # half size each
    price_min = params.get("price_min", 5000)
    entry_thresh = params.get("entry_threshold", 3.0) * 0.7  # lower threshold

    cash = float(INITIAL_CASH)
    positions: List[VirtualPosition] = []
    trades: List[VirtualTrade] = []

    eligible = [s for s in ranking if s["change_pct"] >= entry_thresh and s["price"] >= price_min]

    for stock in eligible[:max_pos]:
        alloc = INITIAL_CASH * size_pct
        if cash < alloc * 0.3:
            break
        buy_amount = min(alloc, cash)
        qty = int(buy_amount / stock["price"])
        if qty <= 0:
            continue
        cost = qty * stock["price"]
        cash -= cost
        positions.append(VirtualPosition(
            code=stock["code"], name=stock["name"],
            entry_price=stock["price"], qty=qty,
            current_price=stock["price"], high_price=stock["price"],
        ))
        trades.append(VirtualTrade(
            code=stock["code"], name=stock["name"], strategy="B",
            side="BUY", price=stock["price"], qty=qty,
            timestamp=datetime.now().strftime("%H:%M:%S"),
        ))

    closed_positions = []
    for pos in positions:
        stock_data = next((s for s in ranking if s["code"] == pos.code), None)
        if stock_data:
            import random
            change = stock_data["change_pct"] / 100
            # 3-day hold: larger range of outcomes
            sim_change = random.uniform(sl * 0.8, change * 1.2)
            exit_price = int(pos.entry_price * (1 + sim_change))
        else:
            exit_price = pos.current_price

        pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
        reason = "HOLD"

        if pnl_pct >= tp:
            reason = "TP"
            exit_price = int(pos.entry_price * (1 + tp))
            pnl_pct = tp
        elif pnl_pct <= sl:
            reason = "SL"
            exit_price = int(pos.entry_price * (1 + sl))
            pnl_pct = sl
        else:
            reason = "TIME"

        pnl = (exit_price - pos.entry_price) * pos.qty
        cash += exit_price * pos.qty

        trades.append(VirtualTrade(
            code=pos.code, name=pos.name, strategy="B",
            side="SELL", price=exit_price, qty=pos.qty,
            pnl=pnl, pnl_pct=round(pnl_pct * 100, 2), reason=reason,
            timestamp=datetime.now().strftime("%H:%M:%S"),
        ))
        closed_positions.append((pnl, pnl_pct))

    wins = sum(1 for p, _ in closed_positions if p > 0)
    losses = sum(1 for p, _ in closed_positions if p <= 0)
    total_pnl = sum(p for p, _ in closed_positions)

    result.trades = [asdict(t) for t in trades]
    result.total_pnl = round(total_pnl)
    result.win_count = wins
    result.loss_count = losses
    result.win_rate = round(wins / max(wins + losses, 1) * 100, 1)
    result.cash = round(cash)
    result.total_value = round(cash)
    return result


def _simulate_strategy_c(
    ranking: List[Dict], params: Dict[str, Any]
) -> StrategyResult:
    """Strategy C: Dynamic -- trailing stop that widens, no time limit."""
    result = StrategyResult(name="C", label="Dynamic")
    initial_tp = params.get("exit_target_c", 1.5) / 100
    trail_max = params.get("trail_max_c", 6.0) / 100
    max_pos = params.get("max_positions", 5)
    size_pct = params.get("position_size_pct", 20.0) / 100
    price_min = params.get("price_min", 5000)
    entry_thresh = params.get("entry_threshold", 3.0)

    cash = float(INITIAL_CASH)
    positions: List[VirtualPosition] = []
    trades: List[VirtualTrade] = []

    eligible = [s for s in ranking if s["change_pct"] >= entry_thresh and s["price"] >= price_min]

    for stock in eligible[:max_pos]:
        alloc = INITIAL_CASH * size_pct
        if cash < alloc * 0.5:
            break
        buy_amount = min(alloc, cash)
        qty = int(buy_amount / stock["price"])
        if qty <= 0:
            continue
        cost = qty * stock["price"]
        cash -= cost
        positions.append(VirtualPosition(
            code=stock["code"], name=stock["name"],
            entry_price=stock["price"], qty=qty,
            current_price=stock["price"], high_price=stock["price"],
        ))
        trades.append(VirtualTrade(
            code=stock["code"], name=stock["name"], strategy="C",
            side="BUY", price=stock["price"], qty=qty,
            timestamp=datetime.now().strftime("%H:%M:%S"),
        ))

    closed_positions = []
    open_positions = []
    for pos in positions:
        stock_data = next((s for s in ranking if s["code"] == pos.code), None)
        if stock_data:
            import random
            change = stock_data["change_pct"] / 100
            # Simulate intraday high and current
            sim_high = change * random.uniform(0.8, 1.5)
            sim_current = sim_high * random.uniform(0.6, 1.0)
            high_price = int(pos.entry_price * (1 + sim_high))
            current_price = int(pos.entry_price * (1 + sim_current))
        else:
            high_price = pos.entry_price
            current_price = pos.entry_price

        gain_from_entry = (high_price - pos.entry_price) / pos.entry_price
        # Trailing stop widens as gain increases: initial_tp to trail_max
        trail_pct = min(initial_tp + gain_from_entry * 0.5, trail_max)
        trail_stop_price = int(high_price * (1 - trail_pct))

        current_pnl_pct = (current_price - pos.entry_price) / pos.entry_price

        if current_price <= trail_stop_price and gain_from_entry > initial_tp:
            # Trailing stop hit
            exit_price = trail_stop_price
            pnl_pct = (exit_price - pos.entry_price) / pos.entry_price
            pnl = (exit_price - pos.entry_price) * pos.qty
            cash += exit_price * pos.qty
            reason = "TRAIL"

            trades.append(VirtualTrade(
                code=pos.code, name=pos.name, strategy="C",
                side="SELL", price=exit_price, qty=pos.qty,
                pnl=pnl, pnl_pct=round(pnl_pct * 100, 2), reason=reason,
                timestamp=datetime.now().strftime("%H:%M:%S"),
            ))
            closed_positions.append((pnl, pnl_pct))
        elif current_pnl_pct >= initial_tp:
            # Take initial profit
            exit_price = int(pos.entry_price * (1 + initial_tp))
            pnl_pct = initial_tp
            pnl = (exit_price - pos.entry_price) * pos.qty
            cash += exit_price * pos.qty
            reason = "TP"

            trades.append(VirtualTrade(
                code=pos.code, name=pos.name, strategy="C",
                side="SELL", price=exit_price, qty=pos.qty,
                pnl=pnl, pnl_pct=round(pnl_pct * 100, 2), reason=reason,
                timestamp=datetime.now().strftime("%H:%M:%S"),
            ))
            closed_positions.append((pnl, pnl_pct))
        else:
            # Still holding
            pos.current_price = current_price
            pos.high_price = high_price
            pos.unrealized_pnl = (current_price - pos.entry_price) * pos.qty
            pos.unrealized_pnl_pct = round(current_pnl_pct * 100, 2)
            open_positions.append(pos)

    wins = sum(1 for p, _ in closed_positions if p > 0)
    losses = sum(1 for p, _ in closed_positions if p <= 0)
    total_pnl = sum(p for p, _ in closed_positions)
    # Add unrealized P&L
    unrealized = sum(p.unrealized_pnl for p in open_positions)
    position_value = sum(p.current_price * p.qty for p in open_positions)

    result.trades = [asdict(t) for t in trades]
    result.positions = [asdict(p) for p in open_positions]
    result.total_pnl = round(total_pnl + unrealized)
    result.win_count = wins
    result.loss_count = losses
    result.win_rate = round(wins / max(wins + losses, 1) * 100, 1)
    result.cash = round(cash)
    result.total_value = round(cash + position_value)
    return result


# ── Main Entry ────────────────────────────────────────────────

def run_simulation(ranking: List[Dict], params: Optional[Dict] = None) -> Dict:
    """
    Run 3 strategies simultaneously on the given ranking data.

    Returns:
        {
            "timestamp": "...",
            "initial_cash": 10000000,
            "ranking_count": 20,
            "strategies": [
                {"name": "A", "label": "Conservative", ...},
                {"name": "B", "label": "Aggressive", ...},
                {"name": "C", "label": "Dynamic", ...},
            ]
        }
    """
    if params is None:
        params = dict(DEFAULT_PARAMS)
    else:
        # Merge with defaults
        merged = dict(DEFAULT_PARAMS)
        merged.update(params)
        params = merged

    # Wall-clock start/stop (Jeff 2026-04-29 — backdata for live
    # trading expansion). Lab simulate is ~instant compared to
    # realtime, but we still capture both so downstream analysis
    # has a uniform schema across modes.
    _start_dt = datetime.now()
    result_a = _simulate_strategy_a(ranking, params)
    result_b = _simulate_strategy_b(ranking, params)
    result_c = _simulate_strategy_c(ranking, params)
    _stop_dt = datetime.now()

    _started_at = _start_dt.strftime("%Y-%m-%d %H:%M:%S")
    _stopped_at = _stop_dt.strftime("%Y-%m-%d %H:%M:%S")
    result = {
        "timestamp": _stopped_at,  # legacy alias for stopped_at
        "started_at": _started_at,
        "stopped_at": _stopped_at,
        "initial_cash": INITIAL_CASH,
        "ranking_count": len(ranking),
        "params": params,
        "mode": "simulate",
        "elapsed_sec": round((_stop_dt - _start_dt).total_seconds(), 3),
        "strategies": [
            asdict(result_a),
            asdict(result_b),
            asdict(result_c),
        ],
    }

    # Auto-save results
    _save_result(result)
    return result


def _save_result(result: Dict) -> None:
    """Save simulation result to JSON + CSV."""
    import csv
    from pathlib import Path

    result_dir = Path(__file__).resolve().parent.parent / "data" / "lab_results"
    result_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 1. JSON (전체 결과)
    json_file = result_dir / f"sim_{ts}.json"
    try:
        import json
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info(f"[LAB_SAVE] JSON: {json_file.name}")
    except Exception as e:
        logger.warning(f"[LAB_SAVE] JSON failed: {e}")

    # 2. CSV (거래내역 누적)
    # Schema (Jeff 2026-04-29): started_at + stopped_at columns appended
    # so per-trade rows carry the run window without joining sim_*.json.
    # Pre-existing rows from older runs leave these two columns blank.
    csv_file = result_dir / "trades_history.csv"
    is_new = not csv_file.exists()
    try:
        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow([
                    "sim_time", "strategy", "rank", "code", "name",
                    "entry_price", "exit_price", "qty", "pnl", "pnl_pct",
                    "exit_reason", "source", "params_hash",
                    "started_at", "stopped_at",
                ])
            params_hash = ts  # simple identifier
            source = result.get("params", {}).get("ranking_source", "")
            started_at = result.get("started_at", "")
            stopped_at = result.get("stopped_at", result.get("timestamp", ""))
            for strat in result.get("strategies", []):
                sname = strat.get("name", "")
                for t in strat.get("trades", []):
                    writer.writerow([
                        result["timestamp"], sname, t.get("rank", ""),
                        t.get("code", ""), t.get("name", ""),
                        t.get("entry_price", 0), t.get("exit_price", 0),
                        t.get("qty", 0), t.get("pnl", 0),
                        t.get("pnl_pct", 0), t.get("exit_reason", ""),
                        source, params_hash,
                        started_at, stopped_at,
                    ])
        logger.info(f"[LAB_SAVE] CSV: {csv_file.name}")
    except Exception as e:
        logger.warning(f"[LAB_SAVE] CSV failed: {e}")
        return  # CSV failure block fall-through; PG attempt below
                # would only repeat the same data on a recovered DB,
                # but we still want JSON to remain authoritative.

    # 3. PG (lab_realtime_runs + lab_realtime_trades — Jeff 2026-04-29)
    # Triple-write secondary. JSON + CSV stay authoritative; PG
    # failures swallow silently (kill-switch: QTRON_LAB_REALTIME_PG=0).
    try:
        from web.lab_realtime_pg import save_result_pg
        save_result_pg(result)
    except Exception as e:
        logger.warning(f"[LAB_SAVE] PG insert failed: {e}")


def get_saved_results(limit: int = 20) -> List[Dict]:
    """Return list of saved simulation results (summary only)."""
    import json
    from pathlib import Path

    result_dir = Path(__file__).resolve().parent.parent / "data" / "lab_results"
    if not result_dir.exists():
        return []

    results = []
    for f in sorted(result_dir.glob("sim_*.json"), reverse=True)[:limit]:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            # Summary only
            best = max(data.get("strategies", []), key=lambda s: s.get("total_pnl", 0), default={})
            results.append({
                "file": f.name,
                "timestamp": data.get("timestamp", ""),
                "source": data.get("params", {}).get("ranking_source", ""),
                "ranking_count": data.get("ranking_count", 0),
                "best_strategy": best.get("name", ""),
                "best_pnl": best.get("total_pnl", 0),
                "best_win_rate": best.get("win_rate", 0),
            })
        except Exception:
            continue
    return results
