"""
web/rebalance_api.py -- Rebalance State Machine + Command API
==============================================================
Cycle-based state machine with idempotent commands.

State Machine:
  IDLE -> WINDOW_OPEN -> PREVIEW_READY -> SELL_RUNNING -> SELL_COMPLETE
  -> BUY_READY -> BUY_RUNNING -> BUY_COMPLETE -> IDLE (cycle reset)
  Any state -> SKIPPED -> IDLE
  Any state -> BLOCKED (permission denied)

Rules:
  - BUY_COMPLETE is the ONLY cycle reset trigger
  - Preview creates a snapshot hash; SELL/BUY must match it
  - Same request_id = idempotent (returns previous result)
  - Concurrent execution blocked by lock + phase check
  - cycle_state persisted to state_manager (survives restart)
"""
from __future__ import annotations

import enum
import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("gen4.rest")

WINDOW_ADVANCE_DAYS = 5
PENDING_BUYS_WARN_DAYS = 7

# ── Rebalance Phase (state machine) ──────────────────────────

class RebalPhase(enum.Enum):
    IDLE = "IDLE"
    WINDOW_OPEN = "WINDOW_OPEN"
    PREVIEW_READY = "PREVIEW_READY"
    SELL_RUNNING = "SELL_RUNNING"
    SELL_COMPLETE = "SELL_COMPLETE"
    BUY_READY = "BUY_READY"
    BUY_RUNNING = "BUY_RUNNING"
    BUY_COMPLETE = "BUY_COMPLETE"
    SKIPPED = "SKIPPED"
    BLOCKED = "BLOCKED"


@dataclass
class RebalCycleState:
    """Persistent rebalance cycle state."""
    cycle_id: str = ""
    phase: str = "IDLE"
    preview_batch_id: str = ""
    preview_snapshot_ts: float = 0.0
    preview_hash: str = ""
    preview_sells: List[Dict] = field(default_factory=list)
    preview_buys: List[Dict] = field(default_factory=list)
    sell_status: str = ""
    buy_status: str = ""
    sell_executed_at: str = ""
    buy_executed_at: str = ""
    skipped: bool = False
    last_action: str = ""
    last_action_by: str = "dashboard"
    last_action_ts: str = ""
    # Idempotency: last request_id per action
    last_sell_request_id: str = ""
    last_sell_result: Dict = field(default_factory=dict)
    last_buy_request_id: str = ""
    last_buy_result: Dict = field(default_factory=dict)
    # Mode
    mode: str = "manual"  # "manual" | "auto"


# ── Singleton State + Lock ────────────────────────────────────

_lock = threading.Lock()
_state = RebalCycleState()
_initialized = False


def get_phase() -> str:
    """Public accessor for current rebalance phase (thread-safe)."""
    return _state.phase


def is_busy() -> bool:
    """True if rebalance cycle is in progress (not IDLE/SKIPPED/BLOCKED)."""
    return _state.phase in (
        "PREVIEW_READY", "SELL_RUNNING", "SELL_COMPLETE",
        "BUY_READY", "BUY_RUNNING", "BUY_COMPLETE",
    )


def _ensure_init(state_mgr):
    """Load persisted state on first access."""
    global _state, _initialized
    if _initialized:
        return
    _initialized = True
    try:
        rt = state_mgr.load_runtime()
        saved = rt.get("rebal_cycle_state", {})
        if saved:
            for k, v in saved.items():
                if hasattr(_state, k):
                    setattr(_state, k, v)
            logger.info(
                f"[REBAL_STATE_RESTORED] cycle={_state.cycle_id} "
                f"phase={_state.phase} mode={_state.mode}")
    except Exception as e:
        logger.warning(f"[REBAL_STATE_RESTORE_FAIL] {e}")


def _persist(state_mgr):
    """Save cycle state to runtime."""
    try:
        rt = state_mgr.load_runtime()
        rt["rebal_cycle_state"] = asdict(_state)
        state_mgr.save_runtime(rt)
    except Exception as e:
        logger.error(f"[REBAL_PERSIST_FAIL] {e}")


def _set_phase(phase: str, action: str, state_mgr):
    """Transition phase + persist."""
    prev = _state.phase
    _state.phase = phase
    _state.last_action = action
    _state.last_action_ts = datetime.now().isoformat()
    _persist(state_mgr)
    if prev != phase:
        logger.info(f"[REBAL_PHASE] {prev} -> {phase} ({action})")


# ── Gate Checks ───────────────────────────────────────────────

def _check_gates(state_mgr, config, provider, guard=None) -> Optional[str]:
    """Check all pre-conditions. Returns rejection reason or None."""
    # BuyPermission
    if guard:
        try:
            from risk.exposure_guard import BuyPermission
            perm, reason = guard.get_buy_permission()
            if perm in (BuyPermission.BLOCKED, BuyPermission.RECOVERING):
                return f"BuyPermission={perm.value}: {reason}"
        except Exception:
            pass

    # Open orders
    try:
        open_orders = provider.query_open_orders()
        if open_orders is None:
            return "open_orders query failed"
        if len(open_orders) > 0:
            return f"open_orders={len(open_orders)} (must be 0)"
    except Exception as e:
        return f"open_orders check failed: {e}"

    # Target exists + not stale
    try:
        from strategy.factor_ranker import load_target_portfolio
        target = load_target_portfolio(config.SIGNALS_DIR)
        if not target:
            return "No target portfolio. Run --batch first."
        target_date = target.get("date", "")
        if target_date:
            td = datetime.strptime(target_date, "%Y%m%d").date()
            stale = (date.today() - td).days
            if stale > 7:
                return f"Target stale: {stale} days old (max 7)"
    except Exception as e:
        return f"Target check failed: {e}"

    return None


def _check_sell_gates(state_mgr, config, provider, guard=None) -> Optional[str]:
    """SELL-specific gates."""
    base = _check_gates(state_mgr, config, provider, guard)
    if base:
        return base

    # Pending external
    try:
        pe = state_mgr.load_pending_external()
        if pe and len(pe) > 0:
            return f"pending_external={len(pe)} (must be 0)"
    except Exception:
        pass

    return None


def _check_buy_gates(state_mgr, config, provider, guard=None) -> Optional[str]:
    """BUY-specific gates."""
    base = _check_gates(state_mgr, config, provider, guard)
    if base:
        return base

    if _state.sell_status != "COMPLETE":
        return f"sell_status={_state.sell_status} (need COMPLETE)"

    # T+1 check
    if _state.sell_executed_at:
        try:
            sell_date = datetime.fromisoformat(_state.sell_executed_at).date()
            if sell_date == date.today():
                return "T+0: SELL executed today, BUY available tomorrow (T+1)"
        except Exception:
            pass

    return None


# ── Public API ────────────────────────────────────────────────

def get_rebalance_status(state_mgr, config, guard=None) -> Dict:
    """Status for dashboard display."""
    with _lock:
        _ensure_init(state_mgr)

        from lifecycle.utils import _count_trading_days
        runtime = state_mgr.load_runtime()
        last_rebal = runtime.get("last_rebalance_date", "")
        today_dt = date.today()

        trading_days = 0
        if last_rebal:
            try:
                last_dt = datetime.strptime(last_rebal, "%Y%m%d").date()
                trading_days = _count_trading_days(last_dt, today_dt, config)
            except (ValueError, TypeError):
                trading_days = 999

        threshold = config.REBAL_DAYS
        d_day = threshold - trading_days
        in_window = trading_days >= (threshold - WINDOW_ADVANCE_DAYS)

        # Auto-update phase if needed
        if _state.phase == "IDLE" and in_window:
            _set_phase("WINDOW_OPEN", "window_opened", state_mgr)
        elif _state.phase in ("WINDOW_OPEN", "PREVIEW_READY") and not in_window:
            # Window closed but pending buys exist -> stay
            pb, ss = state_mgr.load_pending_buys()
            if not pb:
                _set_phase("IDLE", "window_closed", state_mgr)

        # Check blocked
        blocked = False
        blocked_reason = ""
        if guard:
            try:
                from risk.exposure_guard import BuyPermission
                perm, reason = guard.get_buy_permission()
                if perm in (BuyPermission.BLOCKED, BuyPermission.RECOVERING):
                    blocked = True
                    blocked_reason = reason
            except Exception:
                pass

        # Button enablement
        phase = _state.phase
        can_preview = phase in ("WINDOW_OPEN", "PREVIEW_READY") and not blocked
        can_sell = phase == "PREVIEW_READY" and not blocked
        can_buy = phase == "BUY_READY" and not blocked
        can_skip = phase in ("WINDOW_OPEN", "PREVIEW_READY", "SELL_COMPLETE",
                             "BUY_READY") and not blocked

        # Pending age
        pending_age = 0
        if _state.sell_executed_at:
            try:
                sd = datetime.fromisoformat(_state.sell_executed_at).date()
                pending_age = (today_dt - sd).days
            except Exception:
                pass

        # Sell disable reason
        sell_disable_reason = ""
        buy_disable_reason = ""
        if blocked:
            sell_disable_reason = buy_disable_reason = blocked_reason
        elif not can_sell and phase == "WINDOW_OPEN":
            sell_disable_reason = "Run Preview first"
        elif not can_sell:
            sell_disable_reason = f"Phase: {phase}"
        if not can_buy and phase == "SELL_COMPLETE":
            buy_disable_reason = "T+1 not passed" if _state.sell_executed_at else "Sell incomplete"
        elif not can_buy:
            buy_disable_reason = f"Phase: {phase}"

        return {
            "mode": _state.mode,
            "phase": phase,
            "cycle_id": _state.cycle_id,
            "in_window": in_window,
            "d_day": d_day,
            "trading_days_since": trading_days,
            "threshold": threshold,
            "last_rebalance": last_rebal,
            "can_preview": can_preview,
            "can_sell": can_sell,
            "can_buy": can_buy,
            "can_skip": can_skip,
            "blocked": blocked,
            "blocked_reason": blocked_reason,
            "sell_disable_reason": sell_disable_reason,
            "buy_disable_reason": buy_disable_reason,
            "sell_status": _state.sell_status,
            "buy_status": _state.buy_status,
            "preview_hash": _state.preview_hash[:8] if _state.preview_hash else "",
            "pending_buys_count": len(_state.preview_buys) if _state.sell_status == "COMPLETE" else 0,
            "pending_age_days": pending_age,
            "pending_age_warn": pending_age >= PENDING_BUYS_WARN_DAYS,
            "is_running": _state.phase in ("SELL_RUNNING", "BUY_RUNNING"),
        }


def create_preview(state_mgr, config, provider) -> Dict:
    """Create preview snapshot. Locks preview_hash for subsequent execute."""
    with _lock:
        _ensure_init(state_mgr)

        if _state.phase not in ("WINDOW_OPEN", "PREVIEW_READY"):
            return {"error": f"Cannot preview in phase {_state.phase}"}

        from strategy.factor_ranker import load_target_portfolio
        from strategy.rebalancer import compute_orders
        from core.portfolio_manager import PortfolioManager

        # Load target
        target = load_target_portfolio(config.SIGNALS_DIR)
        if not target:
            return {"error": "No target portfolio. Run --batch first."}

        # Load portfolio
        saved = state_mgr.load_portfolio()
        if not saved:
            return {"error": "No portfolio state."}

        portfolio = PortfolioManager(
            config.INITIAL_CASH, config.DAILY_DD_LIMIT,
            config.MONTHLY_DD_LIMIT, config.N_STOCKS)
        portfolio.restore_from_dict(saved, buy_cost=config.BUY_COST)

        # Prices
        target_tickers = target["target_tickers"]
        scores = target.get("scores", {})
        all_codes = set(portfolio.positions.keys()) | set(target_tickers)
        prices = {}
        price_fails = []
        for code in all_codes:
            try:
                p = provider.get_current_price(code)
                if p > 0:
                    prices[code] = p
                else:
                    price_fails.append(code)
            except Exception:
                price_fails.append(code)
        portfolio.update_prices(prices)

        # Compute orders
        sell_orders, buy_orders = compute_orders(
            current_positions={
                c: {"quantity": p.quantity, "avg_price": p.avg_price}
                for c, p in portfolio.positions.items()
            },
            target_tickers=target_tickers,
            total_equity=portfolio.get_current_equity(),
            current_cash=portfolio.cash,
            buy_cost=config.BUY_COST,
            sell_cost=config.SELL_COST,
            prices=prices,
            cash_buffer=config.CASH_BUFFER_RATIO,
        )

        # Build preview data
        sells = []
        for o in sell_orders:
            pos = portfolio.positions.get(o.ticker)
            sells.append({
                "code": o.ticker,
                "qty": o.quantity,
                "price": prices.get(o.ticker, 0),
                "amount": int(o.quantity * prices.get(o.ticker, 0)),
                "pnl_pct": round(pos.unrealized_pnl_pct, 4) if pos else 0,
            })

        buys = []
        for rank, o in enumerate(buy_orders, 1):
            s = scores.get(o.ticker, {})
            p = prices.get(o.ticker, 0)
            buys.append({
                "code": o.ticker,
                "target_amount": int(o.target_amount),
                "est_qty": int(o.target_amount / p) if p > 0 else 0,
                "price": p,
                "rank": rank,
                "vol": round(s.get("vol_12m", 0), 4),
                "mom": round(s.get("mom_12_1", 0), 4),
            })

        # Snapshot hash: deterministic from orders
        hash_input = json.dumps({"sells": sells, "buys": buys}, sort_keys=True)
        preview_hash = hashlib.sha256(hash_input.encode()).hexdigest()

        # Create cycle_id if new
        if not _state.cycle_id or _state.phase == "WINDOW_OPEN":
            _state.cycle_id = f"rebal_{date.today().strftime('%Y%m%d')}_{uuid.uuid4().hex[:6]}"

        # Save preview
        _state.preview_batch_id = uuid.uuid4().hex[:12]
        _state.preview_snapshot_ts = time.time()
        _state.preview_hash = preview_hash
        _state.preview_sells = sells
        _state.preview_buys = buys
        _set_phase("PREVIEW_READY", "preview_created", state_mgr)

        logger.info(
            f"[REBAL_PREVIEW_CREATED] cycle={_state.cycle_id} "
            f"hash={preview_hash[:8]} sells={len(sells)} buys={len(buys)}")

        return {
            "cycle_id": _state.cycle_id,
            "preview_hash": preview_hash,
            "preview_batch_id": _state.preview_batch_id,
            "target_date": target.get("date", "?"),
            "equity": int(portfolio.get_current_equity()),
            "cash": int(portfolio.cash),
            "sells": sells,
            "buys": buys,
            "price_fails": price_fails,
        }


def execute_sell(state_mgr, config, provider, executor, trade_logger,
                 tracker, guard=None, name_cache=None,
                 request_id: str = "", preview_hash: str = "") -> Dict:
    """Execute SELL orders from locked preview."""
    with _lock:
        _ensure_init(state_mgr)

        # Idempotency check
        if request_id and request_id == _state.last_sell_request_id:
            logger.info(f"[REBAL_DUPLICATE_BLOCKED] sell request_id={request_id}")
            return _state.last_sell_result

        # Phase check
        if _state.phase == "SELL_RUNNING":
            return {"ok": False, "error": "SELL already running"}
        if _state.phase != "PREVIEW_READY":
            reason = f"Phase={_state.phase}, need PREVIEW_READY"
            logger.warning(f"[REBAL_SELL_REJECTED] {reason}")
            return {"ok": False, "error": reason}

        # Preview hash check
        if preview_hash and preview_hash != _state.preview_hash:
            logger.warning(f"[REBAL_PREVIEW_STALE] expected={_state.preview_hash[:8]} got={preview_hash[:8]}")
            return {"ok": False, "error": "Preview outdated. Run Preview again."}

        # Gate checks
        gate_fail = _check_sell_gates(state_mgr, config, provider, guard)
        if gate_fail:
            logger.warning(f"[REBAL_SELL_REJECTED] {gate_fail}")
            return {"ok": False, "error": gate_fail}

        _set_phase("SELL_RUNNING", "sell_started", state_mgr)

    # Execute outside lock (long-running)
    try:
        from main import _execute_rebalance_live
        from lifecycle.utils import _safe_save
        from core.portfolio_manager import PortfolioManager

        today_str = date.today().strftime("%Y%m%d")
        from strategy.factor_ranker import load_target_portfolio
        target = load_target_portfolio(config.SIGNALS_DIR)
        saved = state_mgr.load_portfolio()
        portfolio = PortfolioManager(
            config.INITIAL_CASH, config.DAILY_DD_LIMIT,
            config.MONTHLY_DD_LIMIT, config.N_STOCKS)
        portfolio.restore_from_dict(saved, buy_cost=config.BUY_COST)

        pfail, pending_buys_list, sell_status = _execute_rebalance_live(
            portfolio, target, config, executor, provider,
            trade_logger, skip_buys=False, logger=logger,
            state_mgr=state_mgr, today_str=today_str,
            buy_scale=1.0, risk_action=None,
            regime="", mode_str="LIVE", tracker=tracker,
            name_cache=name_cache or {},
        )

        if sell_status in ("COMPLETE", "PARTIAL"):
            _safe_save(state_mgr, portfolio, context="rebal_sell_dashboard")
            state_mgr.save_pending_buys(pending_buys_list, sell_status)

        result = {
            "ok": True,
            "sell_status": sell_status,
            "pending_buys": len(pending_buys_list),
            "price_fails": pfail,
            "cycle_id": _state.cycle_id,
        }

        with _lock:
            _state.sell_status = sell_status
            _state.sell_executed_at = datetime.now().isoformat()
            if request_id:
                _state.last_sell_request_id = request_id
                _state.last_sell_result = result

            if sell_status == "COMPLETE":
                _set_phase("SELL_COMPLETE", "sell_complete", state_mgr)
                # Check T+1 for BUY readiness
                _state.phase = "BUY_READY"
                _persist(state_mgr)
            else:
                _set_phase("PREVIEW_READY", f"sell_{sell_status}", state_mgr)

        logger.info(f"[REBAL_SELL_ACCEPTED] cycle={_state.cycle_id} status={sell_status}")
        return result

    except Exception as e:
        logger.error(f"[REBAL_SELL_FAIL] {e}")
        import traceback
        logger.error(traceback.format_exc())
        with _lock:
            _set_phase("PREVIEW_READY", f"sell_failed: {e}", state_mgr)
        return {"ok": False, "error": str(e)}


def execute_buy(state_mgr, config, provider, executor, trade_logger,
                tracker, guard=None, name_cache=None,
                request_id: str = "", preview_hash: str = "") -> Dict:
    """Execute BUY orders from pending buys."""
    with _lock:
        _ensure_init(state_mgr)

        # Idempotency
        if request_id and request_id == _state.last_buy_request_id:
            logger.info(f"[REBAL_DUPLICATE_BLOCKED] buy request_id={request_id}")
            return _state.last_buy_result

        if _state.phase == "BUY_RUNNING":
            return {"ok": False, "error": "BUY already running"}
        if _state.phase != "BUY_READY":
            reason = f"Phase={_state.phase}, need BUY_READY"
            logger.warning(f"[REBAL_BUY_REJECTED] {reason}")
            return {"ok": False, "error": reason}

        # Gate checks
        gate_fail = _check_buy_gates(state_mgr, config, provider, guard)
        if gate_fail:
            logger.warning(f"[REBAL_BUY_REJECTED] {gate_fail}")
            return {"ok": False, "error": gate_fail}

        _set_phase("BUY_RUNNING", "buy_started", state_mgr)

    # Execute outside lock
    try:
        from core.portfolio_manager import PortfolioManager
        from lifecycle.utils import _safe_save
        from report.reporter import make_event_id

        today_str = date.today().strftime("%Y%m%d")
        pending_buys, sell_status = state_mgr.load_pending_buys()

        if not pending_buys:
            with _lock:
                _set_phase("BUY_READY", "no_pending_buys", state_mgr)
            return {"ok": False, "error": "No pending buys"}

        saved = state_mgr.load_portfolio()
        portfolio = PortfolioManager(
            config.INITIAL_CASH, config.DAILY_DD_LIMIT,
            config.MONTHLY_DD_LIMIT, config.N_STOCKS)
        portfolio.restore_from_dict(saved, buy_cost=config.BUY_COST)

        # Apply DD guard buy_scale (same path as auto rebalance)
        _buy_scale = 1.0
        _dd_label = "NORMAL"
        if guard:
            daily_pnl = portfolio.get_daily_pnl_pct()
            monthly_dd = portfolio.get_monthly_dd_pct()
            risk_action = guard.get_risk_action(
                daily_pnl, monthly_dd,
                dd_levels=config.DD_LEVELS,
                safe_mode_release=config.SAFE_MODE_RELEASE_THRESHOLD)
            _buy_scale = risk_action["buy_scale"]
            _dd_label = risk_action.get("level", "NORMAL")
            if daily_pnl <= config.DAILY_DD_LIMIT and _buy_scale > 0:
                _buy_scale = 0.0
                _dd_label = "DAILY_BLOCKED"
            logger.info(f"[REBAL_BUY_DD_GUARD] {_dd_label} buy_scale={_buy_scale:.0%} "
                        f"daily={daily_pnl:.2%} monthly={monthly_dd:.2%}")

        if _buy_scale == 0.0:
            with _lock:
                _set_phase("BUY_READY", f"dd_blocked_{_dd_label}", state_mgr)
            return {"ok": False, "error": f"BUY blocked by DD guard: {_dd_label}"}

        buy_results = []
        for pb in pending_buys:
            code = pb["ticker"]
            base_amount = pb.get("target_amount", 0)
            amount = base_amount * _buy_scale
            price = provider.get_current_price(code)
            if price <= 0:
                buy_results.append({"code": code, "status": "PRICE_FAIL"})
                continue
            base_qty = int(base_amount / price) if price > 0 else 0
            qty = int(amount / price)
            if qty <= 0:
                logger.info(f"[MANUAL_BUY_SKIP] code={code} reason=scaled_to_zero "
                            f"base_qty={base_qty} scale={_buy_scale:.0%}")
                buy_results.append({"code": code, "status": "QTY_ZERO"})
                continue
            if qty != base_qty:
                logger.info(f"[MANUAL_BUY_SCALE] code={code} base_qty={base_qty} "
                            f"scale={_buy_scale:.0%} final_qty={qty} dd={_dd_label}")

            eid = make_event_id(code, "BUY")
            result = executor.execute_buy(code, qty, price, "REBALANCE_ENTRY")

            if not result.get("error"):
                fill_price = result.get("exec_price", price)
                exec_qty = result.get("exec_qty", qty)
                portfolio.add_position(
                    code, exec_qty, fill_price, config.BUY_COST,
                    entry_rank=pb.get("rank", 0),
                    score_mom=pb.get("score_mom", 0))
                trade_logger.log_entry(
                    code, "REBALANCE_ENTRY", exec_qty, fill_price, event_id=eid)
                buy_results.append({
                    "code": code, "status": "FILLED",
                    "qty": exec_qty, "price": fill_price,
                })
                _safe_save(state_mgr, portfolio, context=f"rebal_buy/{code}")
            else:
                buy_results.append({
                    "code": code, "status": "FAILED",
                    "error": result.get("error", ""),
                })

        filled = sum(1 for r in buy_results if r["status"] == "FILLED")
        failed = sum(1 for r in buy_results if r["status"] == "FAILED")

        result = {
            "ok": True,
            "filled": filled,
            "failed": failed,
            "total": len(pending_buys),
            "results": buy_results,
            "cycle_reset": filled > 0,
            "cycle_id": _state.cycle_id,
        }

        with _lock:
            _state.buy_status = "COMPLETE" if filled > 0 else "FAILED"
            _state.buy_executed_at = datetime.now().isoformat()
            if request_id:
                _state.last_buy_request_id = request_id
                _state.last_buy_result = result

            if filled > 0:
                # CYCLE RESET: BUY complete = only reset trigger
                state_mgr.set_last_rebalance_date(today_str)
                state_mgr.clear_pending_buys()
                _safe_save(state_mgr, portfolio, context="rebal_buy_complete")
                _state.cycle_id = ""
                _state.preview_hash = ""
                _state.sell_status = ""
                _state.buy_status = ""
                _state.sell_executed_at = ""
                _state.buy_executed_at = ""
                _set_phase("IDLE", "buy_complete_cycle_reset", state_mgr)
                logger.info(
                    f"[REBAL_BUY_COMPLETE] {filled} filled, {failed} failed. "
                    f"Cycle reset: last_rebalance_date={today_str}")
            else:
                _set_phase("BUY_READY", "buy_all_failed", state_mgr)

        logger.info(f"[REBAL_BUY_ACCEPTED] cycle={_state.cycle_id} filled={filled}")
        return result

    except Exception as e:
        logger.error(f"[REBAL_BUY_FAIL] {e}")
        import traceback
        logger.error(traceback.format_exc())
        with _lock:
            _set_phase("BUY_READY", f"buy_failed: {e}", state_mgr)
        return {"ok": False, "error": str(e)}


def skip_rebalance(state_mgr) -> Dict:
    """Skip this cycle: reset to IDLE."""
    with _lock:
        _ensure_init(state_mgr)
        today_str = date.today().strftime("%Y%m%d")
        state_mgr.set_last_rebalance_date(today_str)
        state_mgr.clear_pending_buys()
        _state.skipped = True
        _state.cycle_id = ""
        _state.preview_hash = ""
        _state.sell_status = ""
        _state.buy_status = ""
        _set_phase("IDLE", "skipped", state_mgr)
        logger.info(f"[REBAL_SKIP] Cycle skipped. Reset to {today_str}")
        return {"ok": True, "reset_date": today_str}


def set_rebalance_mode(mode: str, state_mgr=None) -> str:
    """Toggle manual/auto."""
    with _lock:
        if mode in ("manual", "auto"):
            _state.mode = mode
            if state_mgr:
                _persist(state_mgr)
            logger.info(f"[REBAL_MODE] -> {mode}")
    return _state.mode


def get_rebalance_mode() -> str:
    return _state.mode
