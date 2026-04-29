# -*- coding: utf-8 -*-
"""
portfolio_manager.py — Position & Portfolio Management for Q-TRON US
=====================================================================
Broker = Truth. Local state is metadata auxiliary (HWM, entry_date).
pending_sell_qty is the single state for SELL order dedup.

Unit convention:
- trail_ratio: 0~1 (e.g. 0.12)
- drawdown_pct: always percent, negative (e.g. -8.3)
- near_drawdown_pct: percent threshold (e.g. -8.04)
- Comparisons: pct vs pct only
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("qtron.us.portfolio")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _seconds_since(iso_str: str) -> float:
    """Seconds elapsed since ISO timestamp. Returns inf if empty/invalid."""
    if not iso_str:
        return float("inf")
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return float("inf")


def _is_stale(last_ts: str, current_ts: str, max_gap: float = 600) -> bool:
    """True if gap between two ISO timestamps exceeds max_gap seconds."""
    if not last_ts or not current_ts:
        return False  # No previous data = not stale
    try:
        t_last = datetime.fromisoformat(last_ts)
        t_cur = datetime.fromisoformat(current_ts)
        if t_last.tzinfo is None:
            t_last = t_last.replace(tzinfo=timezone.utc)
        if t_cur.tzinfo is None:
            t_cur = t_cur.replace(tzinfo=timezone.utc)
        return abs((t_cur - t_last).total_seconds()) > max_gap
    except Exception:
        return False


# ── Position ────────────────────────────────────────────────

@dataclass
class USPosition:
    symbol: str
    quantity: int
    avg_price: float           # broker truth (SAFE_SYNC adopts broker value)
    entry_date: str
    high_watermark: float

    current_price: float = 0.0
    last_price_at: str = ""
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    trail_stop_price: float = 0.0
    drawdown_pct: float = 0.0      # HWM 대비 하락% (음수)
    pending_sell_qty: int = 0       # single state for SELL dedup (persisted)
    last_sell_order_at: str = ""    # trigger cooldown (persisted)
    updated_at: str = ""
    source: str = "broker"          # broker | state | reconciled

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> USPosition:
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in d.items() if k in known}
        return cls(**filtered)


# ── RECON Result ────────────────────────────────────────────

@dataclass
class ReconResult:
    added: List[str] = field(default_factory=list)          # broker-only symbols
    removed: List[str] = field(default_factory=list)        # state-only symbols
    qty_mismatch: List[dict] = field(default_factory=list)  # {symbol, state_qty, broker_qty}
    avg_price_mismatch: List[dict] = field(default_factory=list)  # {symbol, state, broker}
    cash_delta: float = 0.0
    equity_delta: float = 0.0
    state_uncertain: bool = False
    clean: bool = True
    action: str = "NONE"   # NONE | LOG_ONLY | LOG_WARNING | SAFE_SYNC | FORCE_SYNC

    def log_summary(self):
        logger.info(
            f"[RECON] action={self.action} "
            f"added={len(self.added)} removed={len(self.removed)} "
            f"qty_mismatch={len(self.qty_mismatch)} "
            f"avg_mismatch={len(self.avg_price_mismatch)} "
            f"cash_delta={self.cash_delta:+.2f} clean={self.clean}"
        )


# ── Portfolio Manager ───────────────────────────────────────

# Drift threshold for avg_price mismatch: above this → SAFE_SYNC
AVG_PRICE_DRIFT_RATIO = 0.005  # 0.5%

# Jump guard ratios
JUMP_GUARD_PREV_RATIO = 0.25  # 25% vs previous price
JUMP_GUARD_HWM_RATIO = 0.30   # 30% vs HWM

# Stale price max gap (seconds)
STALE_PRICE_MAX_GAP = 600

# Trigger cooldown (seconds)
TRIGGER_COOLDOWN_SEC = 60


class PortfolioManagerUS:
    """
    Position tracking with HWM-based trail stop.
    Single Writer: only the main loop modifies positions.
    """

    def __init__(self, cash: float, trail_ratio: float = 0.12,
                 daily_dd_limit: float = -0.04,
                 monthly_dd_limit: float = -0.07,
                 max_positions: int = 20):
        self.cash = cash
        self.trail_ratio = trail_ratio
        self.daily_dd_limit = daily_dd_limit
        self.monthly_dd_limit = monthly_dd_limit
        self.max_positions = max_positions
        self.positions: Dict[str, USPosition] = {}

        # Tracking
        self.broker_snapshot_at: str = ""
        self.last_price_update_at: str = ""

    # ── Price Update ────────────────────────────────────────

    def update_prices(self, prices: Dict[str, float], timestamp: str) -> None:
        """Update current prices. Stale/jump guard included.

        Stale-guard semantics (Jeff 2026-04-30 P0 fix)
        ----------------------------------------------
        Previously this function rejected any update for a position
        whose ``last_price_at`` was older than ``STALE_PRICE_MAX_GAP``
        seconds. The check used ``last_price_at`` (the position's prior
        update time) compared to ``timestamp`` (now), which made the
        guard a one-way latch: once a position fell behind by 10
        minutes — e.g. across an overnight quote-feed gap or after a
        transient Alpaca hiccup — the next update was rejected, which
        meant ``last_price_at`` was never advanced, which meant *every*
        subsequent update was rejected too. Live evidence (broker
        truth diag) showed 16 of 20 positions stuck at the
        2026-04-21 ET close timestamp for 7+ trading days, with the
        engine quietly reading INTC at $66 while the broker quoted
        $91 (a 37.58% gap that the trail-stop machinery never saw).

        The fix: detect the same "long gap" condition but treat it as
        a *recovery event*, not a rejection. The fresh quote is
        accepted; the jump-guard tolerance is widened (one-shot, just
        for this update) to absorb the multi-day cumulative move that
        prompted the recovery; the warning becomes
        ``[STALE_RECOVERY]`` so the operator can correlate with the
        ``[STALE]`` summary alert. Once the position resumes normal
        ticking the recovery flag clears and the standard 25% / 30%
        jump bounds apply again.
        """
        self.last_price_update_at = timestamp

        for sym, pos in self.positions.items():
            price = prices.get(sym, 0)
            if price <= 0 or pos.quantity <= 0:
                continue

            # Stale-recovery detection: was this position previously
            # locked out by a quote-feed gap? Only flag the FIRST
            # update after the gap; subsequent updates run with the
            # standard jump guards.
            stale_recovery = (
                bool(pos.last_price_at)
                and _is_stale(pos.last_price_at, timestamp, STALE_PRICE_MAX_GAP)
            )

            if stale_recovery:
                gap_h = _seconds_since(pos.last_price_at) / 3600
                logger.warning(
                    f"[STALE_RECOVERY] {sym}: refreshing after {gap_h:.1f}h gap — "
                    f"accepting ${price:.2f} (prev cached ${pos.current_price:.2f})"
                )

            # Jump guards. Recovery widens the tolerance one-shot to
            # absorb multi-day cumulative moves while still rejecting
            # obvious typos (>2x or <0.5x of the cached price).
            jump_prev_ratio = 1.0 if stale_recovery else JUMP_GUARD_PREV_RATIO
            jump_hwm_ratio  = 1.0 if stale_recovery else JUMP_GUARD_HWM_RATIO

            prev = pos.current_price
            if prev > 0 and abs(price - prev) / prev > jump_prev_ratio:
                logger.warning(
                    f"[JUMP_PREV] {sym}: {prev:.2f} → {price:.2f} "
                    f"(ratio {abs(price - prev) / prev:.2%}, cap {jump_prev_ratio:.0%}) — skip"
                )
                continue

            if pos.high_watermark > 0 and abs(price - pos.high_watermark) / pos.high_watermark > jump_hwm_ratio:
                logger.warning(
                    f"[JUMP_HWM] {sym}: HWM={pos.high_watermark:.2f} → {price:.2f} "
                    f"(ratio {abs(price - pos.high_watermark) / pos.high_watermark:.2%}, "
                    f"cap {jump_hwm_ratio:.0%}) — skip"
                )
                continue

            pos.current_price = price
            pos.last_price_at = timestamp

            # Drawdown FIRST (before HWM update)
            if pos.high_watermark == 0:
                pos.high_watermark = price
                pos.drawdown_pct = 0.0
            else:
                pos.drawdown_pct = (price / pos.high_watermark - 1) * 100

            # HWM update AFTER drawdown
            pos.high_watermark = max(pos.high_watermark, price)
            pos.trail_stop_price = pos.high_watermark * (1 - self.trail_ratio)

            # Market value / PnL
            pos.market_value = price * pos.quantity
            pos.unrealized_pnl = (price - pos.avg_price) * pos.quantity
            pos.unrealized_pnl_pct = (
                (price / pos.avg_price - 1) * 100 if pos.avg_price > 0 else 0
            )

    # ── Trail Stop ──────────────────────────────────────────

    def check_trail_stops(self) -> Tuple[List[str], List[Tuple[str, float]]]:
        """
        Returns (triggered, near).
        triggered: [symbol, ...]
        near: [(symbol, drawdown_pct), ...]

        Both use trail_stop_price as single source for trigger.
        near_price = HWM * (1 - trail_ratio * 0.67).
        """
        triggered: List[str] = []
        near: List[Tuple[str, float]] = []

        for sym, pos in self.positions.items():
            if pos.quantity <= 0 or pos.current_price <= 0:
                continue

            # Already has pending SELL — skip
            if pos.pending_sell_qty > 0:
                continue

            # Cooldown after recent SELL order
            if pos.last_sell_order_at and _seconds_since(pos.last_sell_order_at) < TRIGGER_COOLDOWN_SEC:
                continue

            trigger_price = pos.trail_stop_price
            near_price = (
                pos.high_watermark * (1 - self.trail_ratio * 0.67)
                if pos.high_watermark > 0 else 0
            )

            if trigger_price > 0 and pos.current_price <= trigger_price:
                triggered.append(sym)
            elif near_price > 0 and pos.current_price <= near_price:
                near.append((sym, pos.drawdown_pct))

        return triggered, near

    # ── Position Mutations ──────────────────────────────────

    def add_position(self, symbol: str, qty: int, avg_price: float,
                     entry_date: str = "") -> None:
        if symbol in self.positions:
            pos = self.positions[symbol]
            # Average up
            total_qty = pos.quantity + qty
            if total_qty > 0:
                pos.avg_price = (
                    (pos.avg_price * pos.quantity + avg_price * qty) / total_qty
                )
            pos.quantity = total_qty
            pos.updated_at = _now_iso()
        else:
            self.positions[symbol] = USPosition(
                symbol=symbol,
                quantity=qty,
                avg_price=avg_price,
                entry_date=entry_date or _now_iso()[:10],
                high_watermark=avg_price,
                current_price=avg_price,
                trail_stop_price=avg_price * (1 - self.trail_ratio),
                updated_at=_now_iso(),
                source="broker",
            )

    def remove_position(self, symbol: str) -> Optional[USPosition]:
        return self.positions.pop(symbol, None)

    def handle_fill(self, event: dict) -> None:
        """
        Process a single fill event from the queue.
        Adjusts quantity and pending_sell_qty.
        """
        sym = event.get("symbol", "")
        side = event.get("side", "")
        filled_qty = event.get("new_fill_qty", 0)
        avg_price = event.get("avg_price", 0)

        if filled_qty <= 0:
            return  # ghost-resolved event with 0 new qty

        if side == "SELL":
            pos = self.positions.get(sym)
            if not pos:
                logger.warning(f"[FILL] SELL {sym} but no position")
                return
            pos.quantity = max(0, pos.quantity - filled_qty)
            pos.pending_sell_qty = max(0, pos.pending_sell_qty - filled_qty)
            pos.updated_at = _now_iso()

            if pos.quantity <= 0:
                pos.pending_sell_qty = 0
                logger.info(f"[FILL] {sym} fully sold, removing position")
                self.remove_position(sym)

        elif side == "BUY":
            self.add_position(sym, filled_qty, avg_price)

    # ── RECON ───────────────────────────────────────────────

    def reconcile_with_broker(
        self,
        broker_holdings: List[dict],
        broker_cash: float,
        dirty_exit: bool,
        open_orders: List[dict],
    ) -> ReconResult:
        """
        Compare local state with broker truth.
        Returns ReconResult with action policy:
        - NONE: clean match
        - LOG_ONLY: has open orders, don't touch
        - LOG_WARNING: minor mismatch
        - SAFE_SYNC: quantity or material avg_price drift
        - FORCE_SYNC: dirty exit, full rebuild needed
        """
        result = ReconResult()

        broker_map = {h["code"]: h for h in broker_holdings}
        state_syms = set(self.positions.keys())
        broker_syms = set(broker_map.keys())

        # Added (broker-only)
        result.added = sorted(broker_syms - state_syms)

        # Removed (state-only)
        result.removed = sorted(state_syms - broker_syms)

        # Quantity & avg_price mismatch
        for sym in state_syms & broker_syms:
            pos = self.positions[sym]
            bh = broker_map[sym]
            b_qty = bh.get("qty", 0)
            b_avg = bh.get("avg_price", 0)

            if pos.quantity != b_qty:
                result.qty_mismatch.append({
                    "symbol": sym,
                    "state_qty": pos.quantity,
                    "broker_qty": b_qty,
                })

            if b_avg > 0 and pos.avg_price > 0:
                drift = abs(b_avg - pos.avg_price) / b_avg
                if drift > 0.001:  # > 0.1% difference
                    result.avg_price_mismatch.append({
                        "symbol": sym,
                        "state": pos.avg_price,
                        "broker": b_avg,
                        "drift_ratio": drift,
                    })

        # Cash / equity delta
        result.cash_delta = broker_cash - self.cash
        result.state_uncertain = dirty_exit

        # Clean check
        result.clean = (
            not result.added
            and not result.removed
            and not result.qty_mismatch
            and not result.avg_price_mismatch
            and abs(result.cash_delta) < 1.0  # < $1 tolerance
        )

        # Action policy
        has_open = bool(open_orders)

        if result.clean:
            result.action = "NONE"
        elif has_open:
            result.action = "LOG_ONLY"
        elif dirty_exit:
            result.action = "FORCE_SYNC"
        elif result.added or result.removed or result.qty_mismatch:
            result.action = "SAFE_SYNC"
        elif result.avg_price_mismatch:
            material = any(
                m["drift_ratio"] > AVG_PRICE_DRIFT_RATIO
                for m in result.avg_price_mismatch
            )
            result.action = "SAFE_SYNC" if material else "LOG_WARNING"
        else:
            result.action = "LOG_WARNING"

        result.log_summary()
        return result

    def apply_recon(self, result: ReconResult,
                    broker_holdings: List[dict],
                    broker_cash: float) -> None:
        """Apply RECON result (FORCE_SYNC or SAFE_SYNC).

        INVARIANT-1 (broker truth):
            모든 positions/cash는 broker 값을 직접 덮어씀.
            내부 계산값(PnL, market_value 등)은 이후 update_prices()가 갱신.

        INVARIANT-2 (pending zeroing):
            FORCE_SYNC/SAFE_SYNC 시 pending_sell_qty 및 last_sell_order_at을
            반드시 0/""으로 초기화.
            → 이후 sync_pending_with_broker()가 동일 broker snapshot 기준으로 재설정.
            이 초기화를 제거하거나 순서를 바꾸면 stale pending이 trail stop을 영구 차단함.

        호출 순서 보장 (위반 금지):
            apply_recon(result, holdings, cash)   # 1. broker truth 반영 + pending=0
            sync_pending_with_broker(open_orders)  # 2. 동일 snapshot으로 pending 재설정
            runtime_data["last_recon_ok"] = ...    # 3. gate runtime 기록
            state_mgr.save_all(...)                # 4. atomic 저장
        """
        if result.action == "FORCE_SYNC":
            self._rebuild_from_broker(broker_holdings, broker_cash)
        elif result.action == "SAFE_SYNC":
            broker_map = {h["code"]: h for h in broker_holdings}
            for m in result.qty_mismatch:
                sym = m["symbol"]
                if sym in self.positions:
                    self.positions[sym].quantity = m["broker_qty"]
                    self.positions[sym].source = "reconciled"
            for m in result.avg_price_mismatch:
                sym = m["symbol"]
                if sym in self.positions:
                    self.positions[sym].avg_price = m["broker"]
                    self.positions[sym].source = "reconciled"
            self.cash = broker_cash

            # Add broker-only positions
            for sym in result.added:
                bh = broker_map[sym]
                self.add_position(sym, bh["qty"], bh["avg_price"])
                self.positions[sym].source = "reconciled"

            # Remove state-only positions
            for sym in result.removed:
                self.remove_position(sym)

        # Reset pending state after RECON (sync_pending will re-set)
        if result.action in ("FORCE_SYNC", "SAFE_SYNC"):
            for pos in self.positions.values():
                pos.pending_sell_qty = 0
                pos.last_sell_order_at = ""

        logger.info(f"[RECON] Applied action={result.action}")

    def _rebuild_from_broker(self, broker_holdings: List[dict],
                             broker_cash: float) -> None:
        """Full rebuild from broker — preserve HWM/entry_date if available."""
        old_positions = dict(self.positions)
        self.positions.clear()
        self.cash = broker_cash

        for bh in broker_holdings:
            sym = bh["code"]
            old = old_positions.get(sym)

            self.positions[sym] = USPosition(
                symbol=sym,
                quantity=bh["qty"],
                avg_price=bh["avg_price"],
                entry_date=old.entry_date if old else _now_iso()[:10],
                high_watermark=old.high_watermark if old else bh.get("cur_price", bh["avg_price"]),
                current_price=bh.get("cur_price", 0),
                market_value=bh.get("market_value", 0),
                unrealized_pnl=bh.get("pnl", 0),
                unrealized_pnl_pct=bh.get("pnl_pct", 0),
                updated_at=_now_iso(),
                source="reconciled",
            )

        logger.info(f"[RECON] Rebuilt {len(self.positions)} positions from broker")

    # ── Pending ↔ Open Orders Sync ──────────────────────────

    def sync_pending_with_broker(self, open_orders: List[dict]) -> None:
        """
        pending_sell_qty를 broker open orders 기준으로 재설정.

        호출 시점:
          - 스타트업 (startup Phase 1.5 이후)
          - periodic RECON 후 (apply_recon 직후, 동일 open_orders snapshot)

        INVARIANT-3 (snapshot 일관성):
            open_orders는 apply_recon에 전달된 것과 동일 broker 조회 결과여야 함.
            다른 시점에 조회한 open_orders를 사용하면 pending 상태가 불일치할 수 있음.

        INVARIANT-4 (단방향 권한):
            이 함수는 pending_sell_qty만 수정. positions qty/avg_price는 건드리지 않음.
            broker truth 반영(qty/price)은 apply_recon 전용.

        결과:
          - pending>0 + broker SELL order 존재 → pending = remaining (유지)
          - pending>0 + broker SELL order 없음 + position 있음 → pending=0 (PENDING_CLEAR)
          - pending>0 + no position → pending=0 (PENDING_FILLED)
          - pending=0 → 변경 없음 (LOG_ONLY 경로에서도 안전)
        """
        orders_by_sym: Dict[str, List[dict]] = {}
        for o in open_orders:
            orders_by_sym.setdefault(o["code"], []).append(o)

        for sym, pos in list(self.positions.items()):
            sym_orders = orders_by_sym.get(sym, [])
            sell_orders = [o for o in sym_orders if o["side"] == "SELL"]
            buy_orders = [o for o in sym_orders if o["side"] == "BUY"]

            if buy_orders:
                logger.info(f"[PENDING_BUY] {sym}: {len(buy_orders)} open BUY order(s)")

            if pos.pending_sell_qty > 0:
                if sell_orders:
                    # Broker has open SELL — keep pending
                    total_remaining = sum(o.get("remaining", 0) for o in sell_orders)
                    if total_remaining > 0:
                        pos.pending_sell_qty = total_remaining
                        logger.info(f"[PENDING_SYNC] {sym}: pending={total_remaining} (broker open SELL)")
                elif pos.quantity > 0:
                    logger.info(
                        f"[PENDING_CLEAR] {sym}: pending_sell={pos.pending_sell_qty} "
                        f"but no broker open SELL order, clearing"
                    )
                    pos.pending_sell_qty = 0
                    pos.last_sell_order_at = ""
                else:
                    logger.info(f"[PENDING_FILLED] {sym}: no holding, interpreting as FILLED")
                    pos.pending_sell_qty = 0
                    pos.last_sell_order_at = ""

    # ── Serialization ───────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "cash": self.cash,
            "trail_ratio": self.trail_ratio,
            "max_positions": self.max_positions,
            "broker_snapshot_at": self.broker_snapshot_at,
            "last_price_update_at": self.last_price_update_at,
            "positions": {
                sym: pos.to_dict() for sym, pos in self.positions.items()
            },
        }

    @classmethod
    def from_dict(cls, data: dict, config=None) -> PortfolioManagerUS:
        trail = data.get("trail_ratio", getattr(config, "TRAIL_PCT", 0.12) if config else 0.12)
        pm = cls(
            cash=data.get("cash", 0),
            trail_ratio=trail,
            daily_dd_limit=getattr(config, "DAILY_DD_LIMIT", -0.04) if config else -0.04,
            monthly_dd_limit=getattr(config, "MONTHLY_DD_LIMIT", -0.07) if config else -0.07,
            max_positions=data.get("max_positions", 20),
        )
        pm.broker_snapshot_at = data.get("broker_snapshot_at", "")
        pm.last_price_update_at = data.get("last_price_update_at", "")

        for sym, pos_data in data.get("positions", {}).items():
            pm.positions[sym] = USPosition.from_dict(pos_data)

        return pm

    # ── Utility ─────────────────────────────────────────────

    def get_equity(self) -> float:
        """Estimated equity = cash + sum(market_value)."""
        return self.cash + sum(p.market_value for p in self.positions.values())

    # ── DD Metrics (P0 fix: fail-closed) ───────────────────

    def get_daily_pnl_pct(self) -> float:
        """
        Daily P&L % = (current_equity / day_open_equity - 1).
        Source of truth: _day_open_equity (set at market open or first price update).
        Fail-closed: returns -999 if calculation impossible → triggers DD block.
        """
        try:
            if not hasattr(self, "_day_open_equity") or self._day_open_equity <= 0:
                logger.warning("[US_DD_CALC_FAIL] _day_open_equity not set → fail-closed")
                return -999.0  # fail-closed: block buys
            equity = self.get_equity()
            if equity <= 0:
                return -999.0
            return equity / self._day_open_equity - 1
        except Exception as e:
            logger.error(f"[US_DD_CALC_FAIL] daily: {e}")
            return -999.0  # fail-closed

    def get_monthly_dd_pct(self) -> float:
        """
        Monthly drawdown % = (current_equity / month_peak_equity - 1).
        Source of truth: _month_peak_equity (tracked continuously, reset on month change).
        Fail-closed: returns -999 if calculation impossible → triggers DD block.
        """
        try:
            if not hasattr(self, "_month_peak_equity") or self._month_peak_equity <= 0:
                logger.warning("[US_DD_CALC_FAIL] _month_peak_equity not set → fail-closed")
                return -999.0
            equity = self.get_equity()
            if equity <= 0:
                return -999.0
            return equity / self._month_peak_equity - 1
        except Exception as e:
            logger.error(f"[US_DD_CALC_FAIL] monthly: {e}")
            return -999.0

    def init_dd_tracking(self, equity: float = 0) -> None:
        """Initialize DD tracking. Call at startup and market open."""
        eq = equity if equity > 0 else self.get_equity()
        if eq <= 0:
            eq = self.cash if self.cash > 0 else 1.0  # safety fallback
        self._day_open_equity = eq
        self._month_peak_equity = getattr(self, "_month_peak_equity", 0)
        if self._month_peak_equity <= 0:
            self._month_peak_equity = eq
        self._current_month = datetime.now().month
        logger.info(
            f"[US_DD_INIT] day_open={self._day_open_equity:.2f} "
            f"month_peak={self._month_peak_equity:.2f}"
        )

    def update_dd_tracking(self) -> None:
        """Update month peak. Call after each price update."""
        equity = self.get_equity()
        if equity <= 0:
            return

        # Month change → reset peak
        now_month = datetime.now().month
        if hasattr(self, "_current_month") and now_month != self._current_month:
            self._month_peak_equity = equity
            self._current_month = now_month
            logger.info(f"[US_DD_MONTH_RESET] new peak={equity:.2f}")
        elif equity > getattr(self, "_month_peak_equity", 0):
            self._month_peak_equity = equity

    def __repr__(self) -> str:
        return (
            f"PortfolioUS(cash=${self.cash:,.2f}, "
            f"positions={len(self.positions)}, "
            f"equity~${self.get_equity():,.2f})"
        )
