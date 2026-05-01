"""alert_dedup.py — Throttle/dedup helpers for US alert hooks.

Pure observability — these helpers do not modify trading state.
Called from the main loop after engine decisions are already made;
they decide *whether* to send a notification, never *what* the
engine does.

Threading: all consumers run in the main loop thread, so plain
module-level state is sufficient. Tests reset state via
``reset_for_test()``.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Tuple

# ── DD label transition ────────────────────────────────────────
_last_dd_label: str = "NORMAL"


def dd_transition(new_label: str) -> Tuple[bool, str, str]:
    """True iff dd_label changed since the last call.

    Returns (fired, prev_label, new_label). Caller composes the
    notify message — this helper only owns the transition latch.
    """
    global _last_dd_label
    prev = _last_dd_label
    if new_label != prev:
        _last_dd_label = new_label
        return True, prev, new_label
    return False, prev, new_label


# ── STALE burst summary ────────────────────────────────────────
STALE_THROTTLE_SEC = 3600       # 1 hour between burst alerts
STALE_AGE_THRESHOLD_SEC = 21600 # 6 hours = pathological staleness

_last_stale_alert_at: float = 0.0
_last_stale_count: int = 0


def count_stale_positions(positions, now_iso: str,
                          age_threshold_sec: float = STALE_AGE_THRESHOLD_SEC
                          ) -> Tuple[int, List[str]]:
    """Return (count, sorted symbols) for positions with last_price_at
    older than age_threshold_sec. Skips qty<=0 to mirror update_prices."""
    try:
        now = datetime.fromisoformat(now_iso)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
    except Exception:
        return 0, []
    stale: List[str] = []
    for sym, pos in positions.items():
        last = getattr(pos, "last_price_at", "") or ""
        qty = getattr(pos, "quantity", 0) or 0
        if not last or qty <= 0:
            continue
        try:
            t_last = datetime.fromisoformat(last)
            if t_last.tzinfo is None:
                t_last = t_last.replace(tzinfo=timezone.utc)
            age = (now - t_last).total_seconds()
            if age > age_threshold_sec:
                stale.append(sym)
        except Exception:
            continue
    return len(stale), sorted(stale)


def stale_should_fire(stale_count: int) -> bool:
    """Throttle: fire on first burst, again only after 1h or on recovery.

    Recovery (count → 0 after non-zero) always fires once — that's
    the signal the operator most needs.
    """
    global _last_stale_alert_at, _last_stale_count
    now = time.time()
    if stale_count == 0:
        if _last_stale_count > 0:
            _last_stale_count = 0
            _last_stale_alert_at = now
            return True
        return False
    if _last_stale_count == 0:
        _last_stale_count = stale_count
        _last_stale_alert_at = now
        return True
    if now - _last_stale_alert_at >= STALE_THROTTLE_SEC:
        _last_stale_count = stale_count
        _last_stale_alert_at = now
        return True
    return False


# ── Single-loop equity drop ────────────────────────────────────
EQUITY_DROP_THRESHOLD = -0.05  # -5%

_prev_equity: float = 0.0
_equity_drop_active: bool = False


def equity_drop_should_fire(equity: float,
                             threshold: float = EQUITY_DROP_THRESHOLD
                             ) -> Tuple[bool, float, float]:
    """Return (fired, prev_equity, current_equity).

    One-shot per drop episode: re-arms only after equity recovers
    above the threshold gap. Prevents spam while the drop persists.
    """
    global _prev_equity, _equity_drop_active
    prev = _prev_equity
    _prev_equity = equity
    if prev <= 0:
        return False, prev, equity
    pct = (equity - prev) / prev
    if pct <= threshold and not _equity_drop_active:
        _equity_drop_active = True
        return True, prev, equity
    if pct > threshold and _equity_drop_active:
        _equity_drop_active = False
    return False, prev, equity


# ── STARTUP_BLOCKED entry one-shot ─────────────────────────────
_startup_alert_sent: bool = False


def startup_block_should_fire() -> bool:
    """One-shot per process: send STARTUP_BLOCKED entry alert once."""
    global _startup_alert_sent
    if _startup_alert_sent:
        return False
    _startup_alert_sent = True
    return True


# ── Trail-near per-symbol throttle (Jeff 2026-05-01) ──────────
# Pre-fix bug: ``us/main.py`` deduped trail-near alerts by a process-
# memory ``_notified_near`` set with reset-on-recovery semantics. When
# a position oscillates around the near boundary (current_price ≈ HWM
# × 0.92 with ``trail_ratio=12%``), it enters/exits the set every
# tick → alert fires every loop. 165 DELL "Trail Stop Near" alerts in
# 24h was the live evidence (2026-05-01 ~04:30 KST screenshot).
#
# Fix: time-based per-symbol throttle.
#   * First entry → fire.
#   * Re-entry after a confirmed recovery gap → fire.
#   * Re-entry without sufficient recovery (jitter) → suppress.
#   * Continuous near for ≥ throttle window → fire reminder once
#     per window (so the operator still sees a stale near).
TRAIL_NEAR_THROTTLE_SEC = 3600          # 1h between same-symbol re-alerts
TRAIL_NEAR_RECOVERY_GRACE_SEC = 300     # 5min out-of-zone before re-arming

_trail_near_state: "dict[str, dict]" = {}


def trail_near_should_fire(
    symbol: str, in_near_zone: bool, *, now: "float | None" = None,
) -> bool:
    """Decide whether to send a Trail Stop Near alert for ``symbol``.

    State machine (per symbol):
      * ``last_alert_at = None``  → never alerted; first in-zone fires.
      * ``out_since = None``      → currently in zone (or never seen).
      * ``out_since = <ts>``      → currently out of zone since <ts>.

    Re-entry fires only when the out_since gap meets the recovery
    grace; otherwise (jitter) the alert is throttled by the 1h
    cooldown.
    """
    if now is None:
        now = time.time()
    st = _trail_near_state.setdefault(symbol, {
        "last_alert_at": None,
        "out_since": None,
    })

    if not in_near_zone:
        # First detection of leaving the zone records the out_since.
        # Subsequent out-of-zone ticks keep the original out_since so
        # the recovery gap accumulates correctly.
        if st["out_since"] is None:
            st["out_since"] = now
        return False

    # In-zone path.
    last_alert_at = st["last_alert_at"]
    out_since     = st["out_since"]
    out_duration  = (now - out_since) if out_since is not None else None

    # Re-entered the zone — clear out_since so the next exit can re-mark it.
    st["out_since"] = None

    fire = False
    if last_alert_at is None:
        fire = True                                        # first sighting
    elif out_duration is not None and out_duration >= TRAIL_NEAR_RECOVERY_GRACE_SEC:
        fire = True                                        # confirmed re-entry after recovery
    elif (now - last_alert_at) >= TRAIL_NEAR_THROTTLE_SEC:
        fire = True                                        # 1h reminder
    # else: jitter or within-cooldown — suppress.

    if fire:
        st["last_alert_at"] = now

    return fire


def trail_near_clear(symbol: str) -> None:
    """Forget all state for one symbol — used when its position closes."""
    _trail_near_state.pop(symbol, None)


# ── Test helper ────────────────────────────────────────────────

def reset_for_test() -> None:
    global _last_dd_label, _last_stale_alert_at, _last_stale_count
    global _prev_equity, _equity_drop_active, _startup_alert_sent
    _last_dd_label = "NORMAL"
    _last_stale_alert_at = 0.0
    _last_stale_count = 0
    _prev_equity = 0.0
    _equity_drop_active = False
    _startup_alert_sent = False
    _trail_near_state.clear()
