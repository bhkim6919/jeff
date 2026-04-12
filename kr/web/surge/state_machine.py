# -*- coding: utf-8 -*-
"""
state_machine.py -- Stock State Machine (lock-free)
=====================================================
상태 전이 유효성 검증 + 이력 기록.
내부 lock 없음 — engine._lock이 보호.
"""
from __future__ import annotations

import time
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Set


class StockState(str, Enum):
    SCANNED = "SCANNED"
    WATCHING = "WATCHING"
    READY_TO_BUY = "READY_TO_BUY"
    BUY_PENDING = "BUY_PENDING"
    BOUGHT = "BOUGHT"
    SELL_PENDING = "SELL_PENDING"
    CLOSED = "CLOSED"
    SKIPPED = "SKIPPED"


# ── 허용 전이 (completeness: 모든 비정상 → SKIPPED) ──────────
VALID_TRANSITIONS: Dict[StockState, Set[StockState]] = {
    StockState.SCANNED:      {StockState.WATCHING, StockState.SKIPPED},
    StockState.WATCHING:     {StockState.READY_TO_BUY, StockState.BOUGHT, StockState.SKIPPED},
    StockState.READY_TO_BUY: {StockState.BUY_PENDING, StockState.SKIPPED},
    StockState.BUY_PENDING:  {StockState.BOUGHT, StockState.SKIPPED},
    StockState.BOUGHT:       {StockState.SELL_PENDING, StockState.CLOSED},
    StockState.SELL_PENDING: {StockState.CLOSED},
    StockState.CLOSED:       set(),   # terminal
    StockState.SKIPPED:      set(),   # terminal
}

TERMINAL_STATES = {StockState.CLOSED, StockState.SKIPPED}


@dataclass
class StateTransition:
    timestamp: float
    code: str
    from_state: Optional[str]
    to_state: str
    reason: str
    context: Optional[dict] = None


class StateTracker:
    """
    Per-stock state tracker.  NO internal lock — caller (engine) must hold lock.
    """

    def __init__(self):
        self._states: Dict[str, StockState] = {}
        self._transitions: List[StateTransition] = []
        self._cooldowns: Dict[str, float] = {}   # code → expiry ts

    # ── Query ─────────────────────────────────────────────

    def get(self, code: str) -> Optional[StockState]:
        return self._states.get(code)

    def get_codes_in_state(self, state: StockState) -> List[str]:
        return [c for c, s in self._states.items() if s == state]

    def is_cooled_down(self, code: str) -> bool:
        expiry = self._cooldowns.get(code, 0)
        return time.time() >= expiry

    # ── Mutate ────────────────────────────────────────────

    def transition(
        self,
        code: str,
        to_state: StockState,
        reason: str,
        context: Optional[dict] = None,
    ) -> bool:
        """
        Validate and apply state transition.
        Returns True if transition succeeded, False if rejected.
        """
        current = self._states.get(code)

        # New code: only SCANNED allowed as first state
        if current is None:
            if to_state != StockState.SCANNED:
                return False
            self._states[code] = to_state
            self._transitions.append(StateTransition(
                timestamp=time.time(),
                code=code,
                from_state=None,
                to_state=to_state.value,
                reason=reason,
                context=context,
            ))
            return True

        # Terminal state: no further transitions
        if current in TERMINAL_STATES:
            return False

        # Validate transition
        allowed = VALID_TRANSITIONS.get(current, set())
        if to_state not in allowed:
            return False

        self._states[code] = to_state
        self._transitions.append(StateTransition(
            timestamp=time.time(),
            code=code,
            from_state=current.value,
            to_state=to_state.value,
            reason=reason,
            context=context,
        ))
        return True

    def set_cooldown(self, code: str, seconds: float) -> None:
        self._cooldowns[code] = time.time() + seconds

    def reset(self) -> None:
        self._states.clear()
        self._transitions.clear()
        self._cooldowns.clear()

    # ── Serialization ─────────────────────────────────────

    def get_transitions(self, code: Optional[str] = None, limit: int = 100) -> List[dict]:
        items = self._transitions
        if code:
            items = [t for t in items if t.code == code]
        return [asdict(t) for t in items[-limit:]]

    def get_state_summary(self) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for s in self._states.values():
            counts[s.value] = counts.get(s.value, 0) + 1
        return counts
