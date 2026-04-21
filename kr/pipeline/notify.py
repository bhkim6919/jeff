# -*- coding: utf-8 -*-
"""kr/pipeline/notify.py — Telegram transition notifier for Pipeline Orchestrator.

Stateless-ish wrapper that compares the current PipelineState to what it
last reported and emits a Telegram message on meaningful transitions:

    - step.status becomes DONE              → ✅ "<step> DONE" + details
    - step.status becomes FAILED *and*
      fail_count ≥ max_fails (abandoned)    → 🚨 "<step> ABANDONED" + error

Quiet steps (bootstrap_env, gate_observer) are suppressed by default —
bootstrap fires every orchestrator wake-up and gate_observer is often
missing (module_not_found SKIP) which would be noise.

Dedup: `_seen` set keyed by (trade_date, step_name, terminal_status).
Same key never re-emits. On trade_date rollover the set is cleared.

Telegram is best-effort via `notify.telegram_bot.send` — all exceptions
are swallowed so a network outage cannot break the orchestrator tick.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from .schema import STATUS_DONE, STATUS_FAILED
from .state import PipelineState, StepState

_log = logging.getLogger("gen4.pipeline.notify")

# Steps we never notify on — too chatty or too unreliable.
QUIET_STEPS = frozenset({"bootstrap_env", "gate_observer"})

# Per-step max_fails (mirrors StepBase.backoff_max_fails). Used to detect
# "abandoned" transitions. Kept local so notify.py doesn't import the step
# classes (avoids circular imports at tray load time).
_MAX_FAILS = {
    "batch": 3,
    "lab_eod_kr": 3,
    "lab_eod_us": 3,
    "backup": 2,
}


class PipelineNotifier:
    """Detects DONE / ABANDONED transitions and emits Telegram.

    Hold one instance for the lifetime of the tray process. Call
    `notify_transitions(state)` after each orchestrator tick. Duplicate
    calls for the same (date, step, status) are dropped.
    """

    def __init__(self, *, send_fn=None):
        """Create a notifier.

        Parameters
        ----------
        send_fn : callable(text: str, severity: str) -> bool
            Injection point for tests. Default: `notify.telegram_bot.send`.
        """
        self._send_fn = send_fn
        # (trade_date_iso, step_name, terminal_status) → True
        self._seen: set[tuple[str, str, str]] = set()
        self._current_date: Optional[date] = None

    # ---------- public ----------

    def notify_transitions(self, state: PipelineState) -> list[str]:
        """Scan state, emit Telegram for any unreported terminal transition.

        Returns the list of step names for which a notification was sent
        this call (empty list if nothing new).
        """
        sent: list[str] = []
        try:
            self._rollover_if_new_day(state.trade_date)
            date_iso = state.trade_date.isoformat()

            for step_name, step in state.steps.items():
                if step_name in QUIET_STEPS:
                    continue
                msg = self._format_if_newly_terminal(step_name, step, date_iso)
                if msg is None:
                    continue
                text, severity = msg
                if self._send(text, severity):
                    sent.append(step_name)
        except Exception as e:  # noqa: BLE001 — never break the tick
            _log.warning("[PIPELINE_NOTIFY_CRASH] %s", e)
        return sent

    # ---------- internals ----------

    def _rollover_if_new_day(self, today: date) -> None:
        if self._current_date is None:
            self._current_date = today
            return
        if today != self._current_date:
            _log.info(
                "[PIPELINE_NOTIFY_ROLLOVER] prev=%s new=%s — clearing dedup",
                self._current_date, today,
            )
            self._seen.clear()
            self._current_date = today

    def _format_if_newly_terminal(
        self,
        step_name: str,
        step: StepState,
        date_iso: str,
    ) -> Optional[tuple[str, str]]:
        """Return (text, severity) if this step just reached a notable terminal
        state that we haven't reported yet. None otherwise.
        """
        if step.status == STATUS_DONE:
            key = (date_iso, step_name, STATUS_DONE)
            if key in self._seen:
                return None
            self._seen.add(key)
            return (self._fmt_done(step_name, step, date_iso), "INFO")

        if step.status == STATUS_FAILED:
            max_f = _MAX_FAILS.get(step_name, 3)
            if step.fail_count < max_f:
                # Still retryable — don't page the user mid-retry.
                return None
            key = (date_iso, step_name, "ABANDONED")
            if key in self._seen:
                return None
            self._seen.add(key)
            return (
                self._fmt_abandoned(step_name, step, date_iso, max_f),
                "CRITICAL",
            )

        return None

    def _fmt_done(self, step_name: str, step: StepState, date_iso: str) -> str:
        d = step.details or {}
        head = f"<b>[PIPE] {step_name}</b> DONE\ntrade_date: {date_iso}"

        extras: list[str] = []
        if step_name == "batch":
            if "target_count" in d:
                extras.append(f"targets: {d['target_count']}")
            if "selected_source" in d:
                extras.append(f"source: {d['selected_source']}")
            if "duration_sec" in d:
                extras.append(f"dur: {d['duration_sec']}s")
        elif step_name == "lab_eod_kr":
            if "trades" in d:
                extras.append(f"trades: {d['trades']}")
            if "strategies" in d:
                extras.append(f"strategies: {d['strategies']}")
            if "duration_sec" in d:
                extras.append(f"dur: {d['duration_sec']}s")
        elif step_name == "lab_eod_us":
            if "strategy_count" in d:
                extras.append(f"strategies: {d['strategy_count']}")
            if "duration_sec" in d:
                extras.append(f"dur: {d['duration_sec']}s")
        elif step_name == "backup":
            if "summary_head" in d:
                extras.append(d["summary_head"][:120])
            if "duration_sec" in d:
                extras.append(f"dur: {d['duration_sec']}s")

        if extras:
            return head + "\n" + " · ".join(extras)
        return head

    def _fmt_abandoned(
        self,
        step_name: str,
        step: StepState,
        date_iso: str,
        max_fails: int,
    ) -> str:
        err = (step.last_error or "unknown")
        if len(err) > 300:
            err = err[:300] + "…"
        return (
            f"<b>[PIPE] {step_name}</b> ABANDONED 🚨\n"
            f"trade_date: {date_iso}\n"
            f"{step.fail_count}/{max_fails} attempts exhausted\n"
            f"last_error: {err}\n"
            f"→ 수동 복구 필요"
        )

    def _send(self, text: str, severity: str) -> bool:
        try:
            fn = self._send_fn
            if fn is None:
                # Lazy import — avoids hard dep at pipeline load time.
                from notify.telegram_bot import send as _tg_send
                fn = _tg_send
            ok = fn(text, severity)
            if ok:
                _log.info(
                    "[PIPELINE_NOTIFY_SENT] sev=%s head=%s",
                    severity, text.split("\n")[0][:80],
                )
            else:
                _log.warning("[PIPELINE_NOTIFY_DROPPED] head=%s",
                             text.split("\n")[0][:80])
            return bool(ok)
        except Exception as e:  # noqa: BLE001
            _log.warning("[PIPELINE_NOTIFY_SEND_CRASH] %s", e)
            return False


# ---------- module-level singleton (tray usage pattern) ----------

_HOLDER: Optional[PipelineNotifier] = None


def get_notifier() -> PipelineNotifier:
    """Lazy global notifier — mirrors tray_integration.HOLDER pattern."""
    global _HOLDER
    if _HOLDER is None:
        _HOLDER = PipelineNotifier()
    return _HOLDER
