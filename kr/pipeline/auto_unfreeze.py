"""kr/pipeline/auto_unfreeze.py — Item 3 (2026-04-30 RCA).

ABANDONED-state auto-unfreeze for the pipeline orchestrator.

Problem fixed
-------------
On 2026-04-30 the KR_BATCH preflight failed 3 times in 4 minutes
because ``backtest/data_full/ohlcv/`` was missing. The step transitioned
to ABANDONED. About 50 minutes later an operator manually restored the
directory, but ABANDONED state is sticky: ``BackoffTracker.can_run_now``
returns ``(False, "abandoned")`` once ``fail_count >= max_fails``, so
the orchestrator never retried even though the underlying cause was
fully resolved. KR_EOD (16:05) was missed as a downstream consequence.

Behavior
--------
On each orchestrator tick, BEFORE the backoff/abandoned gate, this
module:

  1. Inspects the step's ``last_error``.
  2. Classifies it as RECOVERABLE (data/file missing — fixable by
     refreshing OHLCV / KOSPI / similar inputs) or NON-RECOVERABLE
     (import / permission / config / logic error — would re-fail).
  3. For RECOVERABLE errors: runs a fast health probe. If healthy
     NOW, calls ``BackoffTracker.reset(state)`` to clear fail_count
     and last_failed_at, allowing the step to re-enter the backoff
     gate cleanly on the same tick.
  4. Single-shot per (trade_date, step_name) — persisted via marker
     file at ``kr/data/pipeline/unfreeze_{date}_{step}.json`` so that
     a re-failure inside the same trade_date does not loop with
     repeated unfreeze→retry→fail→unfreeze.

NEVER auto-unfreezes:
  * import failures, ModuleNotFound — code/env error
  * PermissionError, access denied — filesystem rights error
  * config_init / config_import — bad config
  * universe_builder build_crash — logic error
  * any error containing "syntax" / "TypeError" — code bug

Public API
----------
``maybe_unfreeze(step, state, *, logger=None) -> bool``
    True iff the step was just unfrozen on this call.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

_log = logging.getLogger("gen4.pipeline.auto_unfreeze")

# Tokens (case-insensitive) that mark a fail as data-recoverable.
# Both the structured `[reason:CODE]` prefix AND legacy unstructured
# substrings are accepted so historical errors are still classifiable.
RECOVERABLE_TOKENS: tuple[str, ...] = (
    "[reason:dir_missing]",
    "[reason:csv_count_low]",
    "[reason:history_sample_low]",
    "[reason:universe_size_low]",
    "preflight_blocked",
    "universe_healthy",
    "ohlcv dir missing",
    "csv count",
    "history sample",
    "universe too small",
)

# Tokens that hard-block auto-unfreeze. Checked FIRST — any match wins
# even if a recoverable token also appears (defensive: avoids unfreezing
# composite errors like "import failed during preflight_blocked").
NON_RECOVERABLE_TOKENS: tuple[str, ...] = (
    "import failed",
    "import error",
    "modulenotfound",
    "permissionerror",
    "permission denied",
    "access denied",
    "config_init",
    "config_import",
    "build_crash",
    "syntaxerror",
    "typeerror",
    "[reason:import_failed]",
    "[reason:build_crash]",
    "[reason:config_import]",
    "[reason:config_init]",
)

# Fast OHLCV probe threshold — mirrors preflight `MIN_CSV_COUNT`.
OHLCV_MIN_CSV_COUNT = 2500


# ── Classifier ───────────────────────────────────────────────────────


def is_data_recoverable(last_error: Optional[str]) -> bool:
    """True iff the persisted ``last_error`` indicates a data/file failure
    that a refresh of inputs could fix.

    Returns False when:
      * last_error is None / empty
      * any NON_RECOVERABLE token matches (hard reject, even if a
        recoverable token also appears)
      * no RECOVERABLE token matches
    """
    if not last_error:
        return False
    err = last_error.lower()
    if any(tok in err for tok in NON_RECOVERABLE_TOKENS):
        return False
    return any(tok in err for tok in RECOVERABLE_TOKENS)


# ── Health probe ─────────────────────────────────────────────────────


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def ohlcv_health_pass() -> tuple[bool, dict]:
    """Fast health check (<1s). Existence + CSV count threshold."""
    ohlcv_dir = _repo_root() / "backtest" / "data_full" / "ohlcv"
    if not ohlcv_dir.exists():
        return False, {"reason": "dir_missing", "ohlcv_dir": str(ohlcv_dir)}
    if not ohlcv_dir.is_dir():
        return False, {"reason": "not_a_dir", "ohlcv_dir": str(ohlcv_dir)}
    try:
        count = sum(1 for _ in ohlcv_dir.glob("*.csv"))
    except OSError as e:
        return False, {"reason": "glob_failed", "err": repr(e)}
    if count < OHLCV_MIN_CSV_COUNT:
        return False, {
            "reason": "csv_count_low",
            "csv_count": count,
            "min": OHLCV_MIN_CSV_COUNT,
        }
    return True, {"csv_count": count, "ohlcv_dir": str(ohlcv_dir)}


# ── Single-shot marker ───────────────────────────────────────────────


def _marker_path(step_name: str, trade_date: str) -> Path:
    return _repo_root() / "kr" / "data" / "pipeline" / (
        f"unfreeze_{trade_date}_{step_name}.json"
    )


def _marker_exists(step_name: str, trade_date: str) -> bool:
    return _marker_path(step_name, trade_date).exists()


def _persist_marker(
    step_name: str, trade_date: str, prev_error: Optional[str],
    probe: dict,
) -> None:
    path = _marker_path(step_name, trade_date)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "step": step_name,
            "trade_date": trade_date,
            "unfreeze_at": datetime.now().isoformat(),
            "prev_error": (prev_error or "")[:500],
            "probe": probe,
        }, indent=2, default=str), encoding="utf-8")
    except OSError as e:
        _log.warning(f"[PIPELINE_UNFREEZE_MARKER_WRITE_FAIL] err={e!r}")


# ── Public API ───────────────────────────────────────────────────────


def maybe_unfreeze(
    step: Any, state: Any, *, logger: Optional[logging.Logger] = None,
) -> bool:
    """Unfreeze ``step`` if it is ABANDONED for a data-recoverable reason
    AND the data is healthy NOW.

    Returns True iff fail_count was reset on this call.

    Caller invariant: ``step._tracker`` is the ``BackoffTracker`` instance
    used during the prior failures, with ``reset(state)`` available.

    Single-shot per (step_name, trade_date) — once unfrozen, subsequent
    calls return False even if the step re-fails.
    """
    log = logger or _log

    try:
        step_state = state.step(step.name)
    except Exception as e:  # noqa: BLE001
        log.warning(f"[PIPELINE_UNFREEZE_STATE_LOOKUP_FAIL] step={step.name} {e!r}")
        return False

    # Cheap eligibility — only ABANDONED steps qualify.
    max_fails = getattr(step._tracker, "max_fails", 3)
    if step_state.fail_count < max_fails:
        return False

    # Classify last_error.
    last_err = step_state.last_error
    if not is_data_recoverable(last_err):
        return False

    # Single-shot guard.
    trade_date = getattr(state, "trade_date", None) or datetime.now().strftime("%Y-%m-%d")
    if _marker_exists(step.name, trade_date):
        # Already unfrozen this trade_date — do not loop.
        return False

    # Health probe.
    healthy, probe_detail = ohlcv_health_pass()
    if not healthy:
        log.info(
            f"[PIPELINE_UNFREEZE_PROBE_FAIL] step={step.name} "
            f"trade_date={trade_date} probe={probe_detail}"
        )
        return False

    # Apply unfreeze.
    try:
        step._tracker.reset(state)
    except Exception as e:  # noqa: BLE001
        log.error(f"[PIPELINE_UNFREEZE_RESET_FAIL] step={step.name} {e!r}")
        return False

    _persist_marker(step.name, trade_date, last_err, probe_detail)
    log.warning(
        f"[PIPELINE_UNFREEZE] step={step.name} trade_date={trade_date} "
        f"prev_fail_count={step_state.fail_count} "
        f"prev_error={(last_err or '')[:200]!r} "
        f"probe={probe_detail}"
    )
    return True


# ── Test helpers ─────────────────────────────────────────────────────


def reset_for_test(step_name: Optional[str] = None,
                    trade_date: Optional[str] = None) -> None:
    """Remove unfreeze markers so tests can re-attempt unfreezing.

    With no args, removes ALL unfreeze markers.
    """
    pipeline_dir = _repo_root() / "kr" / "data" / "pipeline"
    if not pipeline_dir.exists():
        return
    if step_name and trade_date:
        targets = [_marker_path(step_name, trade_date)]
    else:
        targets = list(pipeline_dir.glob("unfreeze_*.json"))
    for p in targets:
        try:
            p.unlink()
        except OSError:
            pass
