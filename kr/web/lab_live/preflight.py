"""preflight.py — Lab Live import self-test.

Why this exists
---------------
On 2026-04-20 a pre-commit stash incident wiped `web/lab_live/market_context.py`
and `web/lab_live/daily_drivers.py`. Tray + uvicorn started cleanly because
neither module is imported at app boot — they are loaded lazily inside
`engine.daily_run()`. The first KR Lab EOD attempt (2026-04-21 15:36 KST)
raised `ModuleNotFoundError: No module named 'web.lab_live.market_context'`
inside `daily_run`, which surfaced only in the `[LAB_LIVE] Run error: ...`
log line. From the dashboard's perspective the system looked healthy.

The wipe stayed undetected for 6 days (4/21 → 4/26 23:52 KST commit
`d077b6fb fix(kr-lab): restore market_context + daily_drivers`). During
that window every Lab EOD silently no-op'd, which froze
`kr/data/lab_live/states/*.json` at the 2026-04-10 snapshot and produced
the "9 strategies @ +0.00%" chart Jeff hit on 2026-04-27.

Contract
--------
- Run `run_preflight()` once at FastAPI app startup.
- It imports each module in `REQUIRED_MODULES` in isolation and records
  per-module ok/error.
- The aggregate result is cached on the `application.state.lab_preflight`
  attribute and exposed via `/api/health` plus `/api/lab/live/preflight`.
- Endpoints that drive Lab EOD (`/api/lab/live/start`,
  `/api/lab/live/run-daily`) must consult `is_blocking_failure()` and
  refuse to run when imports are broken — the orchestrator's
  `lab_eod_kr` step will see the 503 and bail without trashing state.
- A single Telegram CRITICAL is fired per failed startup; subsequent
  duplicate alerts are suppressed via the cache.

This module is intentionally stdlib + `importlib` only. Loading it must
NOT pull in any of the modules under test — the whole point is to be the
canary that flags an Import error before it reaches a daily_run call.
"""
from __future__ import annotations

import importlib
import logging
import os
import threading
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("lab_live.preflight")

# ── Modules required for Lab Live EOD execution ────────────────
# Order is informational (failures are reported in this order); semantics
# don't depend on it. Keep this list synchronised with the actual import
# graph traversed by `engine.daily_run()` and its lazy imports.
REQUIRED_MODULES: List[str] = [
    "web.lab_live.state_store",
    "web.lab_live.engine",
    "web.lab_live.market_context",
    "web.lab_live.daily_drivers",
    "web.lab_live.meta_collector",
]

# Module-level cache so repeat calls (e.g. /api/health polling) don't
# re-import on every hit. Run-once-per-process is the right granularity:
# if a module truly disappeared between successful boot and now, the
# Python import system would not magically heal it without a process
# restart, so re-checking is wasted work.
_RESULT_LOCK = threading.Lock()
_RESULT_CACHE: Optional[Dict[str, Any]] = None
_TELEGRAM_FIRED = False


def _import_one(name: str) -> Optional[str]:
    """Return None on success, error summary string on failure."""
    try:
        importlib.import_module(name)
        return None
    except Exception as e:
        # Capture both the type and the first traceback line so the error
        # is actionable from the alert text alone (no need to dig logs).
        tb_last = traceback.format_exc().strip().splitlines()[-1] if e else ""
        return f"{type(e).__name__}: {e} ({tb_last})"


def run_preflight(*, fire_telegram: bool = True) -> Dict[str, Any]:
    """Run import self-test and cache the result.

    Returns dict with keys:
        ok            bool — all modules imported successfully
        ts            ISO timestamp of the check
        modules       {name: "OK" | "FAIL"}
        errors        {name: error_summary} for failures only
        missing       [name, ...] convenience alias for sorted(errors.keys())

    Subsequent calls return the cached result. Use `reset_cache()` to
    force a re-check (intended for tests; production resets via process
    restart).
    """
    global _RESULT_CACHE, _TELEGRAM_FIRED
    with _RESULT_LOCK:
        if _RESULT_CACHE is not None:
            return _RESULT_CACHE

        modules: Dict[str, str] = {}
        errors: Dict[str, str] = {}
        for name in REQUIRED_MODULES:
            err = _import_one(name)
            if err is None:
                modules[name] = "OK"
            else:
                modules[name] = "FAIL"
                errors[name] = err

        ok = not errors
        result: Dict[str, Any] = {
            "ok": ok,
            "ts": datetime.now().isoformat(timespec="seconds"),
            "modules": modules,
            "errors": errors,
            "missing": sorted(errors.keys()),
        }
        _RESULT_CACHE = result

        if ok:
            logger.info(
                f"[LAB_PREFLIGHT_OK] {len(REQUIRED_MODULES)} modules imported"
            )
        else:
            logger.error(
                f"[LAB_PREFLIGHT_FAIL] missing={sorted(errors.keys())} "
                f"errors={errors}"
            )
            if fire_telegram and not _TELEGRAM_FIRED:
                _TELEGRAM_FIRED = True
                _fire_telegram_critical(result)

        return result


def get_cached() -> Optional[Dict[str, Any]]:
    """Return cached preflight result without running. None if not yet run."""
    with _RESULT_LOCK:
        return _RESULT_CACHE


def is_blocking_failure() -> bool:
    """True iff preflight ran and at least one required module failed.

    Safe to call before run_preflight() — returns False (don't block on
    "unknown" — the boot path will populate the cache before any
    lab_eod_kr trigger). Endpoints that need a strict "must have run"
    check should test `get_cached() is None` separately.
    """
    cached = get_cached()
    if cached is None:
        return False
    return not cached.get("ok", True)


def reset_cache() -> None:
    """Test helper. Clears the cache so the next run_preflight() re-imports."""
    global _RESULT_CACHE, _TELEGRAM_FIRED
    with _RESULT_LOCK:
        _RESULT_CACHE = None
        _TELEGRAM_FIRED = False


def _fire_telegram_critical(result: Dict[str, Any]) -> None:
    """Send a single CRITICAL alert. Best-effort, never raises.

    Imports the notify path lazily so a broken `notify.telegram_bot` (or
    network down) does not turn a preflight FAIL into a preflight
    CRASH on import time.
    """
    try:
        from notify.telegram_bot import send  # noqa: WPS433 — lazy
        missing = ", ".join(result.get("missing") or []) or "(unknown)"
        first_err = ""
        if result.get("errors"):
            first_err = next(iter(result["errors"].values()))
        text = (
            "🚨 <b>KR Lab Live import preflight FAIL</b>\n"
            f"Time: {result.get('ts', '')}\n"
            f"Missing: {missing}\n"
            f"First error: {first_err[:200]}\n"
            "Lab EOD trigger has been disabled until tray restart."
        )
        send(text, severity="CRITICAL")
    except Exception as e:
        # Telegram failure is itself observability data — log it so the
        # operator at least sees the preflight FAIL in the local logs.
        logger.warning(f"[LAB_PREFLIGHT_TELEGRAM_FAIL] {e}")
        # Fallback: write a marker file so external tooling can detect
        # the unalerted failure even if notify.telegram_bot is the
        # broken module.
        try:
            from pathlib import Path
            marker_dir = Path(__file__).resolve().parent.parent.parent / "data" / "lab_live"
            marker_dir.mkdir(parents=True, exist_ok=True)
            marker = marker_dir / "preflight_fail.marker"
            marker.write_text(
                f"ts={result.get('ts')}\n"
                f"missing={result.get('missing')}\n"
                f"errors={result.get('errors')}\n",
                encoding="utf-8",
            )
        except Exception:
            pass


# ── Disable knob — operator override ──────────────────────────
# In rare cases (e.g. Jeff manually tested with one module renamed) the
# preflight may need to be silenced. Set QTRON_LAB_PREFLIGHT=0 in env.
def is_disabled() -> bool:
    return os.environ.get("QTRON_LAB_PREFLIGHT", "1").strip() in ("0", "false", "False")
