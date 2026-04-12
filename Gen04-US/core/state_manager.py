# -*- coding: utf-8 -*-
"""
state_manager.py — Atomic State Persistence for Q-TRON US
==========================================================
- Paired save: portfolio + runtime share saved_at + version_seq
- Atomic write: tmp → read-back verify → bak → rename
- Dirty exit detection via runtime started_at / shutdown_at
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple, List
from zoneinfo import ZoneInfo

logger = logging.getLogger("qtron.us.state")

US_ET = ZoneInfo("US/Eastern")

# ── Rebalance Phase State Machine ───────────────────────────
REBAL_PHASES = (
    "IDLE", "BATCH_RUNNING", "BATCH_DONE", "DUE",
    "EXECUTING", "EXECUTED", "PARTIAL_EXECUTED", "FAILED", "BLOCKED",
)

_VALID_TRANSITIONS = {
    "IDLE":              ["BATCH_RUNNING"],
    "BATCH_RUNNING":     ["BATCH_DONE", "FAILED"],
    "BATCH_DONE":        ["DUE", "IDLE"],
    "DUE":               ["EXECUTING", "BLOCKED"],
    "EXECUTING":         ["EXECUTED", "PARTIAL_EXECUTED", "FAILED"],
    "EXECUTED":          ["IDLE", "BATCH_RUNNING"],
    "PARTIAL_EXECUTED":  ["IDLE", "BATCH_RUNNING"],
    "FAILED":            ["IDLE", "BATCH_RUNNING"],
    "BLOCKED":           ["IDLE", "DUE"],
}

_REBAL_DEFAULTS = {
    "rebal_mode": "manual",
    "rebal_phase": "IDLE",
    "last_rebalance_date": "",
    "next_rebalance_date": "",
    "last_execute_request_id": "",
    "last_execute_business_date": "",
    "last_execute_result": "",
    "last_execute_snapshot_version": "",
    "last_batch_business_date": "",
    "batch_fresh": False,
    "snapshot_version": "",
    "snapshot_created_at": "",
    "execute_lock": False,
    "execute_lock_acquired_at": "",
    "execute_lock_owner": "",
}

MAX_STALENESS_HOURS = 12
LOCK_TIMEOUT_MINUTES = 10


# ── Business Date (단일 기준 함수) ──────────────────────────

def get_business_date_et(provider=None) -> str:
    """
    US/Eastern 기준 현재 영업일. 모든 곳에서 이 함수만 사용.
    - 1차: Alpaca calendar (provider)
    - 2차: fallback → "어제" 기준 (holiday 오판 방지)
    """
    # 1차: provider
    try:
        if provider and hasattr(provider, "get_calendar"):
            from datetime import date as _date
            today = datetime.now(US_ET).date()
            cal = provider.get_calendar(
                start=(today - timedelta(days=7)).isoformat(),
                end=today.isoformat(),
            )
            if cal:
                # cal is list of trading days, get the most recent
                trading_days = [d for d in cal if d <= today.isoformat()]
                if trading_days:
                    bd = trading_days[-1]
                    logger.debug(f"[BUSINESS_DATE_PROVIDER] {bd}")
                    return bd if isinstance(bd, str) else str(bd)
    except Exception as e:
        logger.warning(f"[BUSINESS_DATE_PROVIDER_FAIL] {e}")

    # 2차: fallback — "어제" (holiday 오판 방지, today 반환 금지)
    et_now = datetime.now(US_ET)
    wd = et_now.weekday()
    if wd == 0:    # Mon → Fri
        fallback = (et_now - timedelta(days=3)).date()
    elif wd == 6:  # Sun → Fri
        fallback = (et_now - timedelta(days=2)).date()
    elif wd == 5:  # Sat → Fri
        fallback = (et_now - timedelta(days=1)).date()
    else:          # Tue–Fri → yesterday
        fallback = (et_now - timedelta(days=1)).date()

    result = fallback.strftime("%Y-%m-%d")
    logger.warning(f"[BUSINESS_DATE_FALLBACK] {result}")
    return result


def compute_batch_fresh(rs: dict, today_bd: str) -> bool:
    """batch_fresh = 데이터 기준 (date + snapshot + phase + staleness)."""
    if rs.get("last_batch_business_date", "") != today_bd:
        return False
    if not rs.get("snapshot_version", ""):
        return False
    if rs.get("rebal_phase", "IDLE") not in (
        "BATCH_DONE", "DUE", "EXECUTED", "PARTIAL_EXECUTED"
    ):
        return False
    created_at = rs.get("snapshot_created_at", "")
    if not created_at:
        return False
    try:
        et_now = datetime.now(US_ET)
        created = datetime.fromisoformat(created_at)
        if created.tzinfo is None:
            created = created.replace(tzinfo=US_ET)
        age = (et_now - created).total_seconds()
        return age < MAX_STALENESS_HOURS * 3600
    except Exception:
        return False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class StateManagerUS:
    """
    Manages portfolio_state_us_{mode}.json and runtime_state_us_{mode}.json.

    All saves go through save_all() to keep both files at the same
    saved_at + version_seq — no partial snapshots.
    """

    def __init__(self, state_dir: str | Path, trading_mode: str = "paper"):
        self._state_dir = Path(state_dir)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._mode = trading_mode

        self._portfolio_path = self._state_dir / f"portfolio_state_us_{trading_mode}.json"
        self._runtime_path = self._state_dir / f"runtime_state_us_{trading_mode}.json"

        self._lock = threading.RLock()
        self._version_seq = 0

        # Load existing seq if present
        rt = self._load_json(self._runtime_path)
        if rt:
            self._version_seq = rt.get("version_seq", 0)

    # ── Atomic Write ────────────────────────────────────────

    def _load_json(self, path: Path) -> Optional[dict]:
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"[STATE] Load failed {path.name}: {e}")
            return None

    def _atomic_write(self, path: Path, data: dict) -> bool:
        """tmp → read-back verify → bak → rename"""
        tmp_path = path.with_suffix(".tmp")
        bak_path = path.with_suffix(".bak")

        try:
            # 1. Write to tmp
            content = json.dumps(data, ensure_ascii=False, indent=2, default=str)
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(content)

            # 2. Read-back verify
            with open(tmp_path, "r", encoding="utf-8") as f:
                verify = json.load(f)
            if verify.get("version_seq") != data.get("version_seq"):
                logger.error(f"[STATE] Verify failed: seq mismatch in {path.name}")
                return False

            # 3. Backup existing
            if path.exists():
                shutil.copy2(path, bak_path)

            # 4. Rename (atomic on most OS)
            os.replace(str(tmp_path), str(path))
            return True

        except Exception as e:
            logger.error(f"[STATE] Atomic write failed {path.name}: {e}")
            # Cleanup tmp
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return False

    # ── Paired Save ─────────────────────────────────────────

    def _next_seq(self) -> int:
        self._version_seq += 1
        return self._version_seq

    def save_all(self, portfolio_data: dict, runtime_data: dict) -> bool:
        """Save portfolio + runtime with shared saved_at and version_seq.
        Preserves rebal fields written by batch process."""
        with self._lock:
            # Merge: preserve rebal fields from disk (batch writes directly)
            existing_rt = self._load_json(self._runtime_path) or {}
            for key in _REBAL_DEFAULTS:
                if key in existing_rt and key not in runtime_data:
                    runtime_data[key] = existing_rt[key]

            ts = _now_iso()
            seq = self._next_seq()

            portfolio_data["saved_at"] = ts
            portfolio_data["version_seq"] = seq
            runtime_data["saved_at"] = ts
            runtime_data["version_seq"] = seq

            ok_p = self._atomic_write(self._portfolio_path, portfolio_data)
            ok_r = self._atomic_write(self._runtime_path, runtime_data)

            if ok_p and ok_r:
                logger.debug(f"[STATE] save_all seq={seq}")
            else:
                logger.error(f"[STATE] save_all partial failure: portfolio={ok_p} runtime={ok_r}")

            return ok_p and ok_r

    # ── Load ────────────────────────────────────────────────

    def load_portfolio(self) -> Optional[dict]:
        with self._lock:
            return self._load_json(self._portfolio_path)

    def load_runtime(self) -> Optional[dict]:
        with self._lock:
            return self._load_json(self._runtime_path)

    # ── Rebalance Date ──────────────────────────────────────

    def get_last_rebalance_date(self) -> str:
        rt = self.load_runtime()
        if rt:
            return rt.get("last_rebalance_date", "")
        return ""

    def set_last_rebalance_date(self, date_str: str) -> None:
        # Will be saved via save_all in the next cycle
        pass  # Caller updates runtime_data dict directly

    # ── Lifecycle ───────────────────────────────────────────

    def mark_startup(self) -> dict:
        """Record startup in runtime. Returns runtime_data for save_all."""
        return {
            "started_at": _now_iso(),
            "pid": os.getpid(),
            "shutdown_at": "",
            "shutdown_reason": "",
            "mode": self._mode,
        }

    def mark_shutdown(self, reason: str = "normal") -> dict:
        """Record shutdown in runtime. Returns runtime_data for save_all."""
        rt = self.load_runtime() or {}
        rt["shutdown_at"] = _now_iso()
        rt["shutdown_reason"] = reason
        return rt

    def was_dirty_exit(self) -> bool:
        """True if started_at exists but shutdown_at is missing."""
        rt = self.load_runtime()
        if not rt:
            return False
        started = rt.get("started_at", "")
        shutdown = rt.get("shutdown_at", "")
        return bool(started and not shutdown)

    # ── Rebalance State ────────────────────────────────────

    def get_rebal_state(self) -> dict:
        """Extract rebal fields from runtime_state (defaults for missing)."""
        rt = self.load_runtime() or {}
        return {k: rt.get(k, v) for k, v in _REBAL_DEFAULTS.items()}

    def update_rebal_state(self, updates: dict) -> bool:
        """Atomic merge rebal fields into runtime_state + save."""
        with self._lock:
            rt = self.load_runtime() or {}
            for k, v in updates.items():
                if k in _REBAL_DEFAULTS or k in ("saved_at", "version_seq"):
                    rt[k] = v
            ts = _now_iso()
            seq = self._next_seq()
            rt["saved_at"] = ts
            rt["version_seq"] = seq
            ok = self._atomic_write(self._runtime_path, rt)
            if ok:
                logger.debug(f"[STATE] update_rebal_state seq={seq} keys={list(updates.keys())}")
            else:
                logger.error(f"[STATE] update_rebal_state failed")
            return ok

    def transition_phase(self, new_phase: str) -> Tuple[bool, str]:
        """Transition rebal_phase with validation. Returns (ok, reason)."""
        with self._lock:
            rt = self.load_runtime() or {}
            current = rt.get("rebal_phase", "IDLE")
            allowed = _VALID_TRANSITIONS.get(current, [])
            if new_phase not in allowed:
                reason = f"Invalid transition: {current} → {new_phase} (allowed: {allowed})"
                logger.warning(f"[US_REBAL_PHASE] REJECTED: {reason}")
                return False, reason
            rt["rebal_phase"] = new_phase
            ts = _now_iso()
            seq = self._next_seq()
            rt["saved_at"] = ts
            rt["version_seq"] = seq
            ok = self._atomic_write(self._runtime_path, rt)
            if ok:
                logger.info(f"[US_REBAL_PHASE] {current} → {new_phase}")
            return ok, ""

    def clear_stale_execute_lock(self) -> bool:
        """Clear execute_lock if stale (> LOCK_TIMEOUT_MINUTES). Returns True if cleared."""
        with self._lock:
            rt = self.load_runtime() or {}
            if not rt.get("execute_lock", False):
                return False
            acquired = rt.get("execute_lock_acquired_at", "")
            if not acquired:
                # Lock without timestamp — clear it
                rt["execute_lock"] = False
                rt["execute_lock_owner"] = ""
                rt["execute_lock_acquired_at"] = ""
                self._atomic_write(self._runtime_path, rt)
                logger.warning("[US_REBAL_LOCK_STALE_CLEAR] no timestamp")
                return True
            try:
                acq_time = datetime.fromisoformat(acquired)
                if acq_time.tzinfo is None:
                    acq_time = acq_time.replace(tzinfo=US_ET)
                age_min = (datetime.now(US_ET) - acq_time).total_seconds() / 60
                if age_min > LOCK_TIMEOUT_MINUTES:
                    owner = rt.get("execute_lock_owner", "?")
                    rt["execute_lock"] = False
                    rt["execute_lock_owner"] = ""
                    rt["execute_lock_acquired_at"] = ""
                    if rt.get("rebal_phase") == "EXECUTING":
                        rt["rebal_phase"] = "FAILED"
                    self._atomic_write(self._runtime_path, rt)
                    logger.warning(
                        f"[US_REBAL_LOCK_STALE_CLEAR] age={age_min:.1f}min owner={owner}"
                    )
                    return True
            except Exception as e:
                logger.error(f"[US_REBAL_LOCK_STALE_CLEAR] parse error: {e}")
            return False

    def compute_execute_allowed(
        self, provider=None, config=None
    ) -> Tuple[bool, List[str]]:
        """
        Fresh state load + full condition check.
        Returns (allowed, block_reasons) — severity sorted.
        """
        rs = self.get_rebal_state()
        today_bd = get_business_date_et(provider)
        blocks: List[str] = []

        # 1. Lock
        if rs.get("execute_lock", False):
            blocks.append("EXECUTE_LOCKED")

        # 2. Phase
        if rs.get("rebal_phase") == "EXECUTING":
            blocks.append("ALREADY_EXECUTING")

        # 3. Batch fresh
        if not compute_batch_fresh(rs, today_bd):
            blocks.append("BATCH_NOT_FRESH")

        # 4. Same business date
        if rs.get("last_execute_business_date", "") == today_bd:
            blocks.append("ALREADY_EXECUTED_TODAY")

        # 5. Same snapshot
        sv = rs.get("snapshot_version", "")
        if sv and sv == rs.get("last_execute_snapshot_version", ""):
            blocks.append("SAME_SNAPSHOT")

        # 6. D-day / rebal_due
        next_rd = rs.get("next_rebalance_date", "")
        if next_rd and today_bd < next_rd:
            blocks.append(f"NOT_DUE (next={next_rd})")

        # 7. Buy permission
        if provider and config:
            try:
                from strategy.execution_gate import check_buy_permission
                rt_full = self.load_runtime() or {}
                allowed, reason, scale = check_buy_permission(config, rt_full, provider)
                if not allowed:
                    blocks.append(f"BUY_BLOCKED: {reason}")
            except Exception as e:
                blocks.append(f"BUY_CHECK_ERROR: {e}")

        # 8. Open orders
        if provider:
            try:
                oo = provider.query_open_orders()
                if oo:
                    blocks.append(f"OPEN_ORDERS: {len(oo)}")
            except Exception:
                pass

        return (len(blocks) == 0, blocks)

    # ── Paths (for external reference) ──────────────────────

    @property
    def portfolio_path(self) -> Path:
        return self._portfolio_path

    @property
    def runtime_path(self) -> Path:
        return self._runtime_path
