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
    "last_batch_post_close": False,        # P0-001 marker — set by batch path
    "batch_fresh": False,
    "snapshot_version": "",
    "snapshot_created_at": "",
    "execute_lock": False,
    "execute_lock_acquired_at": "",
    "execute_lock_owner": "",
    # Attempt tracking: snapshot 기반 idempotency
    "last_rebal_attempt_snapshot": "",     # 마지막 시도한 snapshot_version
    "last_rebal_attempt_at": "",           # 시도 시각 (ISO)
    "last_rebal_attempt_result": "",       # SUCCESS / PARTIAL / FAILED / REJECTED
    "last_rebal_attempt_count": 0,         # 같은 snapshot 내 실행 횟수 (REJECTED 미포함)
    "last_rebal_attempt_reason": "",       # reject/fail 사유
}

# Staleness ceiling for ``compute_batch_fresh`` (Jeff 2026-04-29
# escalation — third day in a row showing BATCH_NOT_FRESH between
# the post-close batch and the next morning's market open).
#
# Background:
#   * US batch runs after 16:00 ET (post-close), typically
#     completing 16:00~20:00 ET.
#   * The previous 12h ceiling marked the snapshot stale halfway
#     through the next trading day — the dashboard's
#     ``BATCH_NOT_FRESH`` banner appeared daily from KST ~17:00
#     until the next batch finished (~KST 09:00 next day), about
#     12h of false-positive every cycle.
#   * Operators saw "Failed: Batch not fresh" daily for 3 days even
#     though the batch itself was running and completing on schedule.
#
# 26h covers a full trading-day cycle plus a 2h buffer:
#   batch finishes ET 20:00 (worst case)
#       → KST 09:00 next day, age 13h  → FRESH ✓
#       → next-day market close ET 16:00, age 20h  → FRESH ✓
#       → next-day batch finish ET 20:00, age 24h
#       → 26h gives a 2h grace before the next batch arrives
#
# A snapshot older than 26h is genuinely abandoned (the next batch
# never ran), so the gate keeps protecting against silently-stale
# state — just with a window that matches the US batch cadence
# instead of cutting it in half.
MAX_STALENESS_HOURS = 26
LOCK_TIMEOUT_MINUTES = 10


# ── Business Date (개념별 분리: last_closed vs current) ───────
# US-P0-002: 기존 get_business_date_et는 "가장 최근 종가일"과 "달력상 오늘"을
# 혼재 처리했음. 이제 두 개념을 별도 함수로 분리:
#   - get_last_closed_trading_day(): 종가 확정된 가장 최근 거래일 (execute gate)
#   - get_current_trading_day():     오늘이 거래일이면 오늘, 아니면 마지막 거래일 (batch)
# get_business_date_et는 하위호환 유지 — get_last_closed_trading_day 별칭.

# 미국 주식 장 마감: 16:00 ET (DST/EST 모두 동일 wall-clock)
MARKET_CLOSE_HOUR_ET = 16


def get_last_closed_trading_day(provider=None) -> str:
    """종가 확정된 가장 최근 거래일 (execute gate / compute_batch_fresh 기준).
    - 1차: Alpaca calendar (provider). 오늘이 거래일인데 16:00 ET 이전이면 전 거래일 반환.
    - 2차: 요일 기반 fallback (holiday 오판 방지, today 반환 금지).
    """
    et_now = datetime.now(US_ET)
    try:
        if provider and hasattr(provider, "get_calendar"):
            today = et_now.date()
            cal = provider.get_calendar(
                start=(today - timedelta(days=7)).isoformat(),
                end=today.isoformat(),
            )
            if cal:
                trading_days = [d for d in cal if d <= today.isoformat()]
                if trading_days:
                    bd = trading_days[-1]
                    bd_str = bd if isinstance(bd, str) else str(bd)
                    # 오늘이 거래일인데 아직 16:00 ET 이전이면 직전 거래일 반환
                    if bd_str == today.isoformat() and et_now.hour < MARKET_CLOSE_HOUR_ET:
                        prev = [d for d in trading_days if (d if isinstance(d, str) else str(d)) != bd_str]
                        if prev:
                            bd_str = prev[-1] if isinstance(prev[-1], str) else str(prev[-1])
                    logger.debug(f"[LAST_CLOSED_TRADING_DAY_PROVIDER] {bd_str}")
                    return bd_str
    except Exception as e:
        logger.warning(f"[LAST_CLOSED_TRADING_DAY_PROVIDER_FAIL] {e}")

    # 2차: fallback — 주말/평일에 따라 가장 가까운 평일 (today 반환 금지)
    wd = et_now.weekday()
    if wd == 0:    # Mon → Fri
        fallback = (et_now - timedelta(days=3)).date()
    elif wd == 6:  # Sun → Fri
        fallback = (et_now - timedelta(days=2)).date()
    elif wd == 5:  # Sat → Fri
        fallback = (et_now - timedelta(days=1)).date()
    else:          # Tue–Fri
        # 장 마감 전이면 전일, 장 마감 후면 오늘(현 평일)
        if et_now.hour >= MARKET_CLOSE_HOUR_ET:
            fallback = et_now.date()
        else:
            fallback = (et_now - timedelta(days=1)).date()

    result = fallback.strftime("%Y-%m-%d")
    logger.warning(f"[LAST_CLOSED_TRADING_DAY_FALLBACK] {result}")
    return result


def get_current_trading_day(provider=None) -> str:
    """달력상 오늘이 거래일이면 오늘, 아니면 가장 가까운 과거 거래일.
    - batch 시작 시 "이 배치가 다루는 거래일" 라벨로 사용.
    - 장 마감 전이어도 오늘을 반환 (pre-market 배치는 따로 marker 로 표기).
    """
    et_now = datetime.now(US_ET)
    today = et_now.date()
    try:
        if provider and hasattr(provider, "get_calendar"):
            cal = provider.get_calendar(
                start=(today - timedelta(days=7)).isoformat(),
                end=today.isoformat(),
            )
            if cal:
                trading_days = [d for d in cal if d <= today.isoformat()]
                if trading_days:
                    bd = trading_days[-1]
                    bd_str = bd if isinstance(bd, str) else str(bd)
                    return bd_str
    except Exception as e:
        logger.warning(f"[CURRENT_TRADING_DAY_PROVIDER_FAIL] {e}")

    wd = today.weekday()
    if wd >= 5:  # 주말 → 금요일
        fallback = today - timedelta(days=(wd - 4))
    else:
        fallback = today
    return fallback.strftime("%Y-%m-%d")


# 하위호환: 기존 호출처는 "종가 확정" semantics를 원하는 경우가 많음
def get_business_date_et(provider=None) -> str:
    """DEPRECATED alias — prefer get_last_closed_trading_day().
    기존 호출처와의 하위 호환 유지."""
    return get_last_closed_trading_day(provider)


def is_post_market_close(now_et: Optional[datetime] = None) -> bool:
    """현재 ET 시각이 당일 장 마감(16:00) 이후인가 (평일만)."""
    if now_et is None:
        now_et = datetime.now(US_ET)
    if now_et.weekday() >= 5:
        return False
    return now_et.hour >= MARKET_CLOSE_HOUR_ET


def compute_batch_fresh(rs: dict, today_bd: str) -> bool:
    """batch_fresh = 데이터 기준 (date + snapshot + phase + staleness + post-close).

    US-P0-001: pre-market 배치(04:54 ET 등)는 stale data 사용이므로 fresh 아님.
    snapshot_created_at이 business_date의 16:00 ET 이후인 경우만 fresh.
    """
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
        created_et = created.astimezone(US_ET)

        # Staleness gate
        age = (et_now - created).total_seconds()
        if age >= MAX_STALENESS_HOURS * 3600:
            logger.warning(
                f"[BATCH_FRESH_STALE] created={created_et.isoformat()} "
                f"age={age / 3600:.1f}h >= {MAX_STALENESS_HOURS}h")
            return False

        # US-P0-001: post-close gate — created_at must be >= 16:00 ET of today_bd.
        try:
            y, m, d = [int(x) for x in today_bd.split("-")]
            close_et = datetime(y, m, d, MARKET_CLOSE_HOUR_ET, 0, 0, tzinfo=US_ET)
            if created_et < close_et:
                logger.warning(
                    f"[BATCH_FRESH_PRE_MARKET] created={created_et.isoformat()} "
                    f"< close={close_et.isoformat()} — pre-market batch, "
                    f"treat as not-fresh")
                return False
        except Exception as _e:
            logger.warning(f"[BATCH_FRESH_CLOSE_PARSE_FAIL] {_e} — accepting by age only")

        return True
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

        Rebal fields are **disk-wins**: the web rebalance executor writes
        ``last_execute_*`` / ``rebal_phase`` / ``last_rebalance_date`` to
        disk via ``transition_phase_with_updates``, but the main loop's
        ``runtime_data`` dict is initialised from disk at process start
        and never re-reads. Without this merge the periodic save_all
        would clobber freshly-written rebal results with the stale
        in-memory copy, making the engine "forget" today's executed
        rebalance after the next save tick (Jeff 2026-04-29 — caused
        ``BATCH_DONE`` regression after restart, ``Execute: Ready``
        ready to fire a duplicate rebalance against the same broker).

        The previous merge only copied disk → memory when the key was
        *missing* from ``runtime_data``. Since ``runtime_data`` is
        always seeded from disk at startup, the keys are always
        present, so the disk write was never honoured.

        ``main.py`` never writes any field in ``_REBAL_DEFAULTS``, so
        unconditionally preferring disk is safe — there is no
        memory-side update to lose.
        """
        with self._lock:
            existing_rt = self._load_json(self._runtime_path) or {}
            for key in _REBAL_DEFAULTS:
                if key in existing_rt:
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

    def normalize_batch_state_at_startup(self, provider=None) -> bool:
        """P1-3: Force batch_fresh=False if snapshot_created_at predates the
        batch's own business_date 16:00 ET close. Prevents pre-market batches
        from lingering as "fresh" after a restart. Also forces False if the
        stored last_batch_post_close marker is False.

        Returns True if state was modified.
        """
        with self._lock:
            rt = self.load_runtime() or {}
            if not rt.get("batch_fresh", False):
                return False  # already not-fresh; nothing to do

            # Direct marker (written by us/main.py batch path): pre-market run
            if rt.get("last_batch_post_close") is False:
                rt["batch_fresh"] = False
                ts = _now_iso()
                seq = self._next_seq()
                rt["saved_at"] = ts
                rt["version_seq"] = seq
                self._atomic_write(self._runtime_path, rt)
                logger.warning("[BATCH_STATE_NORMALIZED] last_batch_post_close=False "
                               "→ batch_fresh forced False")
                return True

            created_at = rt.get("snapshot_created_at", "")
            if not created_at:
                rt["batch_fresh"] = False
                ts = _now_iso()
                seq = self._next_seq()
                rt["saved_at"] = ts
                rt["version_seq"] = seq
                self._atomic_write(self._runtime_path, rt)
                logger.warning("[BATCH_STATE_NORMALIZED] batch_fresh=True without "
                               "snapshot_created_at → forced False")
                return True

            # Compare created_at against batch's own business_date close.
            # Prefer last_batch_business_date (batch's own label); fallback to today.
            bd_str = rt.get("last_batch_business_date") \
                or get_last_closed_trading_day(provider)
            try:
                created = datetime.fromisoformat(created_at)
                if created.tzinfo is None:
                    created = created.replace(tzinfo=US_ET)
                created_et = created.astimezone(US_ET)
                y, m, d = [int(x) for x in bd_str.split("-")]
                close_et = datetime(y, m, d, MARKET_CLOSE_HOUR_ET, 0, 0, tzinfo=US_ET)
                if created_et < close_et:
                    rt["batch_fresh"] = False
                    ts = _now_iso()
                    seq = self._next_seq()
                    rt["saved_at"] = ts
                    rt["version_seq"] = seq
                    self._atomic_write(self._runtime_path, rt)
                    logger.warning(
                        f"[BATCH_STATE_NORMALIZED] created={created_et.isoformat()} "
                        f"< close={close_et.isoformat()} for bd={bd_str} "
                        f"— batch_fresh forced False (pre-market snapshot)")
                    return True
            except Exception as e:
                logger.warning(f"[BATCH_STATE_NORMALIZE_FAIL] {e} — leaving state as-is")
            return False

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
        _EXTRA_ALLOWED = {"prev_regime_ema", "prev_regime_level"}
        with self._lock:
            rt = self.load_runtime() or {}
            for k, v in updates.items():
                if k in _REBAL_DEFAULTS or k in _EXTRA_ALLOWED or k in ("saved_at", "version_seq"):
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

    def transition_phase_with_updates(
        self, new_phase: str, updates: dict
    ) -> Tuple[bool, str]:
        """Transition phase + merge additional fields in 1 atomic write."""
        _EXTRA_ALLOWED = {"prev_regime_ema", "prev_regime_level"}
        with self._lock:
            rt = self.load_runtime() or {}
            current = rt.get("rebal_phase", "IDLE")
            allowed = _VALID_TRANSITIONS.get(current, [])
            if new_phase not in allowed:
                reason = f"Invalid transition: {current} → {new_phase} (allowed: {allowed})"
                logger.warning(f"[US_REBAL_PHASE] REJECTED: {reason}")
                return False, reason
            rt["rebal_phase"] = new_phase
            for k, v in updates.items():
                if k in _REBAL_DEFAULTS or k in _EXTRA_ALLOWED:
                    rt[k] = v
            ts = _now_iso()
            seq = self._next_seq()
            rt["saved_at"] = ts
            rt["version_seq"] = seq
            ok = self._atomic_write(self._runtime_path, rt)
            if ok:
                logger.info(
                    f"[US_REBAL_PHASE] {current} → {new_phase} "
                    f"+{list(updates.keys())}"
                )
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

        # 9. P2.4: Auto Trading Gate (BUY-only enforcement, advisory by default)
        try:
            from risk.execution_guard_hook import guard_buy_execution
            from risk.strategy_health import compute_strategy_health
            rt_full = self.load_runtime() or {}
            _equity_dd = float(rt_full.get("equity_dd_pct", 0.0) or 0.0)
            _health = compute_strategy_health(equity_dd_pct=_equity_dd)
            _decision = guard_buy_execution(runtime=rt_full, strategy_health=_health)
            if _decision.block_buy:
                blocks.append(
                    f"AUTO_GATE_BLOCKED: top={_decision.highest_blocker} "
                    f"mode={_decision.mode}"
                )
                logger.critical(
                    f"[BUY_BLOCKED_BY_AUTO_GATE] market=US req=compute_execute "
                    f"top={_decision.highest_blocker} mode={_decision.mode} "
                    f"reason={_decision.reason}"
                )
            elif not _decision.enabled:
                logger.info(
                    f"[BUY_ADVISORY] market=US req=compute_execute "
                    f"mode={_decision.mode} top={_decision.highest_blocker} "
                    f"reason={_decision.reason}"
                )
            else:
                logger.info(
                    f"[BUY_GATE_ALLOWED] market=US req=compute_execute "
                    f"mode={_decision.mode} buy_scale={_decision.buy_scale:.2f}"
                )
        except Exception as _ge:
            logger.error(f"[BUY_GATE_EVAL_ERROR] {type(_ge).__name__}: {_ge}", exc_info=True)

        return (len(blocks) == 0, blocks)

    # ── Paths (for external reference) ──────────────────────

    @property
    def portfolio_path(self) -> Path:
        return self._portfolio_path

    @property
    def runtime_path(self) -> Path:
        return self._runtime_path
