"""
exposure_guard.py — DD-based entry blocking + graduated response
=================================================================
Adapted from Gen2 risk_governor.

Modes (legacy):
  NORMAL         — all operations allowed
  DAILY_BLOCKED  — daily DD <= -4% → block new entries
  MONTHLY_BLOCKED — monthly DD <= -7% → block new entries

DD Graduated Levels (STEP 5):
  DD_CAUTION     — -5%  → buy 70%
  DD_WARNING     — -10% → buy 50%
  DD_CRITICAL    — -15% → buy 0%
  DD_SEVERE      — -20% → buy 0% + trim 20%
  DD_SAFE_MODE   — -25% → buy 0% + trim 50%

BuyPermission (Phase 1 방어):
  NORMAL   — 리밸 정상 실행 (매도+매수)
  REDUCED  — 매도 정상 + 매수 축소 (buy_scale *= 0.5)
  BLOCKED  — 리밸 전체 보류 (포지션 유지, trail stop도 금지)

SAFE MODE release: DD >= -20% (configurable)
"""
from __future__ import annotations
import logging
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("gen4.risk")


class BuyPermission(Enum):
    """리밸 허가 수준. get_buy_permission()에서만 결정."""
    NORMAL = "NORMAL"          # 리밸 정상 실행
    REDUCED = "REDUCED"        # 매도 정상 + 매수 축소
    RECOVERING = "RECOVERING"  # BLOCKED에서 복귀 중 — 주문 금지 유지, 관찰만
    BLOCKED = "BLOCKED"        # 리밸 전체 보류 + trail 금지 (주문 상태 불확실)


class ExposureGuard:
    """
    Evaluates portfolio risk state and blocks new entries if DD limits exceeded.
    Does NOT force-liquidate any positions.
    """

    def __init__(self, daily_dd_limit: float = -0.04,
                 monthly_dd_limit: float = -0.07):
        self.daily_dd_limit = daily_dd_limit
        self.monthly_dd_limit = monthly_dd_limit
        self._last_mode = "NORMAL"

        # STEP 5: trim + safe mode state tracking
        self._last_trim_date: str = ""       # "YYYY-MM-DD" of last trim
        self._last_trim_level: str = ""      # level at last trim
        self._safe_mode_active: bool = False
        self._safe_mode_entered_date: str = ""
        self._safe_mode_reason: str = ""     # reason for force_safe_mode

        # Phase 1: safe_mode 3-level + opt10075 streak + pending_external
        self._safe_mode_level: int = 0       # 0=NORMAL, 1=ALERT, 2=RESTRICT, 3=BLOCK
        self._opt10075_fail_streak: int = 0
        self._opt10075_success_streak: int = 0
        self._pending_external_list: List[dict] = []
        self._blocked_since: Optional[datetime] = None  # BLOCKED 진입 시각

        # Phase 2: Recovery state machine
        # NORMAL → BLOCKED → RECOVERING → REDUCED → NORMAL (계단식)
        self._recovery_state: str = "NORMAL"  # NORMAL|BLOCKED|RECOVERING|REDUCED
        self._recovery_entered_at: Optional[datetime] = None
        self._recovery_observation_sessions: int = 0  # RECOVERING 관찰 세션 수
        self._last_recon_ok: bool = True  # 최근 RECON 정상 여부

    def evaluate(self, daily_pnl: float, monthly_dd: float) -> str:
        """
        Evaluate risk mode.

        Args:
            daily_pnl: Today's PnL percentage (e.g., -0.03 = -3%).
            monthly_dd: Monthly drawdown from peak (e.g., -0.05 = -5%).

        Returns:
            "NORMAL" | "DAILY_BLOCKED" | "MONTHLY_BLOCKED"
        """
        if monthly_dd <= self.monthly_dd_limit:
            mode = "MONTHLY_BLOCKED"
        elif daily_pnl <= self.daily_dd_limit:
            mode = "DAILY_BLOCKED"
        else:
            mode = "NORMAL"

        if mode != self._last_mode:
            if mode != "NORMAL":
                logger.warning(f"Risk mode changed: {self._last_mode} -> {mode} "
                               f"(daily={daily_pnl:.2%}, monthly={monthly_dd:.2%})")
            else:
                logger.info(f"Risk mode restored: {self._last_mode} -> NORMAL")
            self._last_mode = mode

        return mode

    def can_buy(self, daily_pnl: float, monthly_dd: float) -> tuple:
        """
        Check if new buys are allowed.

        Returns:
            (allowed: bool, reason: str)
        """
        # Force safe mode blocks all buys (RECON excess, etc.)
        if self._safe_mode_active or self._safe_mode_level >= 2:
            return False, f"Blocked by SAFE_MODE_L{self._safe_mode_level}: {self._safe_mode_reason}"
        mode = self.evaluate(daily_pnl, monthly_dd)
        if mode == "NORMAL":
            return True, "OK"
        return False, f"Blocked by {mode}: daily={daily_pnl:.2%}, monthly={monthly_dd:.2%}"

    def should_skip_rebalance(self, daily_pnl: float, monthly_dd: float) -> tuple:
        """
        Check if rebalance buys should be skipped (legacy, bool return).
        Note: sells (removing positions) are ALWAYS allowed.
        Only new buys are blocked when DD limits exceeded.

        Returns:
            (skip_buys: bool, reason: str)
        """
        allowed, reason = self.can_buy(daily_pnl, monthly_dd)
        if not allowed:
            return True, f"Buys blocked: {reason}"
        return False, "OK"

    # ── DD Graduated Response (STEP 5) ────────────────────────────

    def get_risk_action(self, daily_pnl: float, monthly_dd: float,
                        dd_levels: tuple = None,
                        safe_mode_release: float = -0.20) -> Dict:
        """
        Graduated DD response. Returns action dict.

        Args:
            daily_pnl: today's PnL (e.g., -0.03)
            monthly_dd: monthly drawdown from peak (e.g., -0.12)
            dd_levels: tuple of (threshold, buy_scale, trim_ratio, label)
                       ordered most severe first
            safe_mode_release: DD above which SAFE MODE is released

        Returns:
            {
                "buy_scale": 0.0~1.0,
                "trim_ratio": 0.0~1.0,
                "level": str,
                "safe_mode": bool,
                "reason": str,
            }
        """
        # Default levels if not provided.
        # MUST match config.DD_LEVELS — kept in sync to prevent silent divergence
        # when called without explicit dd_levels (e.g. tests, emergency fallback).
        if dd_levels is None:
            dd_levels = (
                (-0.25, 0.00, 0.20, "DD_SAFE_MODE"),   # trim_ratio synced with config
                (-0.20, 0.00, 0.20, "DD_SEVERE"),
                (-0.15, 0.00, 0.00, "DD_CRITICAL"),
                (-0.10, 0.50, 0.00, "DD_WARNING"),
                (-0.05, 0.70, 0.00, "DD_CAUTION"),
            )

        # Evaluate monthly DD against graduated levels
        level = "NORMAL"
        buy_scale = 1.0
        trim_ratio = 0.0
        safe_mode = False

        for threshold, scale, trim, label in dd_levels:
            if monthly_dd <= threshold:
                level = label
                buy_scale = scale
                trim_ratio = trim
                safe_mode = ("SAFE_MODE" in label)
                break  # most severe first, stop at first match

        # SAFE MODE hysteresis: separate entry/exit thresholds
        today_str = str(date.today())

        if self._safe_mode_active:
            # Already in safe mode — apply hysteresis for release
            if monthly_dd > safe_mode_release:
                # DD recovered past release threshold
                if self._safe_mode_entered_date == today_str:
                    # Same-day release blocked (anti-flapping)
                    safe_mode = True
                    logger.info("[DD_SAFE_MODE_HELD] same-day release blocked "
                                "(entered today, DD=%.2f%%)", monthly_dd * 100)
                else:
                    self._safe_mode_active = False
                    safe_mode = False
                    logger.info("[DD_SAFE_MODE_RELEASE] DD=%.2f%% > release=%.2f%%",
                                monthly_dd * 100, safe_mode_release * 100)
            else:
                # Still below release threshold — stay in safe mode
                safe_mode = True
        elif safe_mode:
            # New safe mode entry (from DD_LEVELS evaluation)
            self._safe_mode_active = True
            self._safe_mode_entered_date = today_str
            logger.critical("[DD_SAFE_MODE_ENTERED] DD=%.2f%%", monthly_dd * 100)

        # Trim dedup: max 1 trim per day, skip if same level already trimmed
        if trim_ratio > 0:
            if self._last_trim_date == today_str:
                logger.info("[DD_TRIM_SKIPPED] already trimmed today "
                            "(level=%s, prev=%s)", level, self._last_trim_level)
                trim_ratio = 0.0  # suppress duplicate trim

        # Legacy daily guard (override if worse)
        if daily_pnl <= self.daily_dd_limit and buy_scale > 0:
            buy_scale = 0.0
            if level == "NORMAL":
                level = "DAILY_BLOCKED"

        # Note: when DD_LEVELS is active, graduated levels supersede
        # the legacy MONTHLY_DD_LIMIT. Legacy daily guard still applies above.

        reason = (f"{level} (daily={daily_pnl:.2%}, monthly_dd={monthly_dd:.2%}, "
                  f"buy_scale={buy_scale:.0%}, trim={trim_ratio:.0%})")

        # Log transitions
        if level != self._last_mode:
            if level != "NORMAL":
                logger.warning(f"[DD_GUARD] {reason}")
            else:
                logger.info(f"[DD_GUARD] restored to NORMAL")
            self._last_mode = level

        return {
            "buy_scale": buy_scale,
            "trim_ratio": trim_ratio,
            "level": level,
            "safe_mode": safe_mode,
            "reason": reason,
        }

    def mark_trim_executed(self, level: str) -> None:
        """Call after _execute_dd_trim() to prevent same-day repeat."""
        self._last_trim_date = str(date.today())
        self._last_trim_level = level
        logger.info(f"[DD_TRIM_MARKED] level={level} date={self._last_trim_date}")

    def force_safe_mode(self, reason: str = "", level: int = 3) -> bool:
        """Force SAFE MODE from external trigger (e.g., RECON excess).

        Args:
            reason: trigger description
            level: 1=ALERT(알림만), 2=RESTRICT(BUY축소), 3=BLOCK(리밸보류)

        Returns:
            True if level changed (caller should send notification).
        """
        prev_level = self._safe_mode_level
        self._safe_mode_level = max(self._safe_mode_level, level)

        # Legacy compat
        if self._safe_mode_level >= 2:
            self._safe_mode_active = True

        if self._safe_mode_level > prev_level:
            self._safe_mode_entered_date = str(date.today())
            self._safe_mode_reason = reason
            self._last_mode = f"SAFE_MODE_L{self._safe_mode_level}"
            logger.critical(f"[SAFE_MODE] L{prev_level}→L{self._safe_mode_level}: {reason}")
            return True  # level changed → notify
        elif reason and reason not in self._safe_mode_reason:
            self._safe_mode_reason += f"; {reason}"
            logger.critical(f"[SAFE_MODE_L{self._safe_mode_level}] additional: {reason}")
        return False

    def try_release_safe_mode(self, monthly_dd: float,
                              release_threshold: float = -0.20) -> bool:
        """Try releasing safe mode. Returns True if released (→ notify)."""
        if self._safe_mode_level == 0:
            return False
        today_str = str(date.today())
        if today_str == self._safe_mode_entered_date:
            return False  # same-day release blocked
        if monthly_dd > release_threshold:
            prev = self._safe_mode_level
            self._safe_mode_level = 0
            self._safe_mode_active = False
            self._safe_mode_reason = ""
            logger.info(f"[SAFE_MODE] L{prev}→L0 RELEASED (DD={monthly_dd:.2%})")
            return True
        return False

    @property
    def safe_mode_level(self) -> int:
        return self._safe_mode_level

    @property
    def safe_mode_reason(self) -> str:
        return getattr(self, "_safe_mode_reason", "")

    # ── opt10075 fail streak ─────────────────────────────────────

    def record_opt10075_result(self, success: bool) -> bool:
        """Record opt10075 query result. Returns True if state changed."""
        prev_fail = self._opt10075_fail_streak
        if success:
            self._opt10075_success_streak += 1
            if self._opt10075_success_streak >= 2:  # 히스테리시스: 2연속 성공
                self._opt10075_fail_streak = 0
        else:
            self._opt10075_fail_streak += 1
            self._opt10075_success_streak = 0
        changed = prev_fail != self._opt10075_fail_streak
        if changed:
            logger.info(f"[OPT10075_STREAK] fail={self._opt10075_fail_streak} "
                        f"success={self._opt10075_success_streak}")
        return changed

    @property
    def opt10075_fail_streak(self) -> int:
        return self._opt10075_fail_streak

    # ── pending_external 판정 ────────────────────────────────────

    def set_pending_external(self, entries: list) -> None:
        """Update pending_external list from state_mgr."""
        self._pending_external_list = entries or []

    def _has_significant_pending_external(self) -> bool:
        """pending_external이 REDUCED 수준인지 판단."""
        if not self._pending_external_list:
            return False
        if len(self._pending_external_list) >= 3:
            return True
        for pe in self._pending_external_list:
            try:
                req_at = datetime.fromisoformat(pe.get("requested_at", ""))
                age_min = (datetime.now() - req_at).total_seconds() / 60
                if age_min > 30:
                    return True
            except (ValueError, TypeError):
                continue
        return False

    def _has_critical_pending_external(self) -> bool:
        """pending_external이 BLOCKED 수준인지 판단."""
        if not self._pending_external_list:
            return False
        if len(self._pending_external_list) >= 2:
            return True
        return False

    # ── BuyPermission — 단일 리밸 허가 판정 ──────────────────────

    def advance_recovery_state(self) -> None:
        """상태 전이 (최대 1단계). get_buy_permission() 전에 호출."""

        # ── 어떤 상태에서든: BLOCKED 조건 재발 → 즉시 BLOCKED ──
        blocked_reason = self._check_blocked_conditions()
        if blocked_reason:
            self._transition_to("BLOCKED", blocked_reason)
            return  # 1단계 전이 완료

        # ── 현재 상태 기반 1단계 전이 ──
        if self._recovery_state == "BLOCKED":
            if (self._opt10075_success_streak >= 2
                    and not self._has_critical_pending_external()
                    and self._last_recon_ok):
                self._transition_to("RECOVERING", "BLOCKED 해제 조건 충족")
            return  # BLOCKED → RECOVERING만. 더 가지 않음

        if self._recovery_state == "RECOVERING":
            self._recovery_observation_sessions += 1
            if self._recovery_observation_sessions >= 1:
                self._transition_to("REDUCED", "관찰 완료")
            return  # RECOVERING → REDUCED만

        if self._recovery_state == "REDUCED":
            reduced_reason = self._check_reduced_conditions()
            if not reduced_reason:
                self._transition_to("NORMAL", "정상 복귀")
            return  # REDUCED → NORMAL만

    def get_buy_permission(self) -> Tuple[BuyPermission, str]:
        """현재 recovery_state 기준 permission 반환 (순수 판정, 전이 없음).

        advance_recovery_state()를 먼저 호출해야 함.
        """
        if self._recovery_state == "BLOCKED":
            reason = self._check_blocked_conditions()
            return BuyPermission.BLOCKED, reason or "BLOCKED"

        if self._recovery_state == "RECOVERING":
            return BuyPermission.RECOVERING, \
                f"RECOVERING (관찰 {self._recovery_observation_sessions}세션)"

        if self._recovery_state == "REDUCED":
            reason = self._check_reduced_conditions()
            return BuyPermission.REDUCED, reason or "복귀 중 REDUCED"

        # NORMAL — 여전히 REDUCED 조건일 수 있음 (DD 등)
        reduced_reason = self._check_reduced_conditions()
        if reduced_reason:
            return BuyPermission.REDUCED, reduced_reason

        return BuyPermission.NORMAL, ""

    def _check_blocked_conditions(self) -> str:
        """BLOCKED 진입 조건 판정. 어떤 상태에서든 즉시 BLOCKED."""
        if self._safe_mode_level >= 3:
            return f"SAFE_MODE_L3: {self._safe_mode_reason}"
        if self._opt10075_fail_streak >= 2:
            return f"opt10075 {self._opt10075_fail_streak}연속 실패"
        if self._has_critical_pending_external():
            return f"pending_external {len(self._pending_external_list)}건 미해결"
        return ""  # no block

    def _check_reduced_conditions(self) -> str:
        """REDUCED 수준 조건 판정."""
        if self._safe_mode_level >= 2:
            return f"SAFE_MODE_L2: {self._safe_mode_reason}"
        if self._opt10075_fail_streak == 1:
            return "opt10075 1회 실패"
        if self._has_significant_pending_external():
            return "pending_external 미해결"
        return ""  # no reduction

    def _transition_to(self, new_state: str, reason: str) -> None:
        """RecoveryState 전이 + 로그."""
        prev = self._recovery_state
        if prev == new_state:
            return
        self._recovery_state = new_state
        self._recovery_entered_at = datetime.now()

        if new_state == "BLOCKED":
            self._blocked_since = datetime.now()
            self._recovery_observation_sessions = 0
            logger.critical(f"[RECOVERY_STATE] {prev}→BLOCKED: {reason}")
        elif new_state == "RECOVERING":
            self._recovery_observation_sessions = 0
            logger.warning(f"[RECOVERY_STATE] {prev}→RECOVERING: {reason}")
        elif new_state == "REDUCED":
            logger.info(f"[RECOVERY_STATE] {prev}→REDUCED: {reason}")
        elif new_state == "NORMAL":
            self._blocked_since = None
            self._recovery_observation_sessions = 0
            logger.info(f"[RECOVERY_STATE] {prev}→NORMAL: {reason}")

    def update_blocked_tracking(self, permission: BuyPermission) -> None:
        """BLOCKED 진입/해제 시각 추적 (Phase 1 compat)."""
        if permission == BuyPermission.BLOCKED:
            if self._blocked_since is None:
                self._blocked_since = datetime.now()
        elif permission == BuyPermission.NORMAL:
            self._blocked_since = None

    def record_recon_result(self, ok: bool) -> None:
        """RECON 결과 기록 (recovery 전이 판단용)."""
        self._last_recon_ok = ok

    @property
    def recovery_state(self) -> str:
        return self._recovery_state

    @property
    def blocked_duration_hours(self) -> float:
        if self._blocked_since is None:
            return 0.0
        return (datetime.now() - self._blocked_since).total_seconds() / 3600
