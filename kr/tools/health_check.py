"""
health_check.py — Startup Import & Environment Check
=====================================================
Reconstructed after pre-commit stash incident (2026-04-20) wiped this untracked file.
Contract extracted from `__pycache__/health_check.cpython-312.pyc` co_consts +
call site `us/web/app.py:113` (`run_startup_health_check(scope="us")`) and
`kr/tray_server.py:2358` (`run_startup_health_check` for KR).

Behavior:
  - REQUIRED missing → RuntimeError (halt boot)
  - CRITICAL missing → log + Telegram + DataEvent (allow boot)
  - OPTIONAL missing → log only

Philosophy: the health check itself must NEVER crash the caller. All side-effects
(Telegram/DataEvent emission) are best-effort and swallowed silently.
"""
from __future__ import annotations

import importlib
import logging
import sys
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger("gen4.health")


# ────────────────────────────────────────────────────────────
# Package lists (per scope)
# ────────────────────────────────────────────────────────────
_REQUIRED_COMMON: List[str] = ["fastapi", "uvicorn", "pandas", "numpy", "requests", "dotenv"]
_CRITICAL_KR: List[str] = ["yfinance", "pykrx", "psycopg2", "pytz", "bs4"]
_CRITICAL_US: List[str] = ["alpaca", "psycopg2", "pytz"]
_OPTIONAL_PACKAGES: List[str] = []

_REQUIRED_PACKAGES = _REQUIRED_COMMON
_CRITICAL_PACKAGES: List[str] = []  # resolved per-scope


def _get_package_lists(scope: str) -> tuple:
    """scope별 (REQUIRED, CRITICAL, OPTIONAL) 리스트 반환."""
    scope = (scope or "kr").lower()
    critical = _CRITICAL_KR if scope == "kr" else _CRITICAL_US
    return (list(_REQUIRED_COMMON), list(critical), list(_OPTIONAL_PACKAGES))


# ────────────────────────────────────────────────────────────
# Issue record
# ────────────────────────────────────────────────────────────
@dataclass
class HealthIssue:
    category: str   # "REQUIRED" | "CRITICAL" | "OPTIONAL"
    package: str
    error: str
    details: dict = field(default_factory=dict)

    def __str__(self) -> str:
        return f"[{self.category}] {self.package}: {self.error}"


def _try_import(pkg: str) -> str:
    """import 시도. 성공 시 "", 실패 시 에러 메시지 반환."""
    try:
        importlib.import_module(pkg)
        return ""
    except Exception as e:
        return f"{type(e).__name__}: {e}"


def check_critical_imports(scope: str) -> List[HealthIssue]:
    """
    모든 카테고리 import 검사. 이슈 리스트 반환 (빈 리스트면 정상).

    Args:
        scope: "kr" | "us" — scope 에 따라 CRITICAL dep 리스트 다름.
    """
    required, critical, optional = _get_package_lists(scope)
    issues: List[HealthIssue] = []
    for pkg in required:
        err = _try_import(pkg)
        if err:
            issues.append(HealthIssue(category="REQUIRED", package=pkg, error=err))
    for pkg in critical:
        err = _try_import(pkg)
        if err:
            issues.append(HealthIssue(category="CRITICAL", package=pkg, error=err))
    for pkg in optional:
        err = _try_import(pkg)
        if err:
            issues.append(HealthIssue(category="OPTIONAL", package=pkg, error=err))
    return issues


# ────────────────────────────────────────────────────────────
# Side-effects (best-effort, never raise)
# ────────────────────────────────────────────────────────────
def _try_send_telegram(msg: str) -> None:
    """Telegram 전송 시도. 실패는 조용히 넘어감 (health_check가 네트워크 이슈로 죽으면 안 됨)."""
    try:
        try:
            from notify.telegram_bot import send  # type: ignore
        except Exception:
            from kr.notify.telegram_bot import send  # type: ignore
        try:
            send(msg, severity="CRITICAL")
        except TypeError:
            send(msg)
    except Exception:
        pass


def _emit_health_event(issue: HealthIssue, scope: str) -> None:
    """
    DataEvent 모듈이 존재하면 그쪽으로 기록, 없으면 스킵.
    scope 별로 source prefix 구분 (STARTUP.kr / STARTUP.us).
    """
    try:
        try:
            from shared.data_events import emit_event  # type: ignore
        except Exception:
            try:
                from web.data_events import emit_event  # type: ignore
            except Exception:
                return
        level = "CRITICAL" if issue.category in ("REQUIRED", "CRITICAL") else "WARN"
        emit_event(
            source=f"STARTUP.{scope}",
            level=level,
            code=f"import_failed.{issue.category}",
            message=f"{issue.package} dep missing: {issue.error}",
            details={"category": issue.category, "package": issue.package, "scope": scope},
            telegram=(level == "CRITICAL"),
        )
    except Exception:
        pass


# ────────────────────────────────────────────────────────────
# Report
# ────────────────────────────────────────────────────────────
def report_and_maybe_halt(issues: List[HealthIssue], scope: str) -> None:
    """
    이슈 카테고리별 리포팅 및 halt 결정.
    - REQUIRED 존재 → RuntimeError (서버 부팅 중단)
    - CRITICAL 존재 → 로그 + Telegram + DataEvent (부팅 허용)
    - OPTIONAL 존재 → 로그만

    scope: "kr" | "us" — Telegram 메시지 접두어용
    """
    if not issues:
        logger.info(f"[HEALTH_CHECK] {scope}: all imports OK")
        return

    req = [i for i in issues if i.category == "REQUIRED"]
    crit = [i for i in issues if i.category == "CRITICAL"]
    opt = [i for i in issues if i.category == "OPTIONAL"]

    for iss in opt:
        logger.warning(str(iss))
        _emit_health_event(iss, scope)

    if crit:
        msg_lines = [
            f"🚨 <b>[HEALTH_CHECK] {scope.upper()} CRITICAL</b>",
            "다음 패키지가 누락되어 일부 기능이 작동하지 않습니다:",
        ]
        for iss in crit:
            logger.error(str(iss))
            print(str(iss), file=sys.stderr, flush=True)
            msg_lines.append(f"  • <code>{iss.package}</code>: {iss.error}")
            _emit_health_event(iss, scope)
        msg_lines.append("\n복구: <code>pip install -r requirements.lock</code>")
        _try_send_telegram("\n".join(msg_lines))

    if req:
        msg_lines = [
            f"🚨 <b>[HEALTH_CHECK] {scope.upper()} FATAL</b>",
            "다음 REQUIRED 패키지 누락으로 서버 부팅 불가:",
        ]
        for iss in req:
            logger.critical(str(iss))
            print(f"[HEALTH_CHECK] FATAL: {iss}", file=sys.stderr, flush=True)
            msg_lines.append(f"  • <code>{iss.package}</code>: {iss.error}")
            _emit_health_event(iss, scope)
        _try_send_telegram("\n".join(msg_lines))
        names = ", ".join(i.package for i in req)
        raise RuntimeError(f"{scope}: REQUIRED packages missing — {names}")


def run_startup_health_check(scope: str) -> List[HealthIssue]:
    """
    한 줄 진입점. 부팅 코드 시작부에서 호출.

    Args:
        scope: "kr" or "us" — 로그/알림 접두어 + dep 리스트 선택

    Returns:
        이슈 리스트 (빈 리스트면 정상). REQUIRED 누락 시 RuntimeError raise.
    """
    logger.info(f"[HEALTH_CHECK] Running {scope} startup check...")
    issues = check_critical_imports(scope)
    report_and_maybe_halt(issues, scope)
    return issues


# ────────────────────────────────────────────────────────────
# CLI entry
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    _scope = sys.argv[1] if len(sys.argv) > 1 else "kr"
    issues = run_startup_health_check(_scope)
    print(f"[HEALTH_CHECK] {_scope}: {len(issues)} issue(s)")
    for iss in issues:
        print(f"  {iss}")
    has_hard = any(i.category in ("REQUIRED", "CRITICAL") for i in issues)
    sys.exit(2 if has_hard else (1 if issues else 0))
