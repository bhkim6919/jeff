"""
auto_start.py — Q-TRON Automated Startup
==========================================
main.py --live가 CommConnect()로 키움을 직접 띄우므로,
이 스크립트는 LIVE 실행 → 비밀번호 입력만 담당.

Flow:
  1. 02_live.bat 실행 (새 콘솔)
  2. 키움 로그인 창 대기 (CommConnect popup)
  3. 비밀번호 창 대기 → pyautogui로 입력
  4. 완료 확인

Usage:
  Task Scheduler에 08:30 등록 (setup_scheduler.bat)
  또는 수동: 02_live_no_uac.bat
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# ── Logging ─────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"auto_start_{datetime.now():%Y%m%d}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("auto_start")

# ── Config ──────────────────────────────────────────────────────
MAX_RETRIES = 3
RETRY_DELAY = 30
LIVE_BAT = Path(__file__).resolve().parent / "02_live.bat"

# Calibrated 2026-04-07
PASSWORD_X, PASSWORD_Y = 1038, 442  # only used for initial click


def _kakao_alert(message: str) -> None:
    """Send Kakao alert (best-effort, never blocks)."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from notify.kakao_notify import notify
        notify(message)
    except Exception as e:
        logger.warning(f"[KAKAO_FAIL] {e}")


def _telegram_alert(message: str) -> None:
    """Send Telegram alert (best-effort, never blocks)."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        from notify.telegram_notify import notify
        notify(message)
    except Exception as e:
        logger.warning(f"[TELEGRAM_FAIL] {e}")


def _alert(message: str) -> None:
    """Send alert via both Telegram + Kakao."""
    _telegram_alert(message)
    _kakao_alert(message)


def _is_process_running(name: str) -> bool:
    """Check if a process with given name is running."""
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"IMAGENAME eq {name}"],
            capture_output=True, text=True, timeout=10,
        )
        return name.lower() in result.stdout.lower()
    except Exception:
        return False


def _launch_live() -> bool:
    """Launch 02_live.bat in a new console."""
    if not LIVE_BAT.exists():
        logger.error(f"[AUTO_START_FAIL] LIVE bat not found: {LIVE_BAT}")
        return False

    try:
        subprocess.Popen(
            ["cmd", "/c", str(LIVE_BAT)],
            cwd=str(LIVE_BAT.parent),
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
        logger.info("[AUTO_START] LIVE engine launched — waiting for Kiwoom login...")
        return True
    except Exception as e:
        logger.error(f"[AUTO_START_FAIL] LIVE launch failed: {e}")
        return False


def _wait_for_password_window(timeout: int = 60) -> bool:
    """Wait for Kiwoom password window to appear.
    CommConnect() opens login popup, then password dialog follows.
    We detect it by waiting for the login to complete (~10-15s)."""
    logger.info("[AUTO_START] Waiting for password window...")
    # main.py logs "Account password window" when ready
    # Give CommConnect time to complete login
    time.sleep(15)
    logger.info("[AUTO_START] Password window should be ready")
    return True


def _enter_password() -> bool:
    """Enter account password via pyautogui."""
    try:
        import pyautogui
        pyautogui.FAILSAFE = True

        password = os.environ.get("QTRON_ACCT_PW", "")
        if not password:
            logger.error("[LOGIN_FAIL] QTRON_ACCT_PW environment variable not set")
            return False

        # Click password input field
        pyautogui.click(PASSWORD_X, PASSWORD_Y)
        time.sleep(0.3)
        # Type password
        pyautogui.typewrite(password, interval=0.05)
        time.sleep(0.3)
        # Tab x2 → [전체계좌에 등록] → Enter
        pyautogui.press("tab")
        pyautogui.press("tab")
        time.sleep(0.2)
        pyautogui.press("enter")
        time.sleep(1)
        # Tab x2 → [닫기] → Enter
        pyautogui.press("tab")
        pyautogui.press("tab")
        time.sleep(0.2)
        pyautogui.press("enter")
        time.sleep(3)

        logger.info("[AUTO_START_OK] Password entered")
        return True
    except ImportError:
        logger.error("[AUTO_START_FAIL] pyautogui not installed")
        return False
    except Exception as e:
        logger.error(f"[LOGIN_FAIL] Password entry failed: {e}")
        return False


def main() -> int:
    logger.info("=" * 60)
    logger.info(f"[AUTO_START] Q-TRON Auto Startup — {datetime.now():%Y-%m-%d %H:%M:%S}")
    logger.info("=" * 60)

    # Step 0: Weekend check
    if datetime.now().weekday() >= 5:
        logger.info("[AUTO_START] Weekend — skipping")
        return 0

    # Step 1: Launch LIVE (this triggers CommConnect → Kiwoom login)
    if not _launch_live():
        _alert("[AUTO_START_FAIL] LIVE launch failed")
        return 1

    # Step 2: Wait for password window
    _wait_for_password_window()

    # Step 3: Enter password (with retry)
    for attempt in range(1, MAX_RETRIES + 1):
        logger.info(f"[AUTO_START] Password attempt {attempt}/{MAX_RETRIES}")
        if _enter_password():
            break
        if attempt < MAX_RETRIES:
            logger.warning(f"[AUTO_START] Retry in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    else:
        _alert(f"[LOGIN_FAIL] Password failed after {MAX_RETRIES} attempts")
        return 1

    # Minimize all windows (Win+M)
    try:
        subprocess.run(
            ["powershell", "-command",
             "(New-Object -ComObject Shell.Application).MinimizeAll()"],
            timeout=10,
        )
        logger.info("[AUTO_START] All windows minimized (Win+M)")
    except Exception as e:
        logger.warning(f"[AUTO_START] MinimizeAll failed: {e}")

    _alert("[AUTO_START_OK] Q-TRON LIVE started")
    logger.info("[AUTO_START_OK] Complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
