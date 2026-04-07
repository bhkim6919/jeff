"""
ip_monitor.py — Public IP Change Detection + Kakao Alert
=========================================================
주기적으로 공인 IP를 확인하고, 변경 시 카카오 알림 발송.
키움 REST API는 IP 등록제이므로 변경 감지가 중요.

Usage:
    ../.venv64/Scripts/python.exe data/ip_monitor.py          # 1회 체크
    ../.venv64/Scripts/python.exe data/ip_monitor.py --loop    # 10분 간격 반복
"""
from __future__ import annotations

import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger("gen4.rest.ip")

IP_CACHE_FILE = Path(__file__).resolve().parent / "logs" / "last_known_ip.txt"
CHECK_INTERVAL = 600  # 10분


def get_public_ip() -> str:
    """Get current public IP from multiple sources."""
    sources = [
        "https://api.ipify.org",
        "https://ifconfig.me/ip",
        "https://icanhazip.com",
    ]
    for url in sources:
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.text.strip()
        except Exception:
            continue
    return ""


def get_last_known_ip() -> str:
    """Read last known IP from cache file."""
    try:
        if IP_CACHE_FILE.exists():
            return IP_CACHE_FILE.read_text().strip()
    except Exception:
        pass
    return ""


def save_ip(ip: str) -> None:
    """Save current IP to cache file."""
    try:
        IP_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        IP_CACHE_FILE.write_text(ip)
    except Exception as e:
        logger.warning(f"[IP] Save failed: {e}")


def send_kakao_alert(old_ip: str, new_ip: str) -> None:
    """Send Kakao notification about IP change."""
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from notify.kakao_notify import notify
        msg = (
            f"[IP 변경 감지]\n"
            f"이전: {old_ip}\n"
            f"현재: {new_ip}\n"
            f"시각: {datetime.now():%Y-%m-%d %H:%M}\n\n"
            f"키움 REST API IP 등록 변경 필요!\n"
            f"https://openapi.kiwoom.com"
        )
        notify(msg)
        logger.info(f"[IP_ALERT] Kakao sent: {old_ip} -> {new_ip}")
    except Exception as e:
        logger.warning(f"[IP_ALERT] Kakao failed: {e}")


def check_ip() -> dict:
    """Check IP and alert if changed. Returns status dict."""
    current = get_public_ip()
    if not current:
        logger.warning("[IP] Failed to get public IP")
        return {"status": "error", "error": "Cannot reach IP services"}

    last = get_last_known_ip()

    if not last:
        # First run
        save_ip(current)
        logger.info(f"[IP] Initial: {current}")
        return {"status": "init", "ip": current}

    if current == last:
        logger.info(f"[IP] Unchanged: {current}")
        return {"status": "ok", "ip": current}

    # IP changed!
    logger.warning(f"[IP_CHANGED] {last} -> {current}")
    send_kakao_alert(last, current)
    save_ip(current)
    return {"status": "changed", "old": last, "new": current}


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data.rest_logger import setup_rest_logging
    setup_rest_logging()

    loop_mode = "--loop" in sys.argv

    if loop_mode:
        logger.info(f"[IP_MONITOR] Starting loop (interval={CHECK_INTERVAL}s)")
        while True:
            try:
                result = check_ip()
                print(f"[{datetime.now():%H:%M:%S}] {result}")
            except Exception as e:
                logger.error(f"[IP_MONITOR] Error: {e}")
            time.sleep(CHECK_INTERVAL)
    else:
        result = check_ip()
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
