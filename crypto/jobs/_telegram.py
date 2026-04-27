"""Best-effort Telegram notifier for crypto jobs (Jeff D3 Q3 A+ option).

Logger-first contract:
    - Job ALWAYS logs the structured message to stdout/file logger first.
    - Telegram send is attempted only if both env vars are present:
        TELEGRAM_BOT_TOKEN_CRYPTO  (preferred)  or  TELEGRAM_BOT_TOKEN
        TELEGRAM_CHAT_ID_CRYPTO    (preferred)  or  TELEGRAM_CHAT_ID
    - Network / auth / rate-limit errors are caught and logged at WARNING
      level. ``send()`` returns a status string; it never raises.
    - The job's exit code is based on its own work, not on Telegram outcome.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


_TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
_TIMEOUT_SEC = 5.0


def _resolve_credentials() -> tuple[Optional[str], Optional[str]]:
    token = (
        os.environ.get("TELEGRAM_BOT_TOKEN_CRYPTO")
        or os.environ.get("TELEGRAM_BOT_TOKEN")
    )
    chat = (
        os.environ.get("TELEGRAM_CHAT_ID_CRYPTO")
        or os.environ.get("TELEGRAM_CHAT_ID")
    )
    return token, chat


def send(text: str, *, parse_mode: Optional[str] = None) -> str:
    """Best-effort send. Returns one of:
        ``ok``                   — message accepted by Telegram
        ``skipped:no-credentials`` — env not configured
        ``error:<short-reason>`` — network/auth/parse failure (logged WARN)

    Never raises.
    """
    token, chat = _resolve_credentials()
    if not token or not chat:
        logger.info("[telegram] skipped (no credentials)")
        return "skipped:no-credentials"

    try:
        import requests  # type: ignore[import-not-found]
    except ImportError:
        logger.warning("[telegram] requests not installed — skipping")
        return "error:requests-missing"

    payload: dict[str, object] = {"chat_id": chat, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        r = requests.post(
            _TELEGRAM_API.format(token=token),
            json=payload,
            timeout=_TIMEOUT_SEC,
        )
    except requests.RequestException as exc:
        logger.warning("[telegram] network error: %s", exc)
        return f"error:network:{type(exc).__name__}"

    if r.ok:
        logger.info("[telegram] sent (%d bytes)", len(text))
        return "ok"

    logger.warning(
        "[telegram] HTTP %d: %s", r.status_code, r.text[:200]
    )
    return f"error:http:{r.status_code}"
