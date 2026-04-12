"""
rest_token_manager.py — Kiwoom REST API Token Manager
=====================================================
OAuth2 토큰 발급 및 자동 갱신.
토큰 만료 30분 전 자동 refresh.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime

import requests

logger = logging.getLogger("gen4.rest.token")

TOKEN_REFRESH_MARGIN_SEC = 30 * 60  # 만료 30분 전 갱신


class TokenManager:
    """Manages Kiwoom REST API OAuth2 token lifecycle."""

    def __init__(self, app_key: str, app_secret: str, base_url: str) -> None:
        self._app_key = app_key
        self._app_secret = app_secret
        self._base_url = base_url.rstrip("/")
        self._token: str = ""
        self._expires_at: float = 0.0  # Unix timestamp

    @property
    def token(self) -> str:
        """Get valid token. Auto-acquires or refreshes if needed."""
        if not self._token or time.time() >= self._expires_at - TOKEN_REFRESH_MARGIN_SEC:
            self._acquire()
        return self._token

    def auth_headers(self) -> dict:
        """Return headers with Bearer token for API calls."""
        return {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.token}",
        }

    def invalidate(self) -> None:
        """Force re-acquisition on next access."""
        self._token = ""
        self._expires_at = 0.0

    def _acquire(self) -> None:
        """Acquire new token from Kiwoom OAuth2 endpoint."""
        url = f"{self._base_url}/oauth2/token"
        body = {
            "grant_type": "client_credentials",
            "appkey": self._app_key,
            "secretkey": self._app_secret,
        }
        try:
            resp = requests.post(
                url,
                json=body,
                headers={"Content-Type": "application/json;charset=UTF-8"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get("return_code") != 0:
                raise RuntimeError(f"Token error: {data.get('return_msg', 'unknown')}")

            self._token = data["token"]
            # expires_dt format: "20260408151111"
            expires_dt = data.get("expires_dt", "")
            if expires_dt:
                dt = datetime.strptime(expires_dt, "%Y%m%d%H%M%S")
                self._expires_at = dt.timestamp()
            else:
                self._expires_at = time.time() + 24 * 3600  # fallback 24h

            logger.info(
                f"[TOKEN_OK] expires={expires_dt}, "
                f"token={self._token[:12]}..."
            )

            # Update tracker for dashboard
            try:
                from web.api_state import tracker as _tracker
                _tracker.update_token_state(True, self._expires_at)
            except Exception:
                pass  # tracker not available (standalone mode)

        except Exception as e:
            logger.error(f"[TOKEN_FAIL] {e}")
            try:
                from web.api_state import tracker as _tracker
                _tracker.update_token_state(False, 0)
            except Exception:
                pass
            raise
