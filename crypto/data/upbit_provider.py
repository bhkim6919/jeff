"""Upbit Quotation REST client (READ-ONLY, public market data).

D1 scope:
    - Daily candles only (/v1/candles/days)
    - KRW spot markets only
    - No authentication (Quotation API is public)

EXPLICITLY FORBIDDEN (DESIGN.md §2.3):
    - Exchange API endpoints (/v1/orders, /v1/accounts, /v1/withdraws, /v1/deposits)
    - Order placement / cancellation
    - Balance / account inquiries
    - API key / signature handling

Rate limit (Upbit Quotation REST, 2026-04 기준):
    10 req/sec, 600 req/min per IP.

References:
    - DESIGN.md §4.1 Data Source
    - DESIGN.md §5 Time Axes (KST candle vs UTC snapshot)
"""

from __future__ import annotations

import logging
import time
from typing import Any, Optional

import requests


logger = logging.getLogger(__name__)


# --- Constants ---------------------------------------------------------------

UPBIT_QUOTATION_BASE_URL = "https://api.upbit.com"
QUOTATION_RATE_LIMIT_PER_SEC = 10  # 600/min, 10/sec per IP
MIN_REQUEST_INTERVAL_SEC = 1.0 / QUOTATION_RATE_LIMIT_PER_SEC  # 0.1s
DEFAULT_TIMEOUT_SEC = 10
DEFAULT_USER_AGENT = "Q-TRON-Crypto-Lab/D1 (Quotation only; bhkim6919@github)"

DAILY_CANDLES_PATH = "/v1/candles/days"
MARKET_ALL_PATH = "/v1/market/all"
TICKER_PATH = "/v1/ticker"

# Allowed endpoint prefixes (defensive whitelist — runtime guard).
# If a future change tries to call /v1/orders or /v1/accounts, _request() will refuse.
ALLOWED_ENDPOINT_PREFIXES = (
    "/v1/candles/",
    "/v1/market/",
    "/v1/ticker",
    "/v1/trades/",
    "/v1/orderbook",
)

KRW_MARKET_PREFIX = "KRW-"


# --- Exceptions --------------------------------------------------------------


class UpbitProviderError(Exception):
    """Base exception for Upbit Quotation client."""


class UpbitForbiddenEndpointError(UpbitProviderError):
    """Raised when an endpoint outside the read-only whitelist is requested.

    This is a defensive guard against accidental Exchange API calls in D1.
    """


class UpbitRateLimitError(UpbitProviderError):
    """Raised when Upbit returns 429 Too Many Requests."""


# --- Provider ---------------------------------------------------------------


class UpbitQuotationProvider:
    """Read-only Upbit Quotation REST client.

    Thread-safety: NOT thread-safe. Use one instance per thread, or wrap with a lock.
    """

    def __init__(
        self,
        base_url: str = UPBIT_QUOTATION_BASE_URL,
        user_agent: str = DEFAULT_USER_AGENT,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
        min_request_interval_sec: float = MIN_REQUEST_INTERVAL_SEC,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_sec = timeout_sec
        self._min_request_interval_sec = min_request_interval_sec
        self._last_request_at: float = 0.0

        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )

    # --- Internal helpers ----------------------------------------------------

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._min_request_interval_sec:
            time.sleep(self._min_request_interval_sec - elapsed)
        self._last_request_at = time.monotonic()

    def _request(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        if not any(path.startswith(p) for p in ALLOWED_ENDPOINT_PREFIXES):
            raise UpbitForbiddenEndpointError(
                f"D1 read-only client refused endpoint {path!r}. "
                f"Allowed prefixes: {ALLOWED_ENDPOINT_PREFIXES}"
            )

        url = f"{self._base_url}{path}"
        self._throttle()
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout_sec)
        except requests.RequestException as exc:
            raise UpbitProviderError(f"GET {url} failed: {exc}") from exc

        if resp.status_code == 429:
            raise UpbitRateLimitError(
                f"Upbit rate limit hit (HTTP 429). Headers: "
                f"{dict(resp.headers)!r}"
            )
        if not resp.ok:
            raise UpbitProviderError(
                f"GET {url} → HTTP {resp.status_code}: {resp.text[:200]}"
            )
        return resp.json()

    # --- Public API ----------------------------------------------------------

    def list_krw_markets(self) -> list[dict[str, Any]]:
        """List active KRW markets via /v1/market/all.

        Returns dicts with keys: market, korean_name, english_name, market_warning.
        Filters to KRW-* markets only.
        """
        all_markets = self._request(MARKET_ALL_PATH, params={"isDetails": "true"})
        return [m for m in all_markets if m.get("market", "").startswith(KRW_MARKET_PREFIX)]

    def fetch_daily_candles(
        self,
        market: str,
        count: int = 200,
        to_utc: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetch up to 200 daily candles for `market` (e.g. 'KRW-BTC').

        Args:
            market: Upbit market code. Must be KRW-* in D1.
            count: 1..200 candles per call (Upbit hard limit).
            to_utc: Optional. ISO 8601 UTC timestamp ('YYYY-MM-DDTHH:MM:SSZ').
                Returns candles strictly BEFORE `to_utc`. Used for pagination.
                If None, returns the most recent candles.

        Returns:
            List of candle dicts in DESCENDING date order (most recent first).
            Each item contains:
                - market
                - candle_date_time_utc  (e.g., '2026-04-26T00:00:00')
                - candle_date_time_kst  (e.g., '2026-04-26T09:00:00')
                - opening_price, high_price, low_price, trade_price (close)
                - timestamp (epoch ms)
                - candle_acc_trade_price  (거래대금 KRW)
                - candle_acc_trade_volume (거래량 in coin)
                - prev_closing_price
                - change_price, change_rate

        Raises:
            ValueError: invalid market or count.
            UpbitProviderError: HTTP error.
            UpbitRateLimitError: HTTP 429.
        """
        if not market.startswith(KRW_MARKET_PREFIX):
            raise ValueError(
                f"D1 only supports {KRW_MARKET_PREFIX}* markets, got {market!r}"
            )
        if not (1 <= count <= 200):
            raise ValueError(f"count must be 1..200, got {count}")

        params: dict[str, Any] = {"market": market, "count": count}
        if to_utc:
            params["to"] = to_utc

        candles = self._request(DAILY_CANDLES_PATH, params=params)
        if not isinstance(candles, list):
            raise UpbitProviderError(
                f"Unexpected response type from {DAILY_CANDLES_PATH}: "
                f"{type(candles).__name__}"
            )
        return candles


# --- Convenience: paginated range fetch --------------------------------------


def fetch_daily_range(
    provider: UpbitQuotationProvider,
    market: str,
    target_count: int,
    page_size: int = 200,
) -> list[dict[str, Any]]:
    """Paginate `fetch_daily_candles` to retrieve up to `target_count` candles.

    Returns candles in DESCENDING date order, deduplicated on candle_date_time_utc.
    """
    if target_count <= 0:
        return []

    seen_utc: set[str] = set()
    out: list[dict[str, Any]] = []
    to_utc: Optional[str] = None

    while len(out) < target_count:
        page_count = min(page_size, target_count - len(out))
        page = provider.fetch_daily_candles(market, count=page_count, to_utc=to_utc)
        if not page:
            break

        added_this_page = 0
        for c in page:
            key = c.get("candle_date_time_utc", "")
            if key and key not in seen_utc:
                seen_utc.add(key)
                out.append(c)
                added_this_page += 1

        if added_this_page == 0:
            # No new candles; further pagination is fruitless.
            break

        # Pagination: next call should return candles older than the oldest in this page.
        oldest = page[-1]
        oldest_utc = oldest.get("candle_date_time_utc")
        if not oldest_utc:
            break
        # Upbit `to` accepts ISO 8601 UTC; append 'Z' if not present.
        to_utc = oldest_utc if oldest_utc.endswith("Z") else f"{oldest_utc}Z"

        if len(page) < page_count:
            # Upstream returned fewer than requested → exhausted history.
            break

    return out[:target_count]
