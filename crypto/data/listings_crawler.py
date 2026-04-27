"""D2 — Upbit notice crawler for KRW-market delisting events.

Strategy (post-S2 inspection, 2026-04-27):
    Upbit's public notice page (/service_center/notice) is a JS-rendered SPA.
    The underlying API is reachable directly without a browser:

        GET https://api-manager.upbit.com/api/v1/announcements
            ?os=web&category=trade&page={N}&per_page=20

        GET https://api-manager.upbit.com/api/v1/announcements/{id}

    Both return JSON. No Playwright/Selenium dependency.

Filter:
    Only notices whose title contains the keyword "거래지원 종료" (Upbit's
    canonical phrase for delisting) are processed. Title regex extracts the
    coin symbol from the surrounding parens:

        리졸브(RESOLV) 거래지원 종료 안내 (5/26 15:00)
                ^^^^^^

KRW-market scope:
    Many delisting notices cover all markets (KRW + BTC + USDT). Some only
    BTC/USDT. ``affects_krw_market()`` checks the body text for KRW-specific
    language; only KRW-affecting delistings are registered as
    ``KRW-{SYMBOL}`` rows.

Forbidden surface (D1 inheritance):
    Read-only HTTP GET to public notice endpoints. NO Exchange API, NO order
    paths, NO authentication of any kind.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Optional

import requests

from crypto.data.upbit_provider import KRW_MARKET_PREFIX

logger = logging.getLogger(__name__)


# --- Endpoints + headers -----------------------------------------------------

NOTICE_HOST = "https://api-manager.upbit.com"
NOTICE_LIST_PATH = "/api/v1/announcements"
NOTICE_DETAIL_PATH = "/api/v1/announcements/{id}"
SHARE_URL_FMT = "https://upbit.com/service_center/notice?id={id}"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Accept": "application/json",
    "Origin": "https://upbit.com",
    "Referer": "https://upbit.com/service_center/notice",
}

# Notice API is undocumented; default to 2 req/sec which is well below any
# plausible cap. Bump only if Jeff explicitly OKs.
DEFAULT_MIN_INTERVAL_SEC = 0.5
DEFAULT_TIMEOUT_SEC = 10


# --- Filter / parser primitives ---------------------------------------------

DELISTING_KEYWORD = "거래지원 종료"
WARNING_KEYWORD = "유의종목"  # NOT a delisting — separate event
NEW_LISTING_KEYWORD = "신규 거래지원"  # NOT a delisting

# Symbol in title: "...(SYMBOL) 거래지원 종료...". 2~15 chars, A-Z/0-9 (Upbit
# tickers are uppercase alnum).
TITLE_SYMBOL_RE = re.compile(
    r"\(([A-Z0-9]{2,15})\)\s*거래지원\s*종료",
)

# Body date patterns. Order = priority. Each pattern names its format for
# Jeff's G2 verification: ≥ 2 distinct formats encountered = PASS.
BODY_DATE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("YYYY-MM-DD", re.compile(r"(20\d{2})-(\d{1,2})-(\d{1,2})")),
    ("YYYY.MM.DD", re.compile(r"(20\d{2})\.(\d{1,2})\.(\d{1,2})")),
    ("YYYY년 M월 D일", re.compile(r"(20\d{2})년\s*(\d{1,2})월\s*(\d{1,2})일")),
    ("YYYY/MM/DD", re.compile(r"(20\d{2})/(\d{1,2})/(\d{1,2})")),
)

# Body anchor near the delisting date (helps disambiguate when multiple dates
# appear in the body, e.g. "공지일자 2022-05-10 ... 종료일자 2022-05-13").
DELISTING_DATE_ANCHORS = (
    "거래지원 종료 일시",
    "거래지원 종료 일자",
    "거래지원 종료일",
    "거래지원 종료:",
)

# KRW-market presence indicators.
KRW_AFFIRM_TOKENS = ("KRW 마켓", "KRW마켓", "원화 마켓", "원화마켓", "원화(KRW)", "전 마켓", "전체 마켓")
KRW_NEGATE_TOKENS = ("BTC 마켓 거래지원 종료", "USDT 마켓 거래지원 종료")  # only when KRW-affirm absent


# --- Data class -------------------------------------------------------------


@dataclass(frozen=True)
class DelistingNotice:
    """One Upbit delisting event (post-filter)."""

    notice_id: int
    title: str
    listed_at_kst: datetime          # publication time (notice listed)
    symbol: str
    pair: str                        # ``KRW-{symbol}``
    delisted_at_kst: Optional[date]
    date_format_used: Optional[str]  # one of BODY_DATE_PATTERNS names; None if unparsed
    affects_krw: bool
    body_excerpt: str                # ≤ 300 chars around the parsed date
    source_url: str

    def to_listings_row(self) -> dict[str, Any]:
        """Map to a CSV row matching D1's listings.csv schema."""
        return {
            "pair": self.pair,
            "symbol": self.symbol,
            "listed_at": "",
            "delisted_at": self.delisted_at_kst.isoformat() if self.delisted_at_kst else "",
            "delisting_reason": f"Upbit notice #{self.notice_id}: {self.title[:200]}",
            "source": "upbit_notice",
            "notes": (
                f"crawled_from {self.source_url}; "
                f"affects_krw={self.affects_krw}; "
                f"date_format={self.date_format_used or 'unparsed'}"
            ),
        }


# --- Parser helpers ---------------------------------------------------------


def parse_symbol_from_title(title: str) -> Optional[str]:
    m = TITLE_SYMBOL_RE.search(title)
    return m.group(1) if m else None


def is_delisting_title(title: str) -> bool:
    return (
        DELISTING_KEYWORD in title
        and NEW_LISTING_KEYWORD not in title
        and WARNING_KEYWORD not in title
    )


def affects_krw_market(body: str, title: str) -> bool:
    """Heuristic: does this delisting touch the KRW market?"""
    text = (body or "") + "\n" + (title or "")
    if any(tok in text for tok in KRW_AFFIRM_TOKENS):
        return True
    # If body explicitly limits to non-KRW markets, exclude.
    if any(tok in text for tok in KRW_NEGATE_TOKENS):
        return False
    # Default: assume KRW-affecting if the title doesn't restrict markets.
    # Most title-only delistings cover all markets.
    return True


def _earliest_date_in(text: str) -> tuple[Optional[date], Optional[str]]:
    """Find the chronologically EARLIEST date in ``text`` across all known
    formats. Returns (date, format-name) or (None, None) if unparsed.

    'Earliest' is appropriate when scanning a small window near the delisting
    anchor, where the delisting date dominates over later 'last withdrawal'
    fallbacks. Globally, we instead use the 'closest after anchor' helper.
    """
    candidates: list[tuple[date, str]] = []
    for fmt, pat in BODY_DATE_PATTERNS:
        for m in pat.finditer(text):
            try:
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                continue
            candidates.append((d, fmt))
    if not candidates:
        return (None, None)
    # Pick the earliest by date.
    candidates.sort(key=lambda x: x[0])
    return candidates[0]


def parse_delisted_at_from_body(
    body: str,
    listed_at_kst: Optional[datetime] = None,
) -> tuple[Optional[date], Optional[str], str]:
    """Extract the delisting date from a notice body.

    Strategy:
        1. If an anchor phrase ('거래지원 종료 일시' etc.) is present, take the
           closest date AFTER the anchor within 200 chars — this avoids
           accidentally matching the publication date listed earlier in the
           body.
        2. Otherwise, fall back to the earliest date in the entire body that
           is on or after listed_at_kst (the publication date) — delisting
           dates are always in the future relative to publication.

    Returns: (delisted_at_kst, date_format_name, body_excerpt).
    """
    if not body:
        return (None, None, "")

    # 1) Anchor-based search
    for anchor in DELISTING_DATE_ANCHORS:
        idx = body.find(anchor)
        if idx == -1:
            continue
        window = body[idx : idx + 200]
        d, fmt = _earliest_date_in(window)
        if d:
            excerpt = body[max(0, idx - 30) : idx + 200].replace("\n", " ")[:300]
            return (d, fmt, excerpt)

    # 2) Earliest future date relative to listed_at_kst (or all dates if no
    #    publication time provided).
    candidates: list[tuple[date, str, int]] = []
    for fmt, pat in BODY_DATE_PATTERNS:
        for m in pat.finditer(body):
            try:
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                continue
            if listed_at_kst is not None and d < listed_at_kst.date():
                continue
            candidates.append((d, fmt, m.start()))
    if not candidates:
        return (None, None, body[:300].replace("\n", " "))

    candidates.sort(key=lambda x: (x[0], x[2]))
    chosen_date, chosen_fmt, chosen_pos = candidates[0]
    excerpt = body[max(0, chosen_pos - 80) : chosen_pos + 120].replace("\n", " ")[:300]
    return (chosen_date, chosen_fmt, excerpt)


# --- HTTP client ------------------------------------------------------------


class UpbitNoticeCrawlerError(Exception):
    pass


class UpbitNoticeCrawler:
    """Read-only paginated client for the notice API."""

    def __init__(
        self,
        host: str = NOTICE_HOST,
        min_interval_sec: float = DEFAULT_MIN_INTERVAL_SEC,
        timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    ) -> None:
        self._host = host.rstrip("/")
        self._min_interval = min_interval_sec
        self._timeout = timeout_sec
        self._last_request_at: float = 0.0
        self._session = requests.Session()
        self._session.headers.update(DEFAULT_HEADERS)

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_request_at = time.monotonic()

    def _get_json(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        self._throttle()
        url = f"{self._host}{path}"
        try:
            r = self._session.get(url, params=params, timeout=self._timeout)
        except requests.RequestException as exc:
            raise UpbitNoticeCrawlerError(f"GET {url} failed: {exc}") from exc
        if not r.ok:
            raise UpbitNoticeCrawlerError(
                f"GET {url} → HTTP {r.status_code}: {r.text[:200]}"
            )
        try:
            data = r.json()
        except ValueError as exc:
            raise UpbitNoticeCrawlerError(
                f"GET {url} returned non-JSON: {r.text[:200]}"
            ) from exc
        if not data.get("success"):
            raise UpbitNoticeCrawlerError(
                f"GET {url} returned success=false: {data}"
            )
        return data["data"]

    def list_page(self, page: int, per_page: int = 20) -> dict[str, Any]:
        return self._get_json(
            NOTICE_LIST_PATH,
            params={
                "os": "web",
                "category": "trade",
                "page": page,
                "per_page": per_page,
            },
        )

    def detail(self, notice_id: int) -> dict[str, Any]:
        return self._get_json(NOTICE_DETAIL_PATH.format(id=notice_id))


# --- High-level crawl ------------------------------------------------------


def crawl_delistings(
    crawler: UpbitNoticeCrawler,
    *,
    max_pages: int = 20,
    per_page: int = 20,
    fail_soft: bool = True,
) -> tuple[list[DelistingNotice], list[dict[str, Any]]]:
    """Walk up to ``max_pages`` of the notice list, fetch detail of every
    delisting-keyword title, and return the parsed events.

    Returns:
        (delistings, errors)
            delistings — list of DelistingNotice
            errors     — list of {"phase", "page", "id", "exception"} dicts

    fail_soft: if True, individual notice errors are logged into ``errors``
    and the crawl continues. If False, the first error aborts.
    """
    errors: list[dict[str, Any]] = []
    out: list[DelistingNotice] = []
    seen_ids: set[int] = set()

    for page in range(1, max_pages + 1):
        try:
            data = crawler.list_page(page, per_page=per_page)
        except UpbitNoticeCrawlerError as exc:
            errors.append({"phase": "list", "page": page, "exception": str(exc)})
            if not fail_soft:
                break
            continue

        notices = data.get("notices", [])
        if not notices:
            break

        for n in notices:
            title = n.get("title", "")
            if not is_delisting_title(title):
                continue
            symbol = parse_symbol_from_title(title)
            if not symbol:
                # Title contains the keyword but no parseable ticker — skip.
                continue
            notice_id = n["id"]
            if notice_id in seen_ids:
                continue
            seen_ids.add(notice_id)

            try:
                d = crawler.detail(notice_id)
            except UpbitNoticeCrawlerError as exc:
                errors.append(
                    {"phase": "detail", "page": page, "id": notice_id, "exception": str(exc)}
                )
                if not fail_soft:
                    return (out, errors)
                continue

            body = d.get("body", "") or ""
            try:
                listed_at = datetime.fromisoformat(n["listed_at"])
            except (KeyError, ValueError):
                listed_at = None  # type: ignore[assignment]

            affects_krw = affects_krw_market(body, title)
            delisted_at, fmt, excerpt = parse_delisted_at_from_body(body, listed_at)
            out.append(
                DelistingNotice(
                    notice_id=notice_id,
                    title=title,
                    listed_at_kst=listed_at,  # type: ignore[arg-type]
                    symbol=symbol,
                    pair=f"{KRW_MARKET_PREFIX}{symbol}",
                    delisted_at_kst=delisted_at,
                    date_format_used=fmt,
                    affects_krw=affects_krw,
                    body_excerpt=excerpt,
                    source_url=SHARE_URL_FMT.format(id=notice_id),
                )
            )

    return (out, errors)
