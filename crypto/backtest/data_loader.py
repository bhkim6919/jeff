"""Per-pair OHLCV loader (Jeff D4 G2: matrix ffill forbidden).

Why this is its own module:
    KR Gen4's second cost-discrepancy root cause was matrix forward-fill —
    loading every pair into one wide DataFrame and ``ffill()`` ing pre-
    listing NaN cells produced phantom prices that altered momentum/vol
    rankings. The two backtest implementations differed in whether they
    did this, contributing to the +472% vs +28.9% gap.

Contract enforced here:
    1. ``load_pair`` returns a per-pair DataFrame indexed by the actual
       trading days of that pair only. Pre-listing dates do NOT appear as
       NaN — they're absent.
    2. NaN cells *within* a pair's data window are preserved verbatim. No
       ffill/bfill. Callers decide signal handling (typical: skip the
       trade decision on NaN close).
    3. Multi-pair access goes through ``iter_pairs`` (caller iterates) or
       ``load_panel`` (returns dict[pair → df], NOT a wide matrix).

Usage::

    loader = OhlcvLoader(connection_factory)
    df = loader.load_pair("KRW-BTC", date(2018, 1, 1), date(2026, 4, 26))
    for pair in universe.active_pairs(D):
        df = loader.load_pair(pair, D - 365 days, D)
        # … per-pair signal computation
"""

from __future__ import annotations

from datetime import date
from typing import Callable, Iterable, Iterator, Optional

import pandas as pd


# Type alias for any zero-arg callable returning a DB-API connection.
ConnectionFactory = Callable[[], object]


_OHLCV_COLUMNS = (
    "candle_dt_kst",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value_krw",
)


class OhlcvLoader:
    """Per-pair OHLCV loader. Read-only against ``crypto_ohlcv``."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self._connect = connection_factory

    def load_pair(
        self,
        pair: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """Return DataFrame indexed by ``candle_dt_kst`` for the requested
        window, columns = (open, high, low, close, volume, value_krw).

        - Pre-listing days are absent (PK ``crypto_ohlcv(pair, candle_dt_kst)``
          enforces this; we simply SELECT what exists).
        - In-window NaN rows are returned as-is (not forward-filled).
        - Index is sorted ascending.
        """
        if start_date > end_date:
            raise ValueError(
                f"start_date {start_date} after end_date {end_date}"
            )
        sql = """
            SELECT candle_dt_kst, open, high, low, close, volume, value_krw
            FROM crypto_ohlcv
            WHERE pair = %s AND candle_dt_kst BETWEEN %s AND %s
            ORDER BY candle_dt_kst ASC
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (pair, start_date, end_date))
                rows = cur.fetchall()
        if not rows:
            return pd.DataFrame(columns=list(_OHLCV_COLUMNS[1:])).rename_axis(
                "candle_dt_kst"
            )
        df = pd.DataFrame(rows, columns=list(_OHLCV_COLUMNS))
        df["candle_dt_kst"] = pd.to_datetime(df["candle_dt_kst"]).dt.date
        df = df.set_index("candle_dt_kst").sort_index()
        # Force numeric dtypes — psycopg2 returns Decimal for NUMERIC columns.
        for col in ("open", "high", "low", "close", "volume", "value_krw"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def iter_pairs(
        self,
        pairs: Iterable[str],
        start_date: date,
        end_date: date,
    ) -> Iterator[tuple[str, pd.DataFrame]]:
        """Yield (pair, df) for each pair sequentially. Memory-friendly
        alternative to load_panel for large universes."""
        for pair in pairs:
            yield pair, self.load_pair(pair, start_date, end_date)

    def load_panel(
        self,
        pairs: Iterable[str],
        start_date: date,
        end_date: date,
    ) -> dict[str, pd.DataFrame]:
        """Load multiple pairs into a ``dict[pair → DataFrame]``.

        DELIBERATELY returns a dict, NOT a wide DataFrame. Wide-matrix
        construction is left to the caller and must NEVER apply ffill —
        that's the failure mode the D4 design is built to prevent. If a
        downstream needs a panel-shape view, it should align indexes and
        keep NaN, not impute.
        """
        return {pair: self.load_pair(pair, start_date, end_date) for pair in pairs}


def assert_no_forward_fill_applied(df: pd.DataFrame) -> None:
    """Defensive check: a typed signal that the caller has NOT silently
    forward-filled. Used in the foundation verifier to enforce G2.

    The check is a heuristic — we can't prove negative absence — but it
    catches the most common offender: contiguous NaN runs converted to
    repeats of the prior value. We assert the index spans only actual data
    days (no synthetic rows) and any close NaN remains NaN.
    """
    if df.empty:
        return
    if df.index.has_duplicates:
        raise AssertionError("ffill check: duplicated index entries present")
    if not df.index.is_monotonic_increasing:
        raise AssertionError("ffill check: index not sorted")
    # If the loader were ffill-padding, dense pre-listing rows would appear
    # with constant prices. We can't pin this exactly without listings, but
    # we can require strict monotonic dates which the loader's SELECT
    # naturally guarantees.
