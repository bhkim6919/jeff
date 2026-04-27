"""Crypto Lab D1 — PostgreSQL repository for crypto_ohlcv & friends.

Scope (S6 — PG only first, per Jeff 2026-04-27):
    - Read/write crypto_ohlcv via UPSERT (idempotent re-fetch).
    - Compute deterministic row_checksum (SHA256) for D1 PASS #11/#12.
    - Parquet write is intentionally STUBBED here — implemented in S7
      after pyarrow installation is OK'd by Jeff.

Atomic write protocol (DESIGN.md §4.4):
    Full protocol (PG transaction → tmp parquet → checksum verify → rename)
    is realized in S7. S6 implements steps 1, 4, 5, 8 only:
        1. Build per-pair DataFrame from Upbit response
        4. PG transaction begin
        5. UPSERT
        8. PG commit
    Steps 2, 3, 6, 7, 9, 10 (parquet, checksum cross-verify, rename) are S7.

Forbidden surface:
    No Exchange API. No order/account fields. Read-only Quotation source only.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)


# --- Canonical row_checksum --------------------------------------------------

# Numeric precision used in the canonical SHA256 input.
# Chosen to match crypto_ohlcv schema scales (open/high/low/close/volume = 8 dp,
# value_krw = 2 dp). Determinism > round-trip parity; we never re-derive
# numerics from the checksum.
_PRICE_DECIMALS = 8
_VOLUME_DECIMALS = 8
_VALUE_KRW_DECIMALS = 2


def _norm_decimal(value: Optional[float | int | str | Decimal], places: int) -> str:
    """Canonical decimal stringification for checksum determinism.

    Returns ``""`` for None inputs (preserves NaN distinction).
    """
    if value is None:
        return ""
    # Use str() before Decimal to avoid float-binary surprises.
    return f"{Decimal(str(value)):.{places}f}"


def compute_row_checksum(
    pair: str,
    candle_dt_kst: date,
    open_: Optional[float | Decimal],
    high: Optional[float | Decimal],
    low: Optional[float | Decimal],
    close: Optional[float | Decimal],
    volume: Optional[float | Decimal],
    value_krw: Optional[float | Decimal],
) -> bytes:
    """Deterministic 32-byte SHA256 over the canonical row tuple.

    Canonical form:
        ``pair|YYYY-MM-DD|open|high|low|close|volume|value_krw``
        with prices/volume formatted to 8 decimals and value_krw to 2 decimals.

    Re-running ``compute_row_checksum(...)`` with identical inputs MUST return
    identical bytes. This is the runtime guarantee for D1 PASS #11 (DB ↔
    parquet checksum match) and #12 (snapshot_version determinism).
    """
    parts = [
        pair,
        candle_dt_kst.isoformat(),
        _norm_decimal(open_, _PRICE_DECIMALS),
        _norm_decimal(high, _PRICE_DECIMALS),
        _norm_decimal(low, _PRICE_DECIMALS),
        _norm_decimal(close, _PRICE_DECIMALS),
        _norm_decimal(volume, _VOLUME_DECIMALS),
        _norm_decimal(value_krw, _VALUE_KRW_DECIMALS),
    ]
    canonical = "|".join(parts).encode("utf-8")
    return hashlib.sha256(canonical).digest()


# --- Upbit candle → row mapping ----------------------------------------------


def upbit_candle_to_row(candle: dict[str, Any]) -> dict[str, Any]:
    """Map a single Upbit /v1/candles/days entry to a crypto_ohlcv row dict.

    Computes row_checksum on the way out.

    Expected Upbit fields (validated):
        market, candle_date_time_utc, candle_date_time_kst,
        opening_price, high_price, low_price, trade_price,
        candle_acc_trade_volume, candle_acc_trade_price.
    """
    pair = candle["market"]
    if not pair.startswith("KRW-"):
        raise ValueError(f"Non-KRW pair refused: {pair!r}")

    # Upbit timestamps are naive ISO; .date() yields the trade-day DATE.
    candle_dt_kst = datetime.fromisoformat(candle["candle_date_time_kst"]).date()
    candle_dt_utc = datetime.fromisoformat(candle["candle_date_time_utc"]).date()

    open_ = candle.get("opening_price")
    high = candle.get("high_price")
    low = candle.get("low_price")
    close = candle.get("trade_price")
    volume = candle.get("candle_acc_trade_volume")
    value_krw = candle.get("candle_acc_trade_price")

    return {
        "pair": pair,
        "candle_dt_kst": candle_dt_kst,
        "candle_dt_utc": candle_dt_utc,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "value_krw": value_krw,
        "row_checksum": compute_row_checksum(
            pair, candle_dt_kst, open_, high, low, close, volume, value_krw
        ),
    }


# --- PG operations -----------------------------------------------------------


_UPSERT_SQL = """
INSERT INTO crypto_ohlcv
    (pair, candle_dt_kst, candle_dt_utc, open, high, low, close,
     volume, value_krw, row_checksum, fetched_at)
VALUES
    (%(pair)s, %(candle_dt_kst)s, %(candle_dt_utc)s,
     %(open)s, %(high)s, %(low)s, %(close)s,
     %(volume)s, %(value_krw)s, %(row_checksum)s, NOW())
ON CONFLICT (pair, candle_dt_kst) DO UPDATE SET
    candle_dt_utc = EXCLUDED.candle_dt_utc,
    open          = EXCLUDED.open,
    high          = EXCLUDED.high,
    low           = EXCLUDED.low,
    close         = EXCLUDED.close,
    volume        = EXCLUDED.volume,
    value_krw     = EXCLUDED.value_krw,
    row_checksum  = EXCLUDED.row_checksum,
    fetched_at    = NOW()
"""


def upsert_pair_candles(
    conn,
    candles: Iterable[dict[str, Any]],
) -> dict[str, int]:
    """Idempotent UPSERT of one pair's candles inside a transaction.

    Caller controls transaction boundaries: pass a connection in autocommit
    OFF mode and call ``conn.commit()`` after success (or ``conn.rollback()``
    on failure).

    Returns a small stats dict ``{"rows": N, "pairs": K}``.

    Args:
        conn: psycopg2 connection (already opened by shared/db/pg_base).
        candles: iterable of Upbit candle dicts (see ``upbit_candle_to_row``).
    """
    rows = [upbit_candle_to_row(c) for c in candles]
    if not rows:
        return {"rows": 0, "pairs": 0}

    # psycopg2 needs bytes-like row_checksum to bind as BYTEA.
    # bytes() is already accepted; nothing to convert.
    pairs = {r["pair"] for r in rows}

    with conn.cursor() as cur:
        cur.executemany(_UPSERT_SQL, rows)

    return {"rows": len(rows), "pairs": len(pairs)}


def count_rows_for_pair(conn, pair: str) -> int:
    """Return number of crypto_ohlcv rows for a pair."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM crypto_ohlcv WHERE pair = %s",
            (pair,),
        )
        (cnt,) = cur.fetchone()
    return cnt


def latest_candle_dt_kst_for_pair(conn, pair: str) -> Optional[date]:
    """Return the most recent candle_dt_kst stored for ``pair``, or None."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MAX(candle_dt_kst) FROM crypto_ohlcv WHERE pair = %s",
            (pair,),
        )
        (latest,) = cur.fetchone()
    return latest


def aggregate_checksum_for_pair(conn, pair: str) -> str:
    """Return a short SHA256 hex digest of the pair's row_checksum stream.

    Used in S7 for cross-store verification. In S6 we expose it for early
    smoke tests (re-running the same pair must produce the same digest).
    """
    h = hashlib.sha256()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT row_checksum FROM crypto_ohlcv "
            "WHERE pair = %s ORDER BY candle_dt_kst",
            (pair,),
        )
        for (chk,) in cur:
            h.update(bytes(chk))
    return h.hexdigest()


# --- Schema apply (small helper for scripts/crypto/apply_schema.py) ----------


SCHEMA_SQL_PATH = (
    Path(__file__).resolve().parent / "schema.sql"
)


def apply_schema(conn) -> None:
    """Execute ``crypto/db/schema.sql`` against the open connection.

    Idempotent: schema.sql uses CREATE TABLE IF NOT EXISTS + CREATE INDEX IF
    NOT EXISTS, wrapped in BEGIN/COMMIT.
    """
    sql = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
    # schema.sql contains its own COMMIT; conn is left in idle state.


# --- PG read for parquet sync (S7) ------------------------------------------


_SELECT_PAIR_SQL = """
SELECT pair, candle_dt_kst, candle_dt_utc,
       open, high, low, close, volume, value_krw,
       row_checksum, fetched_at
FROM crypto_ohlcv
WHERE pair = %s
ORDER BY candle_dt_kst ASC
"""


def read_pair_rows_from_pg(conn, pair: str) -> list[dict[str, Any]]:
    """Read all crypto_ohlcv rows for ``pair`` as parquet-ready dicts.

    Returned rows match parquet_io.PARQUET_COLUMNS order/keys exactly. Numeric
    columns are coerced from psycopg2 Decimal to float (parquet float64 schema).
    Sorted by candle_dt_kst ASC — the canonical order for aggregate checksum.
    """
    with conn.cursor() as cur:
        cur.execute(_SELECT_PAIR_SQL, (pair,))
        cols = [d[0] for d in cur.description]
        out: list[dict[str, Any]] = []
        for raw in cur:
            row = dict(zip(cols, raw))
            # Decimal → float for parquet float64 columns. psycopg2 returns
            # decimal.Decimal for NUMERIC; pyarrow accepts Decimal-via-float.
            for k in ("open", "high", "low", "close", "volume", "value_krw"):
                if row[k] is not None:
                    row[k] = float(row[k])
            # row_checksum is memoryview/bytes from psycopg2; normalize to bytes.
            row["row_checksum"] = bytes(row["row_checksum"])
            # fetched_at is timezone-aware datetime — pyarrow accepts as-is.
            out.append(row)
    return out
