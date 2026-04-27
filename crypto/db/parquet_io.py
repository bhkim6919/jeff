"""Parquet I/O for crypto_ohlcv (S7).

Per DESIGN.md §4.3 / §4.4:
    - One parquet file per pair: crypto/data/ohlcv/KRW-{symbol}.parquet
    - Atomic write: ``{path}.tmp`` → ``fsync`` → ``os.replace`` to ``{path}``
    - Schema mirrors PG ``crypto_ohlcv`` exactly (S3 schema.sql).
    - Cache / fallback role only — PG remains canonical truth.

Forbidden surface: nothing here touches Upbit Exchange API or order/account
data. Inputs are crypto_ohlcv rows already validated by the provider/repository.
"""

from __future__ import annotations

import hashlib
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


# Column order = PG schema order (DESIGN.md §11). Keep stable forever; cached
# parquet readers depend on this layout.
PARQUET_COLUMNS: tuple[str, ...] = (
    "pair",
    "candle_dt_kst",
    "candle_dt_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "value_krw",
    "row_checksum",
    "fetched_at",
)


# Explicit pyarrow schema. Avoids pandas auto-inference drift across pyarrow
# versions and keeps the file dtype-stable for downstream consumers.
PARQUET_SCHEMA: pa.Schema = pa.schema(
    [
        ("pair", pa.string()),
        ("candle_dt_kst", pa.date32()),
        ("candle_dt_utc", pa.date32()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.float64()),
        ("value_krw", pa.float64()),
        ("row_checksum", pa.binary(32)),  # SHA256 = 32 bytes
        ("fetched_at", pa.timestamp("us", tz="UTC")),
    ]
)


# Compression: snappy is the de-facto default for parquet — fast read/write,
# good ratio, broad reader support.
PARQUET_COMPRESSION = "snappy"


def _rows_to_arrow_table(rows: list[dict[str, Any]]) -> pa.Table:
    """Build a pa.Table from a list of crypto_ohlcv row dicts.

    Rows must contain all PARQUET_COLUMNS. Order within rows is irrelevant —
    we project by name in PARQUET_COLUMNS order before writing.
    """
    if not rows:
        return PARQUET_SCHEMA.empty_table()

    columns: dict[str, list[Any]] = {col: [] for col in PARQUET_COLUMNS}
    for r in rows:
        for col in PARQUET_COLUMNS:
            columns[col].append(r[col])

    arrays = []
    for col in PARQUET_COLUMNS:
        field = PARQUET_SCHEMA.field(col)
        arrays.append(pa.array(columns[col], type=field.type))
    return pa.Table.from_arrays(arrays, schema=PARQUET_SCHEMA)


# --- Atomic write -----------------------------------------------------------


@contextmanager
def _tmp_parquet_path(final_path: Path):
    """Yield a sibling .tmp path. Cleans up on exception."""
    tmp = final_path.with_suffix(final_path.suffix + ".tmp")
    try:
        yield tmp
    except BaseException:
        if tmp.exists():
            try:
                tmp.unlink()
                logger.warning("Cleaned up tmp parquet on failure: %s", tmp)
            except OSError:
                pass
        raise


def write_pair_parquet_atomic(
    pair: str,
    rows: list[dict[str, Any]],
    out_dir: Path,
) -> Path:
    """Atomically write ``rows`` for ``pair`` to a parquet file.

    Protocol (DESIGN.md §4.4):
        1. Build pa.Table from rows
        2. Write to ``{path}.tmp`` (fully buffered)
        3. fsync the tmp file (and its dir, best-effort)
        4. ``os.replace(tmp, final)`` — atomic on Windows + POSIX
        5. Cleanup tmp on any exception

    Returns the final parquet path.

    The caller is responsible for ordering — but this function does NOT sort
    rows. Sorting must happen in the caller (e.g. by candle_dt_kst).
    """
    if not pair.startswith("KRW-"):
        raise ValueError(f"D1 only supports KRW-* pairs, got {pair!r}")

    out_dir.mkdir(parents=True, exist_ok=True)
    final_path = out_dir / f"{pair}.parquet"

    table = _rows_to_arrow_table(rows)

    with _tmp_parquet_path(final_path) as tmp:
        pq.write_table(table, tmp, compression=PARQUET_COMPRESSION)
        # fsync the file's bytes
        with tmp.open("rb+") as f:
            os.fsync(f.fileno())
        os.replace(tmp, final_path)

    logger.info(
        "wrote parquet pair=%s rows=%d path=%s",
        pair, len(rows), final_path,
    )
    return final_path


# --- Read + verification -----------------------------------------------------


def read_pair_parquet(parquet_path: Path) -> list[dict[str, Any]]:
    """Read all rows for one pair, sorted by candle_dt_kst ASC.

    Returns list of dicts with PARQUET_COLUMNS keys. Empty list if file does
    not exist (caller decides how to interpret missing parquet).
    """
    if not parquet_path.exists():
        return []
    table = pq.read_table(parquet_path)
    df = table.to_pandas()
    df = df.sort_values("candle_dt_kst").reset_index(drop=True)
    return df.to_dict("records")


def aggregate_checksum_from_parquet(parquet_path: Path) -> str:
    """Same algorithm as repository.aggregate_checksum_for_pair() but reads
    parquet instead of PG.

    Iterates row_checksum bytes in candle_dt_kst ASC order and SHA256s the
    concatenation. Empty / missing → empty hex digest sentinel.
    """
    if not parquet_path.exists():
        return ""
    table = pq.read_table(parquet_path, columns=["candle_dt_kst", "row_checksum"])
    df = table.to_pandas().sort_values("candle_dt_kst").reset_index(drop=True)
    h = hashlib.sha256()
    for chk in df["row_checksum"]:
        h.update(bytes(chk))
    return h.hexdigest()


def count_rows_parquet(parquet_path: Path) -> int:
    """Cheap row count via parquet metadata (no full read)."""
    if not parquet_path.exists():
        return 0
    md = pq.read_metadata(parquet_path)
    return md.num_rows


# --- Tmp residual scan -------------------------------------------------------


def list_tmp_residuals(out_dir: Path) -> list[Path]:
    """Return any leftover ``*.parquet.tmp`` files under ``out_dir``.

    D1 PASS condition: this list must be empty after a successful run.
    """
    if not out_dir.exists():
        return []
    return sorted(out_dir.glob("*.parquet.tmp"))
