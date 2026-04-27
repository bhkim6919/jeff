"""S9 — crypto_ohlcv data quality checks.

Five sections (Jeff S9 spec, 2026-04-27):
    1. Coverage   — first_seen 기준 per-pair coverage_pct ≥ 95%
    2. Gap        — max consecutive missing days per pair, threshold ≤ 7
    3. Duplicate  — (pair, candle_dt_kst) PK duplicates, threshold = 0
    4. Outlier    — daily_return |Δ| > 50%, volume top 1% (report only)
    5. Time consistency — candle_dt_kst vs candle_dt_utc mismatch, threshold = 0

Pure analytical functions: each returns plain dicts/lists. Persistence
(HTML report) is the caller's job.

Constraints:
    - Simple statistics only (Jeff S9: no complex models in D1).
    - Reads PG only (parquet equivalence is an S7 concern, already passed).
    - SQL aggregates wherever possible to avoid pulling 84k rows into Python
      for every check.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

logger = logging.getLogger(__name__)


# --- 1. Coverage --------------------------------------------------------------


def compute_coverage(conn, threshold_pct: float = 95.0) -> dict[str, Any]:
    """Per-pair coverage_pct = rows / (last_seen - first_seen + 1) * 100.

    Returns:
        {
            "threshold_pct": 95.0,
            "pairs": [{"pair", "rows", "first_seen", "last_seen",
                       "expected_days", "coverage_pct", "pass"}, ...],
            "fail_count": int,
            "pass": bool,
        }

    A pair with a single row is reported with coverage_pct = 100.0
    (degenerate but not failing).
    """
    sql = """
        SELECT pair,
               COUNT(*)              AS n,
               MIN(candle_dt_kst)    AS first_seen,
               MAX(candle_dt_kst)    AS last_seen
        FROM crypto_ohlcv
        GROUP BY pair
        ORDER BY pair
    """
    rows: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for pair, n, first_seen, last_seen in cur.fetchall():
            expected_days = (last_seen - first_seen).days + 1
            cov = 100.0 if expected_days <= 0 else (n / expected_days * 100.0)
            rows.append({
                "pair": pair,
                "rows": int(n),
                "first_seen": first_seen.isoformat(),
                "last_seen": last_seen.isoformat(),
                "expected_days": int(expected_days),
                "coverage_pct": round(cov, 4),
                "pass": cov >= threshold_pct,
            })
    fail_count = sum(1 for r in rows if not r["pass"])
    return {
        "threshold_pct": threshold_pct,
        "pairs": rows,
        "fail_count": fail_count,
        "pass": fail_count == 0,
    }


# --- 2. Gap -------------------------------------------------------------------


def compute_gaps(conn, threshold_days: int = 7) -> dict[str, Any]:
    """Per-pair maximum consecutive-missing-day gap.

    Implementation: pull (pair, candle_dt_kst) ordered, compute consecutive
    day deltas in Python. With ≈ 84 k rows this is a single ~50 ms scan.

    Returns:
        {
            "threshold_days": 7,
            "pairs": [{"pair", "rows", "max_gap_days", "gap_count", "pass"}],
            "fail_count": int,
            "pass": bool,
            "histogram": {
                "1": int, "2": int, ..., ">14": int   # missing-days bucket
            },
        }
    """
    sql = """
        SELECT pair, candle_dt_kst
        FROM crypto_ohlcv
        ORDER BY pair, candle_dt_kst
    """
    per_pair: dict[str, list[date]] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for pair, dt in cur:
            per_pair.setdefault(pair, []).append(dt)

    histogram: dict[str, int] = {}

    def _bucket(days: int) -> str:
        if days >= 15:
            return ">14"
        return str(days)

    pairs_out: list[dict[str, Any]] = []
    for pair in sorted(per_pair):
        dts = per_pair[pair]
        if len(dts) < 2:
            pairs_out.append({
                "pair": pair, "rows": len(dts),
                "max_gap_days": 0, "gap_count": 0, "pass": True,
            })
            continue
        max_gap = 0
        gap_count = 0
        for i in range(1, len(dts)):
            missing = (dts[i] - dts[i - 1]).days - 1
            if missing > 0:
                histogram[_bucket(missing)] = histogram.get(_bucket(missing), 0) + 1
                gap_count += 1
                if missing > max_gap:
                    max_gap = missing
        pairs_out.append({
            "pair": pair,
            "rows": len(dts),
            "max_gap_days": max_gap,
            "gap_count": gap_count,
            "pass": max_gap <= threshold_days,
        })

    fail_count = sum(1 for p in pairs_out if not p["pass"])
    # Sorted bucket order for stable HTML rendering
    ordered_keys = [str(i) for i in range(1, 15)] + [">14"]
    histogram_sorted = {k: histogram.get(k, 0) for k in ordered_keys if histogram.get(k, 0)}
    return {
        "threshold_days": threshold_days,
        "pairs": pairs_out,
        "fail_count": fail_count,
        "pass": fail_count == 0,
        "histogram": histogram_sorted,
    }


# --- 3. Duplicate -------------------------------------------------------------


def compute_duplicates(conn) -> dict[str, Any]:
    """(pair, candle_dt_kst) PK duplicates. Should be 0 by schema PK.

    Returns:
        {"duplicate_count": int, "samples": [...], "pass": bool}
    """
    sql = """
        SELECT pair, candle_dt_kst, COUNT(*) AS n
        FROM crypto_ohlcv
        GROUP BY pair, candle_dt_kst
        HAVING COUNT(*) > 1
        ORDER BY n DESC, pair, candle_dt_kst
        LIMIT 20
    """
    samples: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql)
        for pair, dt, n in cur:
            samples.append({"pair": pair, "candle_dt_kst": dt.isoformat(), "count": int(n)})
        # Get the total count (separate query — HAVING + COUNT requires subquery)
        cur.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT 1
                FROM crypto_ohlcv
                GROUP BY pair, candle_dt_kst
                HAVING COUNT(*) > 1
            ) d
            """
        )
        (total,) = cur.fetchone()

    return {
        "duplicate_count": int(total),
        "samples": samples,
        "pass": int(total) == 0,
    }


# --- 4. Outlier (report only — never fails the gate) -------------------------


def compute_outliers(
    conn,
    return_threshold: float = 0.50,
    volume_percentile: float = 0.99,
) -> dict[str, Any]:
    """Surface |daily_return| > 50% rows and volume top 1% rows.

    Outliers do NOT fail the gate (Jeff S9: report only). The objective is
    visibility for downstream backtester sanity-checks (Phase 3+).

    Returns:
        {
            "return_threshold": 0.5,
            "big_return_count": int,
            "big_return_samples": [...],
            "volume_p99": float,
            "volume_p99_count": int,
            "volume_top_samples": [...],
        }
    """
    big_returns_sql = """
        WITH with_lag AS (
            SELECT
                pair, candle_dt_kst, close,
                LAG(close) OVER (PARTITION BY pair ORDER BY candle_dt_kst) AS prev_close
            FROM crypto_ohlcv
        )
        SELECT pair, candle_dt_kst, close, prev_close,
               (close - prev_close) / NULLIF(prev_close, 0) AS daily_return
        FROM with_lag
        WHERE prev_close IS NOT NULL
          AND ABS((close - prev_close) / NULLIF(prev_close, 0)) > %s
        ORDER BY ABS((close - prev_close) / NULLIF(prev_close, 0)) DESC
    """
    with conn.cursor() as cur:
        cur.execute(big_returns_sql, (return_threshold,))
        all_big = cur.fetchall()

    big_return_samples = [
        {
            "pair": p,
            "candle_dt_kst": dt.isoformat(),
            "close": float(c) if c is not None else None,
            "prev_close": float(pc) if pc is not None else None,
            "daily_return": float(dr) if dr is not None else None,
        }
        for (p, dt, c, pc, dr) in all_big[:30]
    ]

    # Volume top 1% — use PERCENTILE_CONT for the threshold, then count.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY volume) "
            "FROM crypto_ohlcv WHERE volume IS NOT NULL",
            (volume_percentile,),
        )
        (p99,) = cur.fetchone()
        p99 = float(p99) if p99 is not None else 0.0

        cur.execute(
            "SELECT COUNT(*) FROM crypto_ohlcv WHERE volume > %s",
            (p99,),
        )
        (vol_count,) = cur.fetchone()

        cur.execute(
            "SELECT pair, candle_dt_kst, volume, value_krw "
            "FROM crypto_ohlcv WHERE volume > %s "
            "ORDER BY volume DESC LIMIT 30",
            (p99,),
        )
        vol_samples = [
            {
                "pair": p,
                "candle_dt_kst": dt.isoformat(),
                "volume": float(v) if v is not None else None,
                "value_krw": float(vk) if vk is not None else None,
            }
            for p, dt, v, vk in cur.fetchall()
        ]

    return {
        "return_threshold": return_threshold,
        "big_return_count": len(all_big),
        "big_return_samples": big_return_samples,
        "volume_p99": p99,
        "volume_p99_count": int(vol_count),
        "volume_top_samples": vol_samples,
    }


# --- 5. Time consistency (D1 PASS #13) ---------------------------------------


def compute_time_consistency(conn) -> dict[str, Any]:
    """candle_dt_kst vs candle_dt_utc mismatch count.

    For D1 daily candles per S4 hypothesis B, the two DATEs are equal-valued
    invariant. Any mismatch indicates either a bug in the upbit_provider mapping
    or an Upbit-side change that requires §5.2 re-verification.

    Returns:
        {"mismatch_count": int, "samples": [...], "pass": bool}
    """
    sql_count = """
        SELECT COUNT(*)
        FROM crypto_ohlcv
        WHERE candle_dt_kst <> candle_dt_utc
    """
    sql_samples = """
        SELECT pair, candle_dt_kst, candle_dt_utc
        FROM crypto_ohlcv
        WHERE candle_dt_kst <> candle_dt_utc
        ORDER BY pair, candle_dt_kst
        LIMIT 20
    """
    with conn.cursor() as cur:
        cur.execute(sql_count)
        (total,) = cur.fetchone()
        samples: list[dict[str, Any]] = []
        if total:
            cur.execute(sql_samples)
            for pair, kst, utc in cur:
                samples.append({
                    "pair": pair,
                    "candle_dt_kst": kst.isoformat(),
                    "candle_dt_utc": utc.isoformat(),
                })
    return {
        "mismatch_count": int(total),
        "samples": samples,
        "pass": int(total) == 0,
    }


# --- Aggregate header ---------------------------------------------------------


def compute_summary(conn) -> dict[str, Any]:
    """Lightweight totals used in the Summary section of the report."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT pair) FROM crypto_ohlcv")
        (pairs,) = cur.fetchone()
        cur.execute("SELECT COUNT(*) FROM crypto_ohlcv")
        (rows,) = cur.fetchone()
        cur.execute("SELECT MIN(candle_dt_kst), MAX(candle_dt_kst) FROM crypto_ohlcv")
        first_kst, last_kst = cur.fetchone()
    return {
        "pair_count": int(pairs),
        "row_count": int(rows),
        "earliest_kst": first_kst.isoformat() if first_kst else None,
        "latest_kst": last_kst.isoformat() if last_kst else None,
    }
