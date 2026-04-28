"""D3-2 reconcile DB ↔ CSV for crypto_listings (report-only).

Per Jeff D3-2 scope (2026-04-28):
    - Compare row counts
    - Detect pair / source / delisted_at drift
    - Emit JSON evidence
    - Logger alert on drift
    - Telegram best-effort
    - **Auto-fix forbidden** — report-only

The comparison itself (``compare()``) is pure and operates on in-memory dicts,
so synthetic drift can be exercised without touching PG/CSV. The runnable
entry (``scripts/crypto/reconcile_db_csv.py``) wraps PG + CSV I/O around it.

Exit codes:
    0 — clean (parity perfect)
    1 — drift detected (no writes, alert fired)
    2 — fatal error (couldn't read PG or CSV)
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from crypto.db.env import ensure_main_project_env_loaded
from crypto.jobs._telegram import send as telegram_send

logger = logging.getLogger(__name__)


# Resolve worktree root (crypto/jobs/this.py → parents[2])
_HERE = Path(__file__).resolve()
WORKTREE_ROOT = _HERE.parents[2]
DEFAULT_CSV = WORKTREE_ROOT / "crypto" / "data" / "listings.csv"
DEFAULT_EVIDENCE_DIR = WORKTREE_ROOT / "crypto" / "data" / "_verification"

DEFAULT_DRIFT_FIELDS: tuple[str, ...] = ("source", "delisted_at")
TELEGRAM_DRIFT_SAMPLE_PAIRS = 5  # cap pair listings in alert text


# --- Comparison primitives ---------------------------------------------


def _norm(value: Any) -> Optional[str]:
    """Normalize NULL/empty/whitespace to None for parity comparison.

    PG returns ``None`` for NULL columns; CSV stores ``""`` for the same.
    Date-like values get coerced to their ISO string. Everything else is
    stringified and stripped.
    """
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    s = str(value).strip()
    return s if s else None


@dataclass(frozen=True)
class FieldDrift:
    pair: str
    field: str
    db_value: Optional[str]
    csv_value: Optional[str]


@dataclass
class ReconcileReport:
    db_total: int = 0
    csv_total: int = 0
    only_in_db: list[str] = field(default_factory=list)
    only_in_csv: list[str] = field(default_factory=list)
    field_drift: list[FieldDrift] = field(default_factory=list)
    fields_compared: tuple[str, ...] = DEFAULT_DRIFT_FIELDS

    @property
    def row_count_diff(self) -> int:
        return self.db_total - self.csv_total

    @property
    def drift_detected(self) -> bool:
        return bool(self.only_in_db or self.only_in_csv or self.field_drift)

    @property
    def drift_by_field(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for d in self.field_drift:
            out[d.field] = out.get(d.field, 0) + 1
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": {
                "db_total_rows": self.db_total,
                "csv_total_rows": self.csv_total,
                "row_count_match": self.row_count_diff == 0,
                "row_count_diff": self.row_count_diff,
                "pairs_only_in_db": len(self.only_in_db),
                "pairs_only_in_csv": len(self.only_in_csv),
                "field_drift_count": len(self.field_drift),
                "drift_by_field": self.drift_by_field,
                "drift_detected": self.drift_detected,
                "fields_compared": list(self.fields_compared),
            },
            "drift": {
                "only_in_db": sorted(self.only_in_db),
                "only_in_csv": sorted(self.only_in_csv),
                "field_drift": [asdict(d) for d in self.field_drift],
            },
        }


def compare(
    db_rows: Iterable[dict[str, Any]],
    csv_rows: Iterable[dict[str, Any]],
    *,
    fields: tuple[str, ...] = DEFAULT_DRIFT_FIELDS,
) -> ReconcileReport:
    """Compare DB and CSV rows, returning a drift report.

    Each input row must have a ``pair`` key. ``fields`` lists the columns to
    compare per-pair (default: ``source`` + ``delisted_at`` per Jeff's
    explicit scope). Order-independent — rows are indexed by pair.

    Pure function: no I/O, no logging.
    """
    db_by_pair: dict[str, dict[str, Any]] = {}
    for r in db_rows:
        pair = r.get("pair")
        if not pair:
            continue
        db_by_pair[pair] = r

    csv_by_pair: dict[str, dict[str, Any]] = {}
    for r in csv_rows:
        pair = r.get("pair")
        if not pair:
            continue
        csv_by_pair[pair] = r

    db_pairs = set(db_by_pair)
    csv_pairs = set(csv_by_pair)

    only_in_db = sorted(db_pairs - csv_pairs)
    only_in_csv = sorted(csv_pairs - db_pairs)

    drifts: list[FieldDrift] = []
    for pair in sorted(db_pairs & csv_pairs):
        db_row = db_by_pair[pair]
        csv_row = csv_by_pair[pair]
        for f in fields:
            d = _norm(db_row.get(f))
            c = _norm(csv_row.get(f))
            if d != c:
                drifts.append(
                    FieldDrift(pair=pair, field=f, db_value=d, csv_value=c)
                )

    return ReconcileReport(
        db_total=len(db_by_pair),
        csv_total=len(csv_by_pair),
        only_in_db=only_in_db,
        only_in_csv=only_in_csv,
        field_drift=drifts,
        fields_compared=fields,
    )


# --- I/O wrappers -------------------------------------------------------


def _read_pg_listings(conn) -> list[dict[str, Any]]:
    sql = """
        SELECT pair, symbol, listed_at, delisted_at, delisting_reason,
               source, notes
        FROM crypto_listings
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _read_csv_listings(csv_path: Path) -> list[dict[str, Any]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"listings CSV not found: {csv_path}")
    with csv_path.open(encoding="utf-8") as f:
        return [dict(r) for r in csv.DictReader(f)]


# --- Alert formatting ---------------------------------------------------


def _format_telegram_alert(report: ReconcileReport, evidence_path: Path) -> str:
    by_field = report.drift_by_field
    by_field_str = ", ".join(f"{k}={v}" for k, v in sorted(by_field.items())) or "0"

    sample_db = ", ".join(report.only_in_db[:TELEGRAM_DRIFT_SAMPLE_PAIRS])
    sample_csv = ", ".join(report.only_in_csv[:TELEGRAM_DRIFT_SAMPLE_PAIRS])

    lines = [
        "crypto/D3-2 DRIFT",
        f"db={report.db_total} csv={report.csv_total} diff={report.row_count_diff:+d}",
        f"only_in_db={len(report.only_in_db)}"
        + (f" ({sample_db})" if sample_db else ""),
        f"only_in_csv={len(report.only_in_csv)}"
        + (f" ({sample_csv})" if sample_csv else ""),
        f"field_drift={len(report.field_drift)} ({by_field_str})",
        f"evidence={evidence_path.name}",
    ]
    return "\n".join(lines)


def _log_drift(report: ReconcileReport) -> None:
    by_field = report.drift_by_field
    logger.warning(
        "[D3-2] DRIFT — db=%d csv=%d diff=%+d only_in_db=%d only_in_csv=%d "
        "field_drift=%d (%s)",
        report.db_total,
        report.csv_total,
        report.row_count_diff,
        len(report.only_in_db),
        len(report.only_in_csv),
        len(report.field_drift),
        ", ".join(f"{k}={v}" for k, v in sorted(by_field.items())) or "—",
    )
    # Per-pair detail at INFO level — verbose but bounded by the universe size.
    for d in report.field_drift:
        logger.info(
            "[D3-2] drift pair=%s field=%s db=%r csv=%r",
            d.pair, d.field, d.db_value, d.csv_value,
        )
    for p in report.only_in_db:
        logger.info("[D3-2] only_in_db pair=%s", p)
    for p in report.only_in_csv:
        logger.info("[D3-2] only_in_csv pair=%s", p)


# --- CLI ----------------------------------------------------------------


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--csv",
        type=Path,
        default=DEFAULT_CSV,
        help=f"listings CSV path (default {DEFAULT_CSV}).",
    )
    p.add_argument(
        "--evidence-dir",
        type=Path,
        default=DEFAULT_EVIDENCE_DIR,
        help=f"Evidence JSON output dir (default {DEFAULT_EVIDENCE_DIR}).",
    )
    p.add_argument(
        "--no-telegram",
        action="store_true",
        help="Skip Telegram alert even on drift (logger + JSON still emitted).",
    )
    return p.parse_args(argv)


def run(argv: Optional[list[str]] = None) -> int:
    """Reconcile entry. Returns shell exit code."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    args = _parse_args(argv)
    started_at_utc = (
        datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    )
    print("=" * 78)
    print(f"[D3-2] reconcile_db_csv @ {started_at_utc}")
    print(f"  csv={args.csv}")
    print("=" * 78)

    # 1) Read both sides
    try:
        ensure_main_project_env_loaded()
        from shared.db.pg_base import connection
        with connection() as conn:
            db_rows = _read_pg_listings(conn)
    except Exception as exc:
        msg = f"PG read failed: {exc}"
        logger.error("[D3-2] %s", msg)
        print(f"[fail] {msg}", file=sys.stderr)
        _write_evidence(
            args.evidence_dir, started_at_utc,
            payload={
                "started_at_utc": started_at_utc,
                "completed_at_utc": _now(),
                "mode": "report-only",
                "fatal_error": msg,
                "exit_code": 2,
            },
        )
        return 2

    try:
        csv_rows = _read_csv_listings(args.csv)
    except Exception as exc:
        msg = f"CSV read failed: {exc}"
        logger.error("[D3-2] %s", msg)
        print(f"[fail] {msg}", file=sys.stderr)
        _write_evidence(
            args.evidence_dir, started_at_utc,
            payload={
                "started_at_utc": started_at_utc,
                "completed_at_utc": _now(),
                "mode": "report-only",
                "fatal_error": msg,
                "exit_code": 2,
            },
        )
        return 2

    # 2) Compare (pure)
    report = compare(db_rows, csv_rows)
    print(f"[summary] db={report.db_total} csv={report.csv_total} "
          f"diff={report.row_count_diff:+d}")
    print(f"  only_in_db={len(report.only_in_db)} "
          f"only_in_csv={len(report.only_in_csv)} "
          f"field_drift={len(report.field_drift)} "
          f"by_field={report.drift_by_field}")

    # 3) Logger alert (always — clean is INFO, drift is WARNING)
    if report.drift_detected:
        _log_drift(report)
    else:
        logger.info(
            "[D3-2] CLEAN — db=%d csv=%d, no drift on fields=%s",
            report.db_total, report.csv_total, list(report.fields_compared),
        )

    # 4) Evidence (always)
    payload: dict[str, Any] = {
        "started_at_utc": started_at_utc,
        "completed_at_utc": _now(),
        "mode": "report-only",
        "csv_path": str(args.csv),
        **report.to_dict(),
    }

    # 5) Telegram (best-effort, drift only)
    telegram_status = "skipped:no-drift"
    if report.drift_detected and not args.no_telegram:
        # We don't yet know the evidence path — write first so the message
        # can reference it, then write again with telegram_status.
        evidence_path = _write_evidence(args.evidence_dir, started_at_utc, payload)
        telegram_status = telegram_send(_format_telegram_alert(report, evidence_path))
    elif args.no_telegram and report.drift_detected:
        telegram_status = "skipped:--no-telegram"

    payload["telegram_status"] = telegram_status
    payload["exit_code"] = 1 if report.drift_detected else 0
    final_path = _write_evidence(args.evidence_dir, started_at_utc, payload)
    print(f"[evidence] {final_path}")
    print(f"[telegram] {telegram_status}")

    if report.drift_detected:
        print(f"[verdict] DRIFT — see evidence + logs (no auto-fix per D3-2 scope)")
        return 1
    print(f"[verdict] CLEAN")
    return 0


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_evidence(
    dir_path: Path,
    started_at_utc: str,
    payload: dict[str, Any],
) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    fname = f"reconcile_db_csv_{started_at_utc[:10]}.json"
    out = dir_path / fname
    out.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return out


if __name__ == "__main__":
    raise SystemExit(run())
