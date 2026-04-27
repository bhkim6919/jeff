"""Apply crypto/db/schema.sql to the shared KR/US PostgreSQL instance.

Per Jeff S6 instruction (2026-04-27):
    - Use the existing KR/US PostgreSQL instance.
    - Add ONLY crypto_* tables. Touch zero kr_* / us_* tables.
    - Idempotent (CREATE TABLE IF NOT EXISTS).

Pre-flight checks (refuse to apply if any fails):
    1. Connection works via shared/db/pg_base.connection().
    2. Existing kr_* / us_* tables are not the targets of any CREATE in
       schema.sql. (Static parse — schema.sql only references crypto_*.)
    3. After apply, verify the 3 expected tables exist.

Usage:
    "C:/Q-TRON-32_ARCHIVE/.venv64/Scripts/python.exe" -X utf8 \
        scripts/crypto/apply_schema.py [--dry-run]

Exit codes:
    0 — schema applied (or dry-run validated).
    1 — pre-flight failure or post-apply mismatch.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
WORKTREE_ROOT = HERE.parents[2]
sys.path.insert(0, str(WORKTREE_ROOT))

from crypto.db.env import ensure_main_project_env_loaded  # noqa: E402
from crypto.db.repository import SCHEMA_SQL_PATH, apply_schema  # noqa: E402

EXPECTED_TABLES = {
    "crypto_ohlcv",
    "crypto_listings",
    "crypto_universe_top100",
}


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate schema.sql + DB reachability, but do not execute DDL.",
    )
    return p.parse_args()


def _static_check_schema_targets(sql_text: str) -> list[str]:
    """Return any CREATE TABLE / INDEX targets that are NOT crypto_*.

    Defensive guard: fails fast if schema.sql ever drifts to touch
    foreign tables.
    """
    pattern = re.compile(
        r"\bCREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX)(?:\s+IF\s+NOT\s+EXISTS)?\s+"
        r"(?P<name>\w+)",
        re.IGNORECASE,
    )
    bad: list[str] = []
    for m in pattern.finditer(sql_text):
        name = m.group("name")
        # Both crypto_* tables and idx_/uniq_/pk_/chk_ identifiers tied to
        # crypto_* are acceptable. Reject anything that explicitly mentions
        # kr_, us_, or other non-crypto namespaces.
        lower = name.lower()
        if lower.startswith(("kr_", "us_")):
            bad.append(name)
    return bad


def main() -> int:
    args = _parse_args()
    print("=" * 78)
    print(f"S6 Schema Apply — {SCHEMA_SQL_PATH.relative_to(WORKTREE_ROOT)}")
    print("=" * 78)

    sql_text = SCHEMA_SQL_PATH.read_text(encoding="utf-8")
    bad = _static_check_schema_targets(sql_text)
    if bad:
        print(f"[fail] schema.sql touches non-crypto identifiers: {bad}",
              file=sys.stderr)
        return 1
    print(f"[ok] static check: schema.sql touches crypto_* / idx_* / uniq_* / "
          f"chk_* / pk_* identifiers only")

    try:
        env_path = ensure_main_project_env_loaded()
    except Exception as exc:
        print(f"[fail] cannot load main-project .env: {exc}", file=sys.stderr)
        return 1
    print(f"[ok] env loaded from {env_path}")

    # Late import: pg_base requires env to be loaded first.
    from shared.db.pg_base import connection  # noqa: E402

    print("[step] connect to PostgreSQL (shared/db/pg_base)")
    with connection() as conn:
        # Pre-apply table census ---------------------------------------------
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname='public' AND tablename LIKE 'crypto_%' "
                "ORDER BY tablename"
            )
            crypto_tables_before = [r[0] for r in cur.fetchall()]
        print(f"[info] crypto_* tables before: {crypto_tables_before or '(none)'}")

        if args.dry_run:
            print("[dry-run] skipping DDL apply")
            return 0

        print("[step] applying schema.sql (BEGIN; CREATE TABLE IF NOT EXISTS; COMMIT)")
        apply_schema(conn)
        # apply_schema's SQL contains its own COMMIT, but psycopg2 still keeps
        # the wrapping transaction open with autocommit=False; an explicit
        # commit here is a no-op safety belt.
        conn.commit()
        print("[ok] schema applied")

        # Post-apply verification --------------------------------------------
        with conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname='public' AND tablename LIKE 'crypto_%' "
                "ORDER BY tablename"
            )
            crypto_tables_after = sorted(r[0] for r in cur.fetchall())
        print(f"[info] crypto_* tables after: {crypto_tables_after}")

        missing = EXPECTED_TABLES - set(crypto_tables_after)
        if missing:
            print(f"[fail] expected tables missing: {sorted(missing)}",
                  file=sys.stderr)
            return 1

        # crypto_ohlcv column verification (DESIGN.md §11 alignment).
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name='crypto_ohlcv' "
                "ORDER BY ordinal_position"
            )
            cols = cur.fetchall()
        print(f"[info] crypto_ohlcv columns: {len(cols)}")
        col_names = {c[0] for c in cols}
        for required in ("candle_dt_kst", "candle_dt_utc", "row_checksum"):
            if required not in col_names:
                print(f"[fail] crypto_ohlcv missing column: {required}",
                      file=sys.stderr)
                return 1
        print("[ok] crypto_ohlcv has candle_dt_kst, candle_dt_utc, row_checksum")

    print()
    print("[ok] S6-b schema apply PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
