# -*- coding: utf-8 -*-
"""
migration.py — PostgreSQL 스키마 마이그레이션 시스템
====================================================
앱 코드 내 CREATE TABLE / ALTER TABLE 금지.
모든 스키마 변경은 이 시스템을 통해서만 수행한다.

Usage:
    from shared.db.migration import MigrationRunner
    runner = MigrationRunner()
    runner.apply_pending()
"""
from __future__ import annotations

import importlib
import importlib.util
import logging
from pathlib import Path
from typing import List, Optional, Tuple

from shared.db.pg_base import connection

logger = logging.getLogger("qtron.migration")

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

# ── _schema_versions 테이블 ────────────────────────────────────

_SCHEMA_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS _schema_versions (
    version     INTEGER PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMPTZ DEFAULT NOW(),
    checksum    TEXT
);
"""


class MigrationRunner:
    """Forward-only 마이그레이션 runner. Idempotent."""

    def __init__(self, migrations_dir: Optional[Path] = None):
        self._dir = migrations_dir or MIGRATIONS_DIR

    def _ensure_schema_table(self, conn) -> None:
        """_schema_versions 테이블 존재 보장."""
        cur = conn.cursor()
        cur.execute(_SCHEMA_TABLE_DDL)
        conn.commit()
        cur.close()

    def get_current_version(self, conn) -> int:
        """현재 적용된 최신 버전. 없으면 0."""
        self._ensure_schema_table(conn)
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(version), 0) FROM _schema_versions")
        (v,) = cur.fetchone()
        cur.close()
        return v

    def _discover_migrations(self) -> List[Tuple[int, str, object]]:
        """
        migrations/ 디렉토리에서 v{NNN}_*.py 파일을 version 순으로 반환.
        각 모듈은 VERSION, DESCRIPTION, up(conn) 필수.
        """
        results = []
        if not self._dir.exists():
            return results

        for f in sorted(self._dir.glob("v[0-9]*.py")):
            if f.name == "__init__.py":
                continue
            mod_name = f.stem
            spec = importlib.util.spec_from_file_location(mod_name, str(f))
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)

            version = getattr(mod, "VERSION", None)
            desc = getattr(mod, "DESCRIPTION", mod_name)
            up_fn = getattr(mod, "up", None)

            if version is None or up_fn is None:
                logger.warning(
                    f"[MIGRATION] skip {f.name}: missing VERSION or up()"
                )
                continue

            results.append((version, desc, mod))

        results.sort(key=lambda x: x[0])
        return results

    def get_pending(self, conn) -> List[Tuple[int, str, object]]:
        """미적용 마이그레이션 목록."""
        current = self.get_current_version(conn)
        return [
            (v, d, m) for v, d, m in self._discover_migrations() if v > current
        ]

    def apply_pending(self, dry_run: bool = False) -> List[str]:
        """
        미적용 마이그레이션 순차 적용. Idempotent.

        Returns:
            적용된 마이그레이션 설명 리스트.
        """
        applied = []

        with connection() as conn:
            pending = self.get_pending(conn)
            if not pending:
                logger.info("[MIGRATION] no pending migrations")
                return applied

            for version, desc, mod in pending:
                if dry_run:
                    logger.info(f"[MIGRATION] dry-run: v{version:03d} {desc}")
                    applied.append(f"v{version:03d} {desc} (dry-run)")
                    continue

                try:
                    mod.up(conn)

                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO _schema_versions (version, description) "
                        "VALUES (%s, %s) "
                        "ON CONFLICT (version) DO NOTHING",
                        (version, desc),
                    )
                    cur.close()
                    conn.commit()

                    logger.info(
                        f"[MIGRATION] applied v{version:03d}: {desc}"
                    )
                    applied.append(f"v{version:03d} {desc}")

                except Exception as e:
                    conn.rollback()
                    logger.error(
                        f"[MIGRATION] FAILED v{version:03d}: {desc}",
                        exc_info=e,
                    )
                    raise RuntimeError(
                        f"Migration v{version:03d} failed: {e}"
                    ) from e

        return applied

    def status(self) -> dict:
        """현재 마이그레이션 상태 요약."""
        with connection() as conn:
            current = self.get_current_version(conn)
            pending = self.get_pending(conn)

            cur = conn.cursor()
            cur.execute(
                "SELECT version, description, applied_at "
                "FROM _schema_versions ORDER BY version"
            )
            history = [
                {"version": v, "description": d, "applied_at": str(a)}
                for v, d, a in cur.fetchall()
            ]
            cur.close()

        return {
            "current_version": current,
            "pending_count": len(pending),
            "pending": [f"v{v:03d} {d}" for v, d, _ in pending],
            "history": history,
        }
