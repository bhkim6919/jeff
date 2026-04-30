#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""scripts/repair_entry_dates.py — Item 5 (2026-04-30 RCA).

Manual repair tool for ``Position.entry_date`` corruption.

Why
---
On 2026-04-30 the engine cold-started without ``portfolio_state_live.json``
(deleted externally). RECON BROKER_ONLY re-added all 17 broker holdings
via ``add_position(entry_date=date.today())``, replacing each
position's true entry_date with the cold-start date. Trail-stop
calculations and rebal cooldown windows depend on entry_date, so this
caused silent risk-management drift for the affected positions.

Design (Jeff 2026-04-30):
    * **Operator-driven only.** Never auto-invoked. Belongs in
      ``scripts/`` to stay outside the live engine path.
    * **No today() default.** If no confident source is found for a
      position, the script reports UNKNOWN / NEEDS_MANUAL_REVIEW and
      leaves the existing value untouched in --apply mode.
    * **Audit trail distinct from auto-recovery.** Output goes to
      ``backup/reports/manual_recovery/entry_date_repair_{ts}.md``,
      not the ``incidents/`` directory used by Items 2/4.
    * **Read-only by default.** Requires explicit ``--apply`` to
      persist a new state file. ``--apply`` itself first writes a
      timestamped backup of the current state file.

Sources of truth (in preference order):
    1. ``--state-file`` (current portfolio state — used as the baseline
       and as the source for "PRESERVED" provenance).
    2. The configured off-disk mirrors (``QTRON_STATE_BACKUP_DIRS``)
       and the in-place ``.bak`` file. Earliest valid entry_date wins.
    3. ``backup/state_backup_*/`` ad-hoc snapshots, if present.
    4. ``--broker`` (optional) — Kiwoom REST fill history. Disabled
       by default because it requires a live session. Out of scope
       for the initial v1; the script will print a TODO when invoked.

Usage:
    # Dry-run report (default).
    python scripts/repair_entry_dates.py

    # Apply confident replacements.
    python scripts/repair_entry_dates.py --apply

    # Also scan an extra backup directory.
    python scripts/repair_entry_dates.py --extra-backup-dir D:/old_backups/kr_state
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
sys.path.insert(0, str(_REPO_ROOT / "kr"))

logger = logging.getLogger("qtron.repair_entry_dates")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


PROV_PRESERVED = "PRESERVED"        # current state already had a date — kept
PROV_BACKUP    = "BACKUP_RESTORED"   # earliest match from a backup file
PROV_BROKER    = "BROKER_HISTORY"    # broker fill log
PROV_UNKNOWN   = "UNKNOWN"           # NEEDS_MANUAL_REVIEW
PROV_TODAY_SUS = "TODAY_SUSPECT"     # current date == today AND no older source


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_iso_date(s: Any) -> Optional[date]:
    if not isinstance(s, str):
        return None
    s = s.strip().split(" ")[0]  # tolerate "YYYY-MM-DD HH:MM:SS"
    if not _ISO_DATE_RE.match(s):
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _resolve_default_state_file() -> Path:
    return _REPO_ROOT / "kr" / "state" / "portfolio_state_live.json"


def _resolve_default_backup_dirs() -> List[Path]:
    """Same env contract as state_manager._resolve_default_backup_dirs."""
    env = os.environ.get("QTRON_STATE_BACKUP_DIRS")
    if env is not None:
        if env.strip().upper() in ("", "NONE", "NULL", "OFF", "DISABLED"):
            return []
        return [Path(p.strip()) for p in env.split(";") if p.strip()]
    return [Path("C:/QtronBackup/kr/state")]


def _candidate_state_files(
    state_file: Path, extra_backup_dir: Optional[Path],
) -> List[Path]:
    """All files that *might* hold a prior version of `state_file`."""
    name = state_file.name
    candidates: List[Path] = [state_file]
    candidates.append(state_file.with_suffix(".bak"))

    for d in _resolve_default_backup_dirs():
        candidates.append(d / name)
        candidates.append((d / name).with_suffix(".bak"))

    if extra_backup_dir is not None:
        candidates.append(extra_backup_dir / name)
        candidates.append((extra_backup_dir / name).with_suffix(".bak"))

    # Ad-hoc snapshots like backup/state_backup_20260430_1230/...
    snapshots_root = _REPO_ROOT / "backup"
    if snapshots_root.exists():
        for d in sorted(snapshots_root.glob("state_backup_*")):
            candidates.append(d / name)

    # Deduplicate while preserving order.
    seen: set[str] = set()
    out: List[Path] = []
    for c in candidates:
        key = str(c.resolve()) if c.exists() else str(c)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def _load_positions_from_file(path: Path) -> Optional[Dict[str, Dict[str, Any]]]:
    """Return positions dict (code → position dict), or None if unreadable."""
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        logger.debug(f"[REPAIR_LOAD_SKIP] {path}: {e!r}")
        return None
    if not isinstance(data, dict):
        return None
    pos = data.get("positions")
    if not isinstance(pos, dict):
        return None
    return pos


def _gather_candidates(
    code: str,
    candidate_files: List[Path],
) -> List[Tuple[Path, date]]:
    """For one code, scan all candidate files for valid entry_date entries.

    Returns list of (source_path, entry_date) sorted by date ascending.
    """
    found: List[Tuple[Path, date]] = []
    for fp in candidate_files:
        positions = _load_positions_from_file(fp)
        if positions is None:
            continue
        pos = positions.get(code)
        if not isinstance(pos, dict):
            continue
        d = _parse_iso_date(pos.get("entry_date"))
        if d is None:
            continue
        found.append((fp, d))
    # Earliest first — that's typically the *true* entry.
    found.sort(key=lambda x: x[1])
    return found


def _classify(
    code: str,
    current_entry_date: Optional[date],
    candidates: List[Tuple[Path, date]],
    today: date,
) -> Tuple[Optional[date], str, str]:
    """Decide what entry_date this position should have.

    Returns (recommended_date, provenance, rationale).
    A recommended_date of None means "leave existing untouched".
    """
    # No candidates beyond current — keep current if any, else UNKNOWN.
    candidates_excl_current = [
        (p, d) for p, d in candidates if d != current_entry_date
    ]

    earliest = candidates[0][1] if candidates else None

    # Case 1: no candidates anywhere.
    if not candidates and current_entry_date is None:
        return None, PROV_UNKNOWN, "no current value and no backup match"

    # Case 2: current matches earliest backup → trustworthy preserve.
    if current_entry_date is not None and earliest == current_entry_date:
        return current_entry_date, PROV_PRESERVED, (
            f"current entry_date {current_entry_date} agrees with backup "
            f"earliest"
        )

    # Case 3: current is today() and a (strictly) older backup exists →
    # current is suspect (RECON-cold-start pattern).
    if (current_entry_date == today
            and earliest is not None and earliest < today):
        return earliest, PROV_BACKUP, (
            f"current entry_date {current_entry_date} == today, but backup "
            f"holds {earliest} (earlier) — likely RECON cold-start overwrite"
        )

    # Case 4: current is missing/invalid but backup has a date.
    if current_entry_date is None and earliest is not None:
        return earliest, PROV_BACKUP, (
            f"current entry_date missing; backup earliest={earliest}"
        )

    # Case 5: current and backup both have dates and disagree → UNKNOWN.
    # We do NOT auto-pick because the operator must judge which is right.
    if (current_entry_date is not None and earliest is not None
            and earliest != current_entry_date):
        return None, PROV_UNKNOWN, (
            f"current {current_entry_date} != backup earliest {earliest} "
            f"— manual review required"
        )

    # Default: keep current if any.
    if current_entry_date is not None:
        return current_entry_date, PROV_PRESERVED, "no backup contradiction"
    return None, PROV_UNKNOWN, "fallthrough"


def _write_audit(
    *,
    state_file: Path,
    rows: List[Dict[str, Any]],
    apply_mode: bool,
    backup_path: Optional[Path],
) -> Path:
    """Write the markdown audit log."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = _REPO_ROOT / "backup" / "reports" / "manual_recovery"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"entry_date_repair_{ts}.md"

    n_total = len(rows)
    n_preserved = sum(1 for r in rows if r["provenance"] == PROV_PRESERVED)
    n_backup = sum(1 for r in rows if r["provenance"] == PROV_BACKUP)
    n_today = sum(1 for r in rows if r["provenance"] == PROV_TODAY_SUS)
    n_unknown = sum(1 for r in rows if r["provenance"] == PROV_UNKNOWN)

    lines: List[str] = [
        f"# Entry-date repair — {datetime.now().isoformat()}",
        "",
        "Generated by `scripts/repair_entry_dates.py` (Item 5, "
        "2026-04-30 RCA). This is a **MANUAL** recovery tool — distinct "
        "from the auto-recovery incidents under "
        "`backup/reports/incidents/`.",
        "",
        f"- **state_file**: `{state_file}`",
        f"- **mode**: {'APPLY' if apply_mode else 'DRY-RUN (no writes)'}",
    ]
    if backup_path is not None:
        lines.append(f"- **pre-apply backup**: `{backup_path}`")
    lines += [
        "",
        "## Summary",
        "",
        f"- positions inspected: **{n_total}**",
        f"- preserved (current matches backup or no contradiction): "
        f"**{n_preserved}**",
        f"- restored from backup (current was today / missing): **{n_backup}**",
        f"- still flagged as UNKNOWN — manual review required: **{n_unknown}**",
        f"- TODAY_SUSPECT (current==today, no older source): **{n_today}**",
        "",
        "## Per-position detail",
        "",
        "| code | current | recommended | provenance | rationale | candidates |",
        "|------|---------|-------------|-----------|-----------|------------|",
    ]
    for r in rows:
        cands = "; ".join(
            f"{Path(p).name}={d}" for p, d in r["candidates"]
        ) or "—"
        lines.append(
            f"| `{r['code']}` "
            f"| {r['current'] or '—'} "
            f"| {r['recommended'] or '—'} "
            f"| {r['provenance']} "
            f"| {r['rationale']} "
            f"| {cands} |"
        )

    lines += [
        "",
        "## Action",
        "",
        (
            "Re-run with `--apply` to persist the recommended values for "
            "rows whose provenance is `BACKUP_RESTORED`. Rows with "
            "`UNKNOWN` are NOT modified by this script — operator must "
            "either accept the current value, look up broker history, "
            "or supply a value via a different tool."
            if not apply_mode else
            "Applied. Pre-apply state was backed up to the path above. "
            "The engine should be restarted so PortfolioManager re-loads "
            "from disk; otherwise the in-memory state retains the old "
            "entry_date values."
        ),
    ]
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def _backup_state_file(state_file: Path) -> Path:
    """Copy `state_file` to a timestamped backup BEFORE overwriting."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = _REPO_ROOT / "backup" / f"state_backup_{ts}_repair_entry_dates"
    backup_dir.mkdir(parents=True, exist_ok=True)
    dst = backup_dir / state_file.name
    shutil.copy2(state_file, dst)
    return dst


def _apply_changes(
    state_file: Path,
    rows: List[Dict[str, Any]],
) -> Tuple[int, int]:
    """Rewrite state_file with recommended values where confidence permits.

    Returns (n_modified, n_unchanged).
    """
    raw = json.loads(state_file.read_text(encoding="utf-8"))
    positions = raw.get("positions", {})
    n_modified = 0
    n_unchanged = 0
    for r in rows:
        code = r["code"]
        if code not in positions:
            n_unchanged += 1
            continue
        # Only apply if we have a confident replacement that's different
        # from the current value. UNKNOWN → leave alone.
        rec = r["recommended"]
        prov = r["provenance"]
        cur = r["current"]
        if rec is None or prov in (PROV_UNKNOWN,):
            n_unchanged += 1
            continue
        if rec == cur:
            n_unchanged += 1
            continue
        positions[code]["entry_date"] = str(rec)
        # Provenance metadata for forensics. Position.from_dict ignores it.
        positions[code]["entry_date_provenance"] = prov
        positions[code]["entry_date_repaired_at"] = datetime.now().isoformat()
        n_modified += 1

    raw["positions"] = positions
    state_file.write_text(
        json.dumps(raw, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return n_modified, n_unchanged


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Repair Position.entry_date from backups (Item 5).",
    )
    parser.add_argument(
        "--state-file", type=str, default=None,
        help=f"Path to portfolio_state file. Default: "
             f"{_resolve_default_state_file()}",
    )
    parser.add_argument(
        "--extra-backup-dir", type=str, default=None,
        help="Additional directory to scan for backup state files.",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Persist recommended replacements. Default is dry-run.",
    )
    parser.add_argument(
        "--broker", action="store_true",
        help="(NOT YET IMPLEMENTED) consult broker fill history.",
    )
    args = parser.parse_args(argv)

    state_file = (
        Path(args.state_file) if args.state_file else _resolve_default_state_file()
    )
    extra_backup = Path(args.extra_backup_dir) if args.extra_backup_dir else None

    if not state_file.exists():
        logger.error(f"[STATE_FILE_MISSING] {state_file}")
        return 1

    if args.broker:
        logger.warning(
            "[REPAIR_BROKER_TODO] --broker source not yet implemented in "
            "v1; only file-based candidates are scanned."
        )

    today = date.today()
    candidate_files = _candidate_state_files(state_file, extra_backup)
    logger.info(
        f"[REPAIR_BEGIN] state_file={state_file} candidates="
        f"{[str(p) for p in candidate_files if p.exists()]}"
    )

    current_positions = _load_positions_from_file(state_file)
    if current_positions is None:
        logger.error(f"[CURRENT_LOAD_FAIL] {state_file} unreadable as JSON")
        return 2

    rows: List[Dict[str, Any]] = []
    for code in sorted(current_positions.keys()):
        pos = current_positions[code]
        cur_date = _parse_iso_date(pos.get("entry_date"))
        cands = _gather_candidates(code, candidate_files)
        # Filter: candidates from `state_file` itself are NOT contradictions
        # — we only consider OTHER files as backup sources.
        cands_other = [(p, d) for p, d in cands if p != state_file]
        rec, prov, rationale = _classify(code, cur_date, cands_other, today)

        # Replace generic PROV_UNKNOWN with PROV_TODAY_SUS for the
        # specific case where current == today AND no backup matches —
        # operator should review even though we don't have a fix.
        if (prov == PROV_UNKNOWN
                and cur_date == today
                and not cands_other):
            prov = PROV_TODAY_SUS
            rationale = (
                "current entry_date is today and no backup found — "
                "RECON cold-start pattern, manual review recommended"
            )

        rows.append({
            "code": code,
            "current": str(cur_date) if cur_date else None,
            "recommended": str(rec) if rec else None,
            "provenance": prov,
            "rationale": rationale,
            "candidates": [(str(p), str(d)) for p, d in cands_other],
        })

    backup_path: Optional[Path] = None
    if args.apply:
        backup_path = _backup_state_file(state_file)
        n_mod, n_keep = _apply_changes(state_file, rows)
        logger.warning(
            f"[REPAIR_APPLIED] state_file={state_file} modified={n_mod} "
            f"unchanged={n_keep} backup={backup_path}"
        )

    audit = _write_audit(
        state_file=state_file, rows=rows,
        apply_mode=args.apply, backup_path=backup_path,
    )
    logger.info(f"[REPAIR_AUDIT_WRITTEN] {audit}")

    n_unknown = sum(1 for r in rows if r["provenance"] in (PROV_UNKNOWN, PROV_TODAY_SUS))
    n_backup = sum(1 for r in rows if r["provenance"] == PROV_BACKUP)
    n_preserved = sum(1 for r in rows if r["provenance"] == PROV_PRESERVED)

    print()
    print(f"  state_file:   {state_file}")
    print(f"  mode:         {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"  positions:    {len(rows)}")
    print(f"  preserved:    {n_preserved}")
    print(f"  backup_resto: {n_backup}")
    print(f"  needs review: {n_unknown}")
    print(f"  audit:        {audit}")
    if backup_path is not None:
        print(f"  pre-apply bk: {backup_path}")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
