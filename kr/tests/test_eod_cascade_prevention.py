"""kr/tests/test_eod_cascade_prevention.py — Item 1-5 (2026-04-30 RCA).

Covers the recovery patch added in response to the 2026-04-30 incident
chain (KR_BATCH preflight → ABANDONED → KR_EOD missing → US_EOD
missing). Each test is hermetic: tmp_path for state, mocked Telegram,
no DB access, no broker session.

Tests:
    Item 1 — state_manager
        * primary save mirrors to backup_dirs
        * mirror failure does NOT block primary save
        * read falls back to off-disk mirror when primary + .bak gone

    Item 2 — preflight_recovery
        * is_recoverable classifier
        * single-shot marker honored across calls
        * recovery success / failure shape

    Item 3 — auto_unfreeze
        * is_data_recoverable classifier
        * non-recoverable error tokens hard-block unfreeze
        * single-shot marker honored across calls

    Item 4 — health_probe
        * check_once with healthy tree returns ok=True, no telegram
        * check_once with missing OHLCV fires alert
        * dedup suppresses re-alert within window

    Item 5 — repair_entry_dates
        * _classify decision matrix (5 cases)
        * end-to-end --apply rewrites only confident replacements
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

# Ensure both `kr/` and the repo root are importable.
_HERE = Path(__file__).resolve()
_KR = _HERE.parents[1]
_ROOT = _HERE.parents[2]
for p in (str(_ROOT), str(_KR)):
    if p not in sys.path:
        sys.path.insert(0, p)


# ─────────────────────────────────────────────────────────────────────
# Item 1 — state_manager multi-location backup
# ─────────────────────────────────────────────────────────────────────

class TestStateManagerBackup:
    def _make(self, state_dir: Path, backup_dirs: list[Path]):
        from core.state_manager import StateManager
        return StateManager(
            state_dir=state_dir, trading_mode="paper",
            backup_dirs=backup_dirs,
        )

    def test_save_mirrors_to_backup_dir(self, tmp_path: Path):
        primary = tmp_path / "state"
        mirror = tmp_path / "QtronBackup" / "kr" / "state"
        mgr = self._make(primary, [mirror])
        ok = mgr.save_portfolio({"cash": 1_000_000, "positions": {}})
        assert ok, "save should succeed"
        # Primary written
        primary_file = primary / "portfolio_state_paper.json"
        assert primary_file.exists()
        # Mirror written
        mirror_file = mirror / "portfolio_state_paper.json"
        assert mirror_file.exists(), f"mirror missing at {mirror_file}"
        # Contents identical
        assert primary_file.read_text() == mirror_file.read_text()

    def test_mirror_failure_does_not_block_primary(
        self, tmp_path: Path, monkeypatch
    ):
        """If a backup_dir is unreachable, primary still succeeds."""
        primary = tmp_path / "state"
        # Use a path that simulates unreachable. We monkeypatch shutil.copy2
        # to raise for the mirror copy step but allow the primary atomic
        # rename through.
        from core import state_manager as sm
        import shutil as _sh

        real_copy = _sh.copy2
        bad_dir = tmp_path / "no_such_drive" / "kr_state"

        def selective_copy(src, dst, *a, **kw):
            if str(bad_dir) in str(dst):
                raise PermissionError("simulated mirror failure")
            return real_copy(src, dst, *a, **kw)

        monkeypatch.setattr(sm, "shutil", _sh, raising=False)
        # patch shutil.copy2 directly used inside state_manager
        monkeypatch.setattr(sm.shutil, "copy2", selective_copy)

        mgr = self._make(primary, [bad_dir])
        ok = mgr.save_portfolio({"cash": 2_000_000, "positions": {}})
        assert ok is True, "primary save must succeed even when mirror fails"
        assert (primary / "portfolio_state_paper.json").exists()

    def test_read_falls_back_to_off_disk_mirror(self, tmp_path: Path):
        """Primary deleted entirely → loads from mirror."""
        primary = tmp_path / "state"
        mirror = tmp_path / "mirror"
        mgr = self._make(primary, [mirror])

        mgr.save_portfolio({"cash": 5_555_555, "positions": {}})
        # Wipe primary + .bak (simulate the 04-30 deletion)
        primary_file = primary / "portfolio_state_paper.json"
        primary_bak = primary / "portfolio_state_paper.bak"
        primary_file.unlink()
        if primary_bak.exists():
            primary_bak.unlink()

        # New manager instance (cold restart) — should load via mirror
        mgr2 = self._make(primary, [mirror])
        loaded = mgr2.load_portfolio()
        assert loaded is not None, "expected mirror fallback to succeed"
        assert loaded.get("cash") == 5_555_555


# ─────────────────────────────────────────────────────────────────────
# Item 2 — preflight_recovery
# ─────────────────────────────────────────────────────────────────────

class TestPreflightRecovery:
    def test_is_recoverable_truth_table(self):
        from pipeline import preflight_recovery as pr
        from pipeline.preflight import CheckResult

        # Recoverable
        for code in ("dir_missing", "csv_count_low",
                     "history_sample_low", "universe_size_low"):
            cr = CheckResult(ok=False, error="x",
                             detail={"reason_code": code})
            assert pr.is_recoverable(cr) is True, code

        # Non-recoverable
        for code in ("import_failed", "build_crash", "config_init"):
            cr = CheckResult(ok=False, error="x",
                             detail={"reason_code": code})
            assert pr.is_recoverable(cr) is False, code

        # Missing reason_code
        cr = CheckResult(ok=False, error="x", detail={})
        assert pr.is_recoverable(cr) is False

    def test_single_shot_marker_blocks_repeat_attempts(
        self, tmp_path: Path, monkeypatch
    ):
        """Second call with same (run_type, trade_date) honors prior result."""
        from pipeline import preflight_recovery as pr
        from pipeline.preflight import CheckResult

        # Redirect repo_root → tmp so marker file lands in tmp.
        monkeypatch.setattr(pr, "_repo_root", lambda: tmp_path)
        # Stub the restore script invocation.
        call_count = {"n": 0}

        def fake_invoke():
            call_count["n"] += 1
            return 0, ""

        monkeypatch.setattr(pr, "_invoke_restore_script", fake_invoke)
        pr.reset_for_test()

        # Fake state object with trade_date.
        class _State:
            trade_date = "2026-04-30"

        cr = CheckResult(ok=False, error="x",
                         detail={"reason_code": "dir_missing"})

        ok1, ev1 = pr.try_auto_recover(
            run_type="KR_BATCH", check_name="universe_healthy",
            check_result=cr, state=_State(),
        )
        assert ok1 is True
        assert call_count["n"] == 1

        # Second call — must NOT re-invoke restore.
        ok2, ev2 = pr.try_auto_recover(
            run_type="KR_BATCH", check_name="universe_healthy",
            check_result=cr, state=_State(),
        )
        assert ok2 is True, "prior success should be returned"
        assert call_count["n"] == 1, "restore must not be called twice"
        assert ev2.get("reason") == "prior_attempt_found"

    def test_failed_recovery_still_persists_marker(
        self, tmp_path: Path, monkeypatch
    ):
        from pipeline import preflight_recovery as pr
        from pipeline.preflight import CheckResult

        monkeypatch.setattr(pr, "_repo_root", lambda: tmp_path)
        monkeypatch.setattr(pr, "_invoke_restore_script",
                            lambda: (3, "verify_universe failed"))
        pr.reset_for_test()

        class _State:
            trade_date = "2026-04-30"

        cr = CheckResult(ok=False, error="x",
                         detail={"reason_code": "csv_count_low"})

        ok, ev = pr.try_auto_recover(
            run_type="KR_BATCH", check_name="universe_healthy",
            check_result=cr, state=_State(),
        )
        assert ok is False
        assert ev["exit_code"] == 3

        # Marker exists with success=False so we don't loop.
        marker = (tmp_path / "kr" / "data" / "pipeline"
                  / "preflight_recovery_2026-04-30_KR_BATCH.json")
        assert marker.exists()
        body = json.loads(marker.read_text(encoding="utf-8"))
        assert body["success"] is False


# ─────────────────────────────────────────────────────────────────────
# Item 3 — auto_unfreeze
# ─────────────────────────────────────────────────────────────────────

class TestAutoUnfreeze:
    def test_classifier_recoverable(self):
        from pipeline import auto_unfreeze as au
        for s in (
            "[reason:dir_missing] OHLCV dir missing: ...",
            "[reason:csv_count_low] CSV count 1 < 2500",
            "preflight_blocked:blocked by: ['universe_healthy']",
            "history sample 30/50 (60.0%) < 80%",
            "universe too small: 12 < 500",
            "OHLCV dir missing: backtest/...",
        ):
            assert au.is_data_recoverable(s) is True, s

    def test_classifier_non_recoverable(self):
        from pipeline import auto_unfreeze as au
        for s in (
            "ModuleNotFoundError: no module named foo",
            "PermissionError: [Errno 13] permission denied",
            "config_init crashed: ImportError",
            "universe_builder build_crash",
            "[reason:import_failed] x",
            None,
            "",
        ):
            assert au.is_data_recoverable(s) is False, s

    def test_classifier_hard_blocks_composite(self):
        """If both recoverable and non-recoverable tokens appear,
        non-recoverable wins."""
        from pipeline import auto_unfreeze as au
        s = "preflight_blocked: import failed during dir_missing check"
        assert au.is_data_recoverable(s) is False

    def test_maybe_unfreeze_resets_fail_count(
        self, tmp_path: Path, monkeypatch
    ):
        from pipeline import auto_unfreeze as au

        # Put marker dir under tmp.
        monkeypatch.setattr(au, "_repo_root", lambda: tmp_path)
        # Stub health probe → healthy.
        monkeypatch.setattr(au, "ohlcv_health_pass",
                            lambda: (True, {"csv_count": 2700}))
        au.reset_for_test()

        # Build a fake step + state.
        class _Step:
            name = "lab_batch"
            class _Tracker:
                max_fails = 3
                resets = 0
                def reset(self, state):
                    self.resets += 1
                    state._step_fail_count = 0
            _tracker = _Tracker()

        class _StepState:
            fail_count = 3
            last_error = "[reason:dir_missing] OHLCV dir missing: ..."

        class _State:
            trade_date = "2026-04-30"
            _step_fail_count = 3
            def step(self, name): return _StepState()

        step = _Step()
        ok = au.maybe_unfreeze(step, _State())
        assert ok is True
        assert step._tracker.resets == 1

        # Second call same trade_date → marker prevents re-unfreeze
        ok2 = au.maybe_unfreeze(step, _State())
        assert ok2 is False, "single-shot marker must block second call"
        assert step._tracker.resets == 1

    def test_maybe_unfreeze_skips_when_health_fails(
        self, tmp_path: Path, monkeypatch
    ):
        from pipeline import auto_unfreeze as au
        monkeypatch.setattr(au, "_repo_root", lambda: tmp_path)
        monkeypatch.setattr(au, "ohlcv_health_pass",
                            lambda: (False, {"reason": "dir_missing"}))
        au.reset_for_test()

        class _Step:
            name = "lab_batch"
            class _Tracker:
                max_fails = 3
                resets = 0
                def reset(self, state): self.resets += 1
            _tracker = _Tracker()

        class _StepState:
            fail_count = 3
            last_error = "[reason:dir_missing] OHLCV dir missing"

        class _State:
            trade_date = "2026-04-30"
            def step(self, name): return _StepState()

        ok = au.maybe_unfreeze(_Step(), _State())
        assert ok is False


# ─────────────────────────────────────────────────────────────────────
# Item 4 — health_probe
# ─────────────────────────────────────────────────────────────────────

class TestHealthProbe:
    def _make_healthy_tree(self, root: Path) -> Any:
        """Build a tmp directory with all four critical artefacts."""
        kr_dir = root / "kr"
        state_dir = kr_dir / "state"
        state_dir.mkdir(parents=True)
        ohlcv_dir = root / "backtest" / "data_full" / "ohlcv"
        ohlcv_dir.mkdir(parents=True)
        index_dir = root / "backtest" / "data_full" / "index"
        index_dir.mkdir(parents=True)

        # State files
        (state_dir / "portfolio_state_live.json").write_text(
            json.dumps({"cash": 0, "positions": {}}), encoding="utf-8")
        (state_dir / "runtime_state_live.json").write_text(
            json.dumps({"started_at": "x"}), encoding="utf-8")

        # 2600 OHLCV files (above MIN_CSV_COUNT)
        for i in range(2600):
            (ohlcv_dir / f"{i:06d}.csv").write_text(
                "date,open,high,low,close,volume\n"
                "2026-04-30,1,1,1,1,1\n", encoding="utf-8")

        # KOSPI csv with last row date == today.
        # Must be >= 1024 bytes (state_canary _check_kospi_csv threshold).
        today = date.today().isoformat()
        rows = [f"2026-{m:02d}-{d:02d},2400.0,2410.5,2390.1,2400.5,1234567"
                for m in range(1, 5) for d in range(1, 28)]
        rows.append(f"{today},2400.0,2410.5,2390.1,2400.5,1234567")
        (index_dir / "KOSPI.csv").write_text(
            "date,open,high,low,close,volume\n" + "\n".join(rows) + "\n",
            encoding="utf-8",
        )

        class _Cfg:
            BASE_DIR = kr_dir

        return _Cfg

    def test_check_once_healthy(self, tmp_path: Path):
        from lifecycle.health_probe import HealthProbe
        cfg = self._make_healthy_tree(tmp_path)
        sent = []
        probe = HealthProbe(
            cfg, interval_sec=60,
            telegram_send=lambda text, sev: sent.append((text, sev)),
        )
        result = probe.check_once()
        assert result["ok"] is True, f"expected ok, got {result}"
        assert sent == [], "no Telegram should fire on healthy tree"

    def test_check_once_missing_ohlcv_fires_alert(self, tmp_path: Path):
        from lifecycle.health_probe import HealthProbe
        cfg = self._make_healthy_tree(tmp_path)
        # Wipe OHLCV
        ohlcv_dir = tmp_path / "backtest" / "data_full" / "ohlcv"
        for f in ohlcv_dir.glob("*"):
            f.unlink()
        ohlcv_dir.rmdir()

        sent = []
        probe = HealthProbe(
            cfg, interval_sec=60,
            telegram_send=lambda text, sev: sent.append((text, sev)),
        )
        result = probe.check_once()
        assert result["ok"] is False
        assert any("ohlcv" in n.lower() for n, d in result["failures"])
        assert len(sent) == 1, f"expected 1 alert, got {sent}"
        assert sent[0][1] == "CRITICAL"

    def test_dedup_suppresses_repeat_alert(self, tmp_path: Path):
        from lifecycle.health_probe import HealthProbe
        cfg = self._make_healthy_tree(tmp_path)
        ohlcv_dir = tmp_path / "backtest" / "data_full" / "ohlcv"
        for f in ohlcv_dir.glob("*"):
            f.unlink()
        ohlcv_dir.rmdir()

        sent = []
        probe = HealthProbe(
            cfg, interval_sec=60,
            telegram_send=lambda text, sev: sent.append((text, sev)),
        )
        probe.check_once()
        probe.check_once()
        # Two probe runs but only ONE Telegram.
        assert len(sent) == 1, f"dedup failed; sent={len(sent)}"


# ─────────────────────────────────────────────────────────────────────
# Item 5 — repair_entry_dates
# ─────────────────────────────────────────────────────────────────────

class TestRepairEntryDates:
    def _import(self):
        # The script lives in scripts/ — import with explicit path.
        from importlib import util
        target = _ROOT / "scripts" / "repair_entry_dates.py"
        spec = util.spec_from_file_location("repair_entry_dates", target)
        mod = util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_classify_preserved_when_current_matches_backup(self):
        rd = self._import()
        today = date(2026, 4, 30)
        cur = date(2026, 4, 10)
        cands = [(Path("/x/portfolio.bak"), date(2026, 4, 10))]
        rec, prov, _ = rd._classify("005930", cur, cands, today)
        assert rec == cur
        assert prov == rd.PROV_PRESERVED

    def test_classify_today_with_older_backup_restores(self):
        rd = self._import()
        today = date(2026, 4, 30)
        cur = today  # current = today (cold-start pattern)
        cands = [(Path("/x/mirror.json"), date(2026, 4, 10))]
        rec, prov, _ = rd._classify("005930", cur, cands, today)
        assert rec == date(2026, 4, 10)
        assert prov == rd.PROV_BACKUP

    def test_classify_disagree_flags_unknown(self):
        rd = self._import()
        today = date(2026, 4, 30)
        cur = date(2026, 3, 15)
        cands = [(Path("/x/mirror.json"), date(2026, 4, 1))]
        rec, prov, _ = rd._classify("005930", cur, cands, today)
        # Both have valid distinct dates; neither matches today() — operator
        # picks. Script does NOT auto-resolve.
        assert rec is None
        assert prov == rd.PROV_UNKNOWN

    def test_classify_current_missing_uses_backup(self):
        rd = self._import()
        today = date(2026, 4, 30)
        cands = [(Path("/x/mirror.json"), date(2026, 4, 1))]
        rec, prov, _ = rd._classify("005930", None, cands, today)
        assert rec == date(2026, 4, 1)
        assert prov == rd.PROV_BACKUP

    def test_classify_no_data_returns_unknown(self):
        rd = self._import()
        today = date(2026, 4, 30)
        rec, prov, _ = rd._classify("005930", None, [], today)
        assert rec is None
        assert prov == rd.PROV_UNKNOWN

    def test_apply_changes_only_writes_confident_replacements(
        self, tmp_path: Path,
    ):
        rd = self._import()
        state_file = tmp_path / "portfolio_state_live.json"
        state_file.write_text(json.dumps({
            "positions": {
                "005930": {
                    "code": "005930", "qty": 10, "entry_price": 70000,
                    "entry_date": str(date.today()),
                },
                "000660": {
                    "code": "000660", "qty": 5, "entry_price": 100000,
                    "entry_date": "2026-03-15",
                },
            },
        }), encoding="utf-8")

        rows = [
            {
                "code": "005930",
                "current": str(date.today()),
                "recommended": "2026-04-10",
                "provenance": rd.PROV_BACKUP,
                "rationale": "x",
                "candidates": [],
            },
            {
                "code": "000660",
                "current": "2026-03-15",
                "recommended": None,
                "provenance": rd.PROV_UNKNOWN,
                "rationale": "x",
                "candidates": [],
            },
        ]
        n_mod, n_keep = rd._apply_changes(state_file, rows)
        assert n_mod == 1
        assert n_keep == 1
        body = json.loads(state_file.read_text(encoding="utf-8"))
        # 005930 was rewritten
        assert body["positions"]["005930"]["entry_date"] == "2026-04-10"
        assert body["positions"]["005930"]["entry_date_provenance"] == \
            rd.PROV_BACKUP
        # 000660 untouched (UNKNOWN)
        assert body["positions"]["000660"]["entry_date"] == "2026-03-15"
        assert "entry_date_provenance" not in body["positions"]["000660"]
