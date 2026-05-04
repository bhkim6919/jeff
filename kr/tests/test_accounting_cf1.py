"""PR-CF1 tests — accounting foundation.

Verifies the 8 mandatory cases Jeff specified for CF1:
  1. initial 5,000,000 + deposit 1,000,000 => invested_capital 6,000,000
  2. duplicate event_id does not double count
  3. withdrawal 500,000 => invested_capital 5,500,000
  4. replay/load returns deterministic summary
  5. malformed event rejected fail-closed
  6. raw equity files untouched
  7. deprecated finance.capital_events path is not imported
  8. anti-pattern regression still passes
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

# Module-under-test imports — fail loudly if accounting module is broken.
from accounting import (  # noqa: E402
    AccountingSummary,
    CapitalConfig,
    CashflowEvent,
    CashflowLedger,
    EventType,
    compute_summary,
    get_initial_capital,
    load_capital_state,
)


REPO_ROOT = Path(__file__).resolve().parents[2]


# ─── Test 1: initial + deposit ───────────────────────────────────


def test_initial_5m_plus_deposit_1m_equals_6m(tmp_path: Path):
    ledger = CashflowLedger(path=tmp_path / "ledger.jsonl")
    ledger.append(CashflowEvent(
        event_date="2026-04-15",
        type=EventType.DEPOSIT,
        amount=1_000_000,
        source="kakao_bank",
        note="initial deposit test",
    ))
    s = compute_summary(ledger, initial_capital=5_000_000)
    assert s.initial_capital == 5_000_000
    assert s.total_deposits == 1_000_000
    assert s.total_withdrawals == 0
    assert s.net_external_flow == 1_000_000
    assert s.invested_capital == 6_000_000
    assert s.event_count == 1


# ─── Test 2: duplicate event_id idempotency ──────────────────────


def test_duplicate_event_id_does_not_double_count(tmp_path: Path):
    ledger = CashflowLedger(path=tmp_path / "ledger.jsonl")
    ev = CashflowEvent(
        event_date="2026-04-15",
        type=EventType.DEPOSIT,
        amount=1_000_000,
        source="kakao_bank",
        event_id="ev-explicit-001",
    )
    first = ledger.append(ev)
    second = ledger.append(ev)
    assert first is True, "first append should succeed"
    assert second is False, "duplicate event_id should be no-op"

    events = ledger.load()
    assert len(events) == 1
    assert events[0].event_id == "ev-explicit-001"

    s = compute_summary(ledger, initial_capital=5_000_000)
    assert s.invested_capital == 6_000_000  # not 7_000_000
    assert s.event_count == 1


def test_auto_derived_event_id_is_idempotent(tmp_path: Path):
    """When event_id is empty, ledger derives one from canonical fields.
    Two appends of the logically-identical event must collapse to one."""
    ledger = CashflowLedger(path=tmp_path / "ledger.jsonl")
    base_kwargs = dict(
        event_date="2026-04-15",
        type=EventType.DEPOSIT,
        amount=1_000_000,
        source="kakao_bank",
        note="same canonical event",
    )
    ledger.append(CashflowEvent(**base_kwargs))
    ledger.append(CashflowEvent(**base_kwargs))  # different recorded_at, same canonical

    events = ledger.load()
    assert len(events) == 1, f"expected 1 event after deduplication, got {len(events)}"


# ─── Test 3: withdrawal reduces invested_capital ─────────────────


def test_withdrawal_500k_yields_invested_capital_5_500_000(tmp_path: Path):
    ledger = CashflowLedger(path=tmp_path / "ledger.jsonl")
    ledger.append(CashflowEvent(
        event_date="2026-04-15",
        type=EventType.DEPOSIT,
        amount=1_000_000,
        source="kakao_bank",
    ))
    ledger.append(CashflowEvent(
        event_date="2026-04-20",
        type=EventType.WITHDRAWAL,
        amount=500_000,
        source="kakao_bank",
    ))
    s = compute_summary(ledger, initial_capital=5_000_000)
    assert s.total_deposits == 1_000_000
    assert s.total_withdrawals == 500_000
    assert s.net_external_flow == 500_000  # +1_000_000 - 500_000
    assert s.invested_capital == 5_500_000
    assert s.event_count == 2


# ─── Test 4: replay determinism ──────────────────────────────────


def test_replay_load_returns_deterministic_summary(tmp_path: Path):
    ledger_path = tmp_path / "ledger.jsonl"
    ledger1 = CashflowLedger(path=ledger_path)
    ledger1.append(CashflowEvent(
        event_date="2026-04-15", type=EventType.DEPOSIT, amount=1_000_000, source="bank-a",
    ))
    ledger1.append(CashflowEvent(
        event_date="2026-04-20", type=EventType.WITHDRAWAL, amount=500_000, source="bank-a",
    ))
    s1 = compute_summary(ledger1, initial_capital=5_000_000)

    # Reload from disk in a fresh instance
    ledger2 = CashflowLedger(path=ledger_path)
    s2 = compute_summary(ledger2, initial_capital=5_000_000)

    assert s1 == s2, "replay must yield identical summary"
    # And reloading multiple times keeps yielding the same:
    s3 = compute_summary(CashflowLedger(path=ledger_path), initial_capital=5_000_000)
    assert s2 == s3


# ─── Test 5: malformed event fail-closed ─────────────────────────


def test_malformed_event_rejected_at_append(tmp_path: Path):
    ledger = CashflowLedger(path=tmp_path / "ledger.jsonl")

    # bad date format
    with pytest.raises(ValueError, match="event_date"):
        ledger.append(CashflowEvent(
            event_date="2026/04/15",  # wrong separator
            type=EventType.DEPOSIT, amount=100, source="x",
        ))

    # unknown type
    with pytest.raises(ValueError, match="unknown event type"):
        ledger.append(CashflowEvent(
            event_date="2026-04-15", type="BOGUS", amount=100, source="x",
        ))

    # negative amount on deposit (not allowed; use manual_adjustment instead)
    with pytest.raises(ValueError, match="amount must be positive"):
        ledger.append(CashflowEvent(
            event_date="2026-04-15", type=EventType.DEPOSIT, amount=-100, source="x",
        ))

    # zero manual_adjustment
    with pytest.raises(ValueError, match="manual_adjustment amount must be non-zero"):
        ledger.append(CashflowEvent(
            event_date="2026-04-15", type=EventType.MANUAL_ADJUSTMENT, amount=0, source="x",
        ))

    # non-KRW currency (CF1 scope)
    with pytest.raises(ValueError, match="KRW only"):
        ledger.append(CashflowEvent(
            event_date="2026-04-15", type=EventType.DEPOSIT, amount=100, source="x",
            currency="USD",
        ))


def test_malformed_line_in_ledger_fails_load(tmp_path: Path):
    p = tmp_path / "ledger.jsonl"
    # First a valid event, then a corrupted line
    p.write_text(
        json.dumps({
            "event_date": "2026-04-15", "type": "deposit", "amount": 1000000,
            "source": "x", "event_id": "e1", "currency": "KRW",
            "recorded_at": "2026-04-15T00:00:00+00:00", "recorded_by": "test", "note": "",
        }) + "\n"
        "{not valid json\n",
        encoding="utf-8",
    )
    ledger = CashflowLedger(path=p)
    with pytest.raises(ValueError, match="invalid JSON"):
        ledger.load()


def test_missing_required_field_in_ledger_fails_load(tmp_path: Path):
    p = tmp_path / "ledger.jsonl"
    p.write_text(json.dumps({"event_date": "2026-04-15"}) + "\n", encoding="utf-8")
    ledger = CashflowLedger(path=p)
    with pytest.raises(ValueError, match="missing required fields"):
        ledger.load()


# ─── Test 6: raw equity files untouched ──────────────────────────


def test_raw_equity_files_untouched_by_cf1_ops(tmp_path: Path):
    """Sanity guard: appending events / computing summaries does NOT
    modify any of the canonical raw equity / state files."""
    raw_paths = [
        REPO_ROOT / "kr" / "data" / "lab_live" / "equity.json",
        REPO_ROOT / "kr" / "data" / "lab_live" / "head.json",
    ]
    snapshots: dict[Path, bytes] = {}
    for p in raw_paths:
        if p.exists():
            snapshots[p] = p.read_bytes()

    # Run a CF1 workflow against an isolated tmp ledger
    ledger = CashflowLedger(path=tmp_path / "ledger.jsonl")
    ledger.append(CashflowEvent(
        event_date="2026-04-15", type=EventType.DEPOSIT, amount=1_000_000, source="x",
    ))
    ledger.append(CashflowEvent(
        event_date="2026-04-20", type=EventType.WITHDRAWAL, amount=500_000, source="x",
    ))
    compute_summary(ledger, initial_capital=5_000_000)

    for p, snap in snapshots.items():
        assert p.read_bytes() == snap, (
            f"CF1 ops mutated raw equity file: {p}. "
            f"raw equity must remain immutable in CF1."
        )


# ─── Test 7: deprecated path not imported ────────────────────────


def test_accounting_module_does_not_import_deprecated_capital_events():
    """Detect actual imports — bare `import X`, `from X import ...`. Allow
    doctrine docstrings to reference the deprecated path by name."""
    import re
    accounting_dir = REPO_ROOT / "kr" / "accounting"
    forbidden_import_patterns = [
        # `from finance.capital_events import ...`
        re.compile(r"^\s*from\s+(?:kr\.)?finance\.capital_events\b", re.MULTILINE),
        # `from finance._deprecated_capital_events import ...`
        re.compile(r"^\s*from\s+(?:kr\.)?finance\._deprecated_capital_events\b", re.MULTILINE),
        # `import finance.capital_events` / `import finance._deprecated_capital_events`
        re.compile(r"^\s*import\s+(?:kr\.)?finance\.(?:_deprecated_)?capital_events\b", re.MULTILINE),
    ]
    offenders: list[str] = []
    for f in accounting_dir.rglob("*.py"):
        text = f.read_text(encoding="utf-8")
        for pat in forbidden_import_patterns:
            for m in pat.finditer(text):
                line_no = text[: m.start()].count("\n") + 1
                offenders.append(f"{f.relative_to(REPO_ROOT)}:{line_no} matches {pat.pattern!r}")
    assert not offenders, (
        "Quarantine breach: kr/accounting/ must not import the "
        "deprecated capital_events module.\nOffenders:\n  " + "\n  ".join(offenders)
    )


# ─── Test 8: anti-pattern regression still passes ─────────────────


def test_anti_pattern_regression_still_passes():
    """Re-run the CF0 regression test as a child process so that adding
    kr/accounting/ does not reintroduce `adjust_equity` or related."""
    result = subprocess.run(
        [
            sys.executable,
            "-m", "pytest",
            "tests/test_no_raw_minus_cashflow_pattern.py",
            "-q", "--tb=short",
        ],
        cwd=str(REPO_ROOT / "kr"),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "CF0 anti-pattern regression failed after CF1 changes.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


# ─── Capital config tests (initial_capital protection) ────────────


def test_initial_capital_loaded_from_committed_state():
    """The committed kr/data/accounting/capital_state.json must hold 5M."""
    state_path = REPO_ROOT / "kr" / "data" / "accounting" / "capital_state.json"
    assert state_path.exists(), "CF1 must commit a default capital_state.json with 5M"
    cfg = load_capital_state(state_path)
    assert cfg.initial_capital == 5_000_000
    assert cfg.currency == "KRW"


def test_load_capital_state_fails_closed_on_missing(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_capital_state(tmp_path / "does_not_exist.json")


def test_load_capital_state_fails_closed_on_malformed(tmp_path: Path):
    bad = tmp_path / "bad.json"
    bad.write_text("{not valid json", encoding="utf-8")
    with pytest.raises(ValueError, match="invalid JSON"):
        load_capital_state(bad)


def test_load_capital_state_rejects_non_krw(tmp_path: Path):
    p = tmp_path / "bad_currency.json"
    p.write_text(json.dumps({"initial_capital": 1_000_000, "currency": "USD"}), encoding="utf-8")
    with pytest.raises(ValueError, match="KRW only"):
        load_capital_state(p)


def test_load_capital_state_rejects_non_positive(tmp_path: Path):
    p = tmp_path / "neg.json"
    p.write_text(json.dumps({"initial_capital": 0, "currency": "KRW"}), encoding="utf-8")
    with pytest.raises(ValueError, match="must be positive"):
        load_capital_state(p)
