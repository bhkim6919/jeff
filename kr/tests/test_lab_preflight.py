"""test_lab_preflight.py — Lab Live import self-test verification.

Coverage matrix (Jeff 2026-04-27 P0):
  1. All required modules present → ok=True, missing=[]
  2. One required module raises ImportError (mocked) → ok=False, named in missing
  3. Cache idempotency: second call returns same dict object without re-importing
  4. reset_cache() clears, next call re-runs
  5. is_blocking_failure() pre-run vs post-run-OK vs post-run-FAIL
  6. Telegram fired once on failure, suppressed on second call
  7. is_disabled() honours QTRON_LAB_PREFLIGHT env var

The check exercised here is purely the preflight module's behaviour
under normal Python; no FastAPI app, no tray, no network. Wire-in tests
(create_app calling run_preflight, /api/health surfacing the result)
belong in an integration test layer that boots uvicorn.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Match the layout used by test_state_v2.py: insert kr/ then bootstrap
# the project root so `shared.*` imports resolve.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import _bootstrap_path  # noqa: F401  -- side-effect

from web.lab_live import preflight  # noqa: E402


def test_1_happy_path_all_imports_succeed():
    """With the real lab_live package intact, every required module loads."""
    preflight.reset_cache()
    result = preflight.run_preflight(fire_telegram=False)
    assert result["ok"] is True, f"errors: {result['errors']}"
    assert result["missing"] == []
    assert set(result["modules"].keys()) == set(preflight.REQUIRED_MODULES)
    assert all(v == "OK" for v in result["modules"].values())
    print(f"  PASS: {len(preflight.REQUIRED_MODULES)} modules OK")


def test_2_simulated_module_failure():
    """Inject an ImportError for one required module → it appears in missing."""
    preflight.reset_cache()

    # Hijack importlib.import_module via the preflight module's reference.
    # Targets a *single* module so the rest still report OK.
    original = preflight.importlib.import_module
    target = "web.lab_live.market_context"

    def fake_import(name, *a, **kw):
        if name == target:
            raise ModuleNotFoundError(f"No module named '{name}' (simulated)")
        return original(name, *a, **kw)

    preflight.importlib.import_module = fake_import
    try:
        result = preflight.run_preflight(fire_telegram=False)
    finally:
        preflight.importlib.import_module = original

    assert result["ok"] is False
    assert target in result["missing"]
    assert result["modules"][target] == "FAIL"
    assert "simulated" in result["errors"][target]
    # Other modules should still be OK so missing list is exact, not avalanche.
    other_ok = [m for m in preflight.REQUIRED_MODULES if m != target]
    for m in other_ok:
        assert result["modules"][m] == "OK", (
            f"unexpected collateral failure on {m}: {result['errors'].get(m)}"
        )
    print(f"  PASS: {target} marked FAIL, others OK")


def test_3_cache_idempotency():
    """Second call returns the SAME cached result object — no re-import."""
    preflight.reset_cache()
    first = preflight.run_preflight(fire_telegram=False)
    second = preflight.run_preflight(fire_telegram=False)
    assert first is second, "cache should return the same dict, not re-run"
    print("  PASS: cache returns same object across calls")


def test_4_reset_cache_forces_rerun():
    preflight.reset_cache()
    a = preflight.run_preflight(fire_telegram=False)
    preflight.reset_cache()
    b = preflight.run_preflight(fire_telegram=False)
    assert a is not b, "reset_cache should force a fresh run"
    # Content equivalent (same env), object identity differs.
    assert a["ok"] == b["ok"]
    print("  PASS: reset_cache forces rerun")


def test_5_is_blocking_failure_states():
    """Pre-run = False (don't block on unknown), post-OK = False, post-FAIL = True."""
    preflight.reset_cache()
    assert preflight.is_blocking_failure() is False, (
        "must NOT block before preflight has run — that is the boot order"
    )

    preflight.run_preflight(fire_telegram=False)
    assert preflight.is_blocking_failure() is False, "OK run should not block"

    preflight.reset_cache()
    original = preflight.importlib.import_module
    def fake_import(name, *a, **kw):
        if name == "web.lab_live.engine":
            raise ImportError("simulated engine break")
        return original(name, *a, **kw)
    preflight.importlib.import_module = fake_import
    try:
        preflight.run_preflight(fire_telegram=False)
    finally:
        preflight.importlib.import_module = original

    assert preflight.is_blocking_failure() is True, "FAIL run must block"
    print("  PASS: blocking states match cache state")


def test_6_telegram_fires_once():
    """Telegram alert is fired on first failed run, suppressed on second."""
    preflight.reset_cache()

    fired = {"n": 0, "last": None}

    # Override _fire_telegram_critical so we don't actually attempt
    # network calls and so we can count invocations.
    original_fire = preflight._fire_telegram_critical

    def mock_fire(result):
        fired["n"] += 1
        fired["last"] = result

    preflight._fire_telegram_critical = mock_fire

    original_import = preflight.importlib.import_module
    def fake_import(name, *a, **kw):
        if name == "web.lab_live.daily_drivers":
            raise ImportError("simulated")
        return original_import(name, *a, **kw)
    preflight.importlib.import_module = fake_import

    try:
        preflight.run_preflight(fire_telegram=True)
        # Cache hit on this second call — _fire_telegram_critical must
        # NOT be called again even if we still pass fire_telegram=True.
        preflight.run_preflight(fire_telegram=True)
    finally:
        preflight.importlib.import_module = original_import
        preflight._fire_telegram_critical = original_fire

    assert fired["n"] == 1, f"expected 1 telegram fire, got {fired['n']}"
    assert fired["last"]["ok"] is False
    print("  PASS: telegram fired exactly once on first failure")


def test_7_disable_via_env_var():
    """QTRON_LAB_PREFLIGHT=0 disables the check (operator escape hatch)."""
    saved = os.environ.get("QTRON_LAB_PREFLIGHT")
    try:
        for v in ("0", "false", "False"):
            os.environ["QTRON_LAB_PREFLIGHT"] = v
            assert preflight.is_disabled() is True, f"value '{v}' should disable"
        for v in ("1", "true", "", "anything"):
            os.environ["QTRON_LAB_PREFLIGHT"] = v
            assert preflight.is_disabled() is False, f"value '{v}' should NOT disable"
        # Default: not set → not disabled
        os.environ.pop("QTRON_LAB_PREFLIGHT", None)
        assert preflight.is_disabled() is False
    finally:
        if saved is None:
            os.environ.pop("QTRON_LAB_PREFLIGHT", None)
        else:
            os.environ["QTRON_LAB_PREFLIGHT"] = saved
    print("  PASS: env var override behaves correctly")


def main():
    tests = [
        ("1. Happy path — all imports succeed", test_1_happy_path_all_imports_succeed),
        ("2. Simulated module failure", test_2_simulated_module_failure),
        ("3. Cache idempotency", test_3_cache_idempotency),
        ("4. reset_cache forces rerun", test_4_reset_cache_forces_rerun),
        ("5. is_blocking_failure states", test_5_is_blocking_failure_states),
        ("6. Telegram fires once", test_6_telegram_fires_once),
        ("7. Disable via env var", test_7_disable_via_env_var),
    ]

    passed, failed = 0, 0
    for name, fn in tests:
        print(f"\n=== TEST {name} ===")
        try:
            fn()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'=' * 40}")
    print(f"RESULTS: {passed}/{passed + failed} passed")
    if failed:
        sys.exit(1)
    else:
        print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
