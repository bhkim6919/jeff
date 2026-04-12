"""
Gen4 Pre-launch Verification Tests
====================================
6 items from the production readiness checklist.
Run: python test_prelaunch.py
     pytest test_prelaunch.py
"""
import sys
import os
import re
import inspect
from pathlib import Path
from datetime import date, datetime, timedelta

# Ensure Gen04 is importable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import Gen4Config
from strategy.scoring import score_universe
from core.portfolio_manager import PortfolioManager


def test_prelaunch():
    cfg = Gen4Config()

    # ── Test 1: scoring.py mom_skip default = 22 ──
    sig = inspect.signature(score_universe)
    default_skip = sig.parameters["mom_skip"].default
    assert default_skip == 22, f"score_universe mom_skip default: got {default_skip}"
    assert cfg.MOM_SKIP == 22, f"config.MOM_SKIP: got {cfg.MOM_SKIP}"
    assert default_skip == cfg.MOM_SKIP, \
        f"defaults mismatch: scoring={default_skip}, config={cfg.MOM_SKIP}"

    # ── Test 2: rebalance date-diff logic ──
    def sim_need_rebalance(last_rebal_str, today_dt, rebal_days=21):
        """Simulates the new rebalance check logic from main.py."""
        if not last_rebal_str:
            return True
        try:
            last_dt = datetime.strptime(last_rebal_str, "%Y%m%d").date()
            days_since = (today_dt - last_dt).days
            return days_since >= rebal_days
        except (ValueError, TypeError):
            return True  # safety fallback

    today = date(2026, 3, 23)

    assert sim_need_rebalance("20260313", today) == False, \
        "10 days ago -> should be no rebalance"
    assert sim_need_rebalance("20260302", today) == True, \
        "21 days ago -> should rebalance"
    assert sim_need_rebalance("20260301", today) == True, \
        "22 days ago -> should rebalance"
    assert sim_need_rebalance("", today) == True, \
        "empty last_rebal -> should rebalance"
    assert sim_need_rebalance("not-a-date", today) == True, \
        "garbled last_rebal -> should rebalance (fallback)"
    assert sim_need_rebalance("20260303", today) == False, \
        "20 days ago -> should be no rebalance"

    # ── Test 3: EOD timing ──
    import main as main_mod

    src = Path(__file__).resolve().parent / "main.py"
    src_text = src.read_text(encoding="utf-8")

    assert "MONITOR_END_HOUR, MONITOR_END_MIN = 15, 20" in src_text, \
        "MONITOR_END defined as 15:20 not found in main.py"
    assert "EOD_EVAL_HOUR, EOD_EVAL_MIN = 15, 30" in src_text, \
        "EOD_EVAL defined as 15:30 not found in main.py"
    assert "eod_target = now.replace(hour=EOD_EVAL_HOUR" in src_text, \
        "EOD wait logic not found"

    t1525 = datetime(2026, 3, 23, 15, 25, 0)
    eod_target = t1525.replace(hour=15, minute=30, second=0)
    assert t1525 < eod_target, f"15:25 -> EOD not yet: t1525={t1525}, eod={eod_target}"
    t1530 = datetime(2026, 3, 23, 15, 30, 0)
    assert t1530 >= eod_target, "15:30 -> EOD should proceed"
    t1531 = datetime(2026, 3, 23, 15, 31, 0)
    assert t1531 >= eod_target, "15:31 -> EOD should proceed"

    # ── Test 4: price=0 handling ──
    assert "price_fail_codes = []" in src_text, \
        "price_fail_codes tracking list not found"
    assert 'logger.warning(f"Price fetch failed: {code}' in src_text, \
        "price failure warning log not found"
    assert 'price-failed codes' in src_text, \
        "final skip summary log not found"

    # ── Test 5: stale/missing target ──
    assert hasattr(cfg, "TARGET_MAX_STALE_DAYS"), \
        "TARGET_MAX_STALE_DAYS attribute not found"
    assert cfg.TARGET_MAX_STALE_DAYS == 3, \
        f"TARGET_MAX_STALE_DAYS: got {getattr(cfg, 'TARGET_MAX_STALE_DAYS', 'N/A')}"
    assert "No target portfolio! Skipping rebalance" in src_text, \
        "missing target guard not found"
    assert "Target is STALE" in src_text, \
        "stale target guard not found"
    assert "Rebalance SKIPPED" in src_text and "monitor-only" in src_text, \
        "monitor-only fallback not found"

    # ── Test 6: PortfolioManager init consistency ──
    pm_live = PortfolioManager(
        cfg.INITIAL_CASH, cfg.DAILY_DD_LIMIT,
        cfg.MONTHLY_DD_LIMIT, cfg.N_STOCKS)
    pm_mock = PortfolioManager(
        cfg.INITIAL_CASH, cfg.DAILY_DD_LIMIT,
        cfg.MONTHLY_DD_LIMIT, cfg.N_STOCKS)

    assert pm_live.daily_dd_limit == pm_mock.daily_dd_limit == cfg.DAILY_DD_LIMIT, \
        f"daily_dd_limit: live={pm_live.daily_dd_limit}, mock={pm_mock.daily_dd_limit}"
    assert pm_live.monthly_dd_limit == pm_mock.monthly_dd_limit == cfg.MONTHLY_DD_LIMIT, \
        f"monthly_dd_limit: live={pm_live.monthly_dd_limit}, mock={pm_mock.monthly_dd_limit}"
    assert pm_live.max_positions == pm_mock.max_positions == cfg.N_STOCKS, \
        f"max_positions: live={pm_live.max_positions}, mock={pm_mock.max_positions}"

    assert (
        'PortfolioManager(\n        config.INITIAL_CASH, config.DAILY_DD_LIMIT,\n        config.MONTHLY_DD_LIMIT, config.N_STOCKS)' in src_text
        or 'PortfolioManager(\n        config.INITIAL_CASH, config.DAILY_DD_LIMIT,\n        config.MONTHLY_DD_LIMIT, config.N_STOCKS)' in src_text.replace('\r\n', '\n')
    ), "run_mock uses full PM args in source: mock PM init doesn't match live"

    # ── Bonus: log step numbers ──
    steps = re.findall(r'\[(\d+)/(\d+)\]', src_text)
    batch_steps = [(n, d) for n, d in steps if d in ('4', '5')]
    assert all(d == '5' for _, d in batch_steps), \
        f"all batch log steps should use /5, found: {batch_steps}"


if __name__ == "__main__":
    test_prelaunch()
    print("ALL PASS")
