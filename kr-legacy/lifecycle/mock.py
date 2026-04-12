"""
Mock mode entry point extracted from main.py.
"""
from __future__ import annotations
import logging

from lifecycle.utils import _safe_save


def run_mock(config):
    """Mock mode: test state + target + reporter without broker."""
    from core.state_manager import StateManager
    from core.portfolio_manager import PortfolioManager
    from strategy.factor_ranker import load_target_portfolio
    from report.reporter import TradeLogger

    trading_mode = "mock"
    logger = logging.getLogger("gen4.mock")
    logger.info("=" * 60)
    logger.info("  Gen4 Mock Mode")
    logger.info(f"  [TRADING_MODE] {trading_mode}")
    logger.info("=" * 60)

    state_mgr = StateManager(config.STATE_DIR, trading_mode=trading_mode)
    portfolio = PortfolioManager(
        config.INITIAL_CASH, config.DAILY_DD_LIMIT,
        config.MONTHLY_DD_LIMIT, config.N_STOCKS)
    trade_logger = TradeLogger(config.REPORT_DIR)

    saved = state_mgr.load_portfolio()
    if saved:
        portfolio.restore_from_dict(saved, buy_cost=config.BUY_COST)
        logger.info(f"Restored: {len(portfolio.positions)} positions")

    target = load_target_portfolio(config.SIGNALS_DIR)
    if target:
        logger.info(f"Target: {len(target['target_tickers'])} stocks (date: {target.get('date', '?')})")
        for i, tk in enumerate(target["target_tickers"][:5], 1):
            s = target["scores"].get(tk, {})
            logger.info(f"  {i}. {tk}  mom={s.get('mom_12_1',0):.4f}")
        if len(target["target_tickers"]) > 5:
            logger.info(f"  ... and {len(target['target_tickers'])-5} more")
    else:
        logger.info("No target portfolio. Run --batch first.")

    summary = portfolio.summary()
    logger.info(f"Portfolio: equity={summary['equity']:,.0f}, "
                 f"cash={summary['cash']:,.0f}, pos={summary['n_positions']}, "
                 f"risk={summary['risk_mode']}")

    # Log equity snapshot
    trade_logger.log_equity(
        summary["equity"], summary["cash"], summary["n_positions"],
        summary["daily_pnl"], summary["monthly_dd"])

    _safe_save(state_mgr, portfolio, context="mock_complete")
    logger.info("Mock complete.")
