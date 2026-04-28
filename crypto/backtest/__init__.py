"""Crypto Lab D4 backtester.

Per Jeff D4 GO (2026-04-28, conditional approval):

    - cost_model.py is the single source of truth for fees + slippage.
      validate / backtest / simulation must all import from here.
    - Pre-listing dates are excluded by the universe; per-pair NaN within
      data is preserved verbatim. Matrix-wide forward-fill is forbidden.
    - Survivorship is enforced via crypto_listings — a pair is in the
      universe on date D iff (listed_at IS NULL or listed_at <= D) AND
      (delisted_at IS NULL or delisted_at > D).
    - D4 universe is *KRW Top 100 static* — engine validation only, not a
      strategy-quality claim. Dynamic universes are reserved for D5.
    - Rebalance cadence is "21 crypto days" — there are no holidays in
      crypto, so "거래일" terminology is forbidden.

D4 deliverables ship as three sequenced PRs:
    PR #1 foundation (this PR) — cost_model, universe, data_loader, portfolio
    PR #2 engine + Momentum 12-1
    PR #3 LowVol+Mom + Trend Following + integration verifier
"""
