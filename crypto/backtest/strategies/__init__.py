"""D4 + D5 backtest strategies.

PR #2 shipped ``Momentum12_1`` as the engine-validation strategy.
PR #3 added ``SMA50_200Trend`` and ``ATRBreakout`` plus a ``BTCHodl``
benchmark for G8 sanity comparison.
D5 STEP 1 (PR #19) added ``Donchian20DBreakout`` — the third Core trend
strategy.
D5 STEP 2 adds ``RSI2MeanReversion`` — the first counter-trend D5 entry,
intended to verify deterministic execution of a low-correlation factor
inside the same rebal-cycle engine.

All strategies share the same contract (``Strategy`` ABC in ``base.py``):
they MUST be deterministic (no random tiebreakers), MUST return a sorted
list of pairs, and MUST declare a ``lookback_days`` so the engine retreats
the start date for warmup (Jeff E6=A).

D5 strategy roster (post-STEP 2):
    momentum_12_1        — engine canary / G6 regression target only (Jeff F4=C).
                           NOT a survivor candidate.
    sma_50_200_trend     — D5 survivor candidate (50/200 trend continuation).
    atr_breakout         — D5 survivor candidate (volatility breakout).
    donchian_20d         — D5 survivor candidate (20-day fresh-cross channel).
    rsi2_mean_reversion  — D5 survivor candidate (counter-trend, oversold bounce).
    btc_hodl             — benchmark only (G8 sanity).

Per Jeff D5 lock:
    - LowVol+Mom and Volatility Pullback are NOT in this module yet.
      Volatility Pullback ships in STEP 3 (experimental tag); the rest
      live in D6+.
    - Regime Switching MUST NOT be added to D5. Its Meta Layer slot is
      reserved for D6.
    - No parameter tuning of any existing default.
    - No strategy-internal risk control (SL/TP/time-based / DD guard /
      position sizing): the engine and any future portfolio overlay own
      that responsibility.
"""
