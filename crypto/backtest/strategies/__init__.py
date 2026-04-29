"""D4 + D5 backtest strategies.

PR #2 shipped ``Momentum12_1`` as the engine-validation strategy.
PR #3 added ``SMA50_200Trend`` and ``ATRBreakout`` plus a ``BTCHodl``
benchmark for G8 sanity comparison.
D5 STEP 1 (PR #19) added ``Donchian20DBreakout`` — the third Core trend
strategy.
D5 STEP 2 (PR #21) added ``RSI2MeanReversion`` — the first counter-
trend D5 entry, intended to verify deterministic execution of a low-
correlation factor inside the same rebal-cycle engine.
D5 STEP 3 adds ``VolatilityPullbackExperimental`` — an event-driven
pullback re-entry strategy. The ``_experimental`` suffix is intentional:
the strategy's intended 2~4 day horizon is structurally incompatible
with the engine's 21-day rebal cycle, so STEP 3 functions as an
**engine boundary stress test**, not a strategy alpha evaluation. See
the strategy module's docstring and PR #23 description for the full
disclaimer.

All strategies share the same contract (``Strategy`` ABC in ``base.py``):
they MUST be deterministic (no random tiebreakers), MUST return a sorted
list of pairs, and MUST declare a ``lookback_days`` so the engine retreats
the start date for warmup (Jeff E6=A).

D5 strategy roster (post-STEP 3):
    momentum_12_1                         — engine canary / G6 regression
                                            (Jeff F4=C). NOT a survivor candidate.
    sma_50_200_trend                      — survivor candidate (50/200 trend).
    atr_breakout                          — survivor candidate (vol breakout).
    donchian_20d                          — survivor candidate (20-day channel).
    rsi2_mean_reversion                   — survivor candidate (oversold bounce).
    volatility_pullback_experimental      — EXPERIMENTAL (engine boundary test).
                                            Tag in name AND docstring per Jeff F21=C.
    btc_hodl                              — benchmark only (G8 sanity).

Per Jeff D5 lock:
    - LowVol+Mom roster lives in D6+. Do not add here.
    - Regime Switching MUST NOT be added to D5. Reserved for D6 Meta Layer.
    - No parameter tuning of any existing default.
    - No strategy-internal risk control (SL / TP / time-based / DD guard /
      position sizing): the engine and any future portfolio overlay own
      that responsibility.
"""
