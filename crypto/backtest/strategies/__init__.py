"""D4 backtest strategies.

PR #2 shipped ``Momentum12_1`` as the engine-validation strategy.
PR #3 adds ``SMA50_200Trend`` and ``ATRBreakout`` plus a ``BTCHodl``
benchmark for G8 sanity comparison.

All strategies share the same contract (``Strategy`` ABC in ``base.py``):
they MUST be deterministic (no random tiebreakers), MUST return a sorted
list of pairs, and MUST declare a ``lookback_days`` so the engine retreats
the start date for warmup (Jeff E6=A).

Per Jeff PR #3 lock:
    - PR #3 stops at three trading strategies (Momentum + SMA + ATR) plus
      one benchmark (BTC HODL).
    - LowVol+Mom, Trend Following, and the remaining D5 roster MUST NOT
      be added here. They live in D5.
    - No parameter tuning of the existing strategies.
"""
