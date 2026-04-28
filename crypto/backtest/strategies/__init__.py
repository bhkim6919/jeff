"""D4 backtest strategies.

PR #2 ships ``Momentum12_1`` as the engine-validation strategy.
PR #3 will add ``LowVolMomentum`` and ``TrendFollowing``.

All strategies share the same contract (``Strategy`` ABC in ``base.py``):
they MUST be deterministic (no random tiebreakers), MUST return a sorted
list of pairs, and MUST declare a ``lookback_days`` so the engine retreats
the start date for warmup (Jeff E6=A).
"""
