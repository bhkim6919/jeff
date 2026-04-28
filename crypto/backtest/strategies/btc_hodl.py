"""BTC HODL benchmark — single-pair always-pick strategy.

Picks ``["KRW-BTC"]`` at every rebal as long as the universe contains it
on the signal date. The engine equal-weights to N=1, so first rebal opens
the full BTC position; subsequent rebals re-target to the same qty (modulo
tiny cash-dust drift from fees), producing a near-pure HODL equity curve.

Why a strategy and not a separate benchmark function:
    Going through the engine pipeline ensures we get the same six-metric
    output, the same equity curve schema, and the same canonical hash
    treatment as the trading strategies — apples-to-apples comparison
    for G8 sanity checks. The minor cash-dust drip is constant across
    runs (deterministic) and therefore does not pollute G6 idempotency.

NaN handling:
    If KRW-BTC is missing from the asof universe (delisted or pre-listed),
    return ``[]`` — engine smart-rebal will close any held BTC the next
    time it has a quote. This shouldn't happen for the verifier window
    (KRW-BTC is in the static Top 100 since 2017) but the guard is here
    to avoid hard failures during sparse-universe checks.

Per Jeff PR #3 lock: this is a benchmark only. NOT a tradable strategy.
"""

from __future__ import annotations

from datetime import date

from crypto.backtest.data_loader import OhlcvLoader
from crypto.backtest.strategies.base import Strategy


_BENCHMARK_PAIR = "KRW-BTC"


class BTCHodl(Strategy):
    """Always picks [KRW-BTC] when it is in the asof universe."""

    name = "btc_hodl"
    lookback_days = 1

    def select(
        self,
        *,
        asof: date,
        universe: list[str],
        loader: OhlcvLoader,
        top_n: int,
    ) -> list[str]:
        if _BENCHMARK_PAIR in universe:
            return [_BENCHMARK_PAIR]
        return []
