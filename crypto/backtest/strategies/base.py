"""Strategy ABC for the D4 backtester.

Determinism contract (Jeff D4 보완 #4 + G6):
    * ``select`` MUST return a sorted list of pair strings.
    * Tiebreakers MUST be deterministic (e.g. score desc, pair asc).
    * No random number usage anywhere in the call path.

Lookback contract (Jeff E6=A):
    Each subclass declares ``lookback_days``. The engine retreats the
    backtest start so the strategy always has a full lookback window
    before the first rebalance. NaN within the window is the strategy's
    responsibility — typically: skip the pair on this rebalance (E5=A).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from crypto.backtest.data_loader import OhlcvLoader


class Strategy(ABC):
    """Base class — every strategy MUST be deterministic + lookback-aware."""

    name: str = "abstract"
    lookback_days: int = 0  # subclasses override

    @abstractmethod
    def select(
        self,
        *,
        asof: date,
        universe: list[str],
        loader: OhlcvLoader,
        top_n: int,
    ) -> list[str]:
        """Return up to ``top_n`` pairs in deterministic order.

        Args:
            asof:    Signal date — the strategy reads OHLCV up to and
                     including this day. The engine passes ``trade_date - 1``
                     per Jeff E1=C (no-lookahead).
            universe: Already-survivorship-filtered list of pairs (sorted).
            loader:  Per-pair OHLCV loader (no matrix ffill).
            top_n:   Engine's max position count (D4: 20).

        Returns:
            list[str] — up to ``top_n`` pairs, sorted ascending by pair name
            for stable ordering across runs.
        """
