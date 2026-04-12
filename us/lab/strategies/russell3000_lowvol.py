# -*- coding: utf-8 -*-
"""
russell3000_lowvol.py — [EXPERIMENTAL] Gen4 Core on Russell 3000
=================================================================
EXPERIMENTAL group | 21-day rebalance | Trail -12% | 20 positions
Same as lowvol_momentum but on Russell 3000 universe (includes small caps).
"""
from __future__ import annotations

from .lowvol_momentum import LowvolMomentumStrategy


class Russell3000LowvolStrategy(LowvolMomentumStrategy):
    """Identical to LowvolMomentum — the difference is the universe (R3000)."""
    name = "russell3000_lowvol"
