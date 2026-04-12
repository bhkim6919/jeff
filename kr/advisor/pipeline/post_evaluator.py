"""Post-Apply Evaluator - track override performance at 5d/20d checkpoints."""
from __future__ import annotations

from ..ingestion.schema import SnapshotWindow


class PostApplyEvaluator:
    """Evaluate override performance after application.

    Checkpoints at 5 and 20 trading days.
    Verdicts: KEEP / REVIEW / ROLLBACK
    Auto-rollback is forbidden - verdict is advisory only.
    """

    CHECKPOINTS = [5, 20]

    def evaluate(self, override: dict,
                 pre_window: SnapshotWindow,
                 post_window: SnapshotWindow) -> dict:
        """Compare pre vs post override performance.

        Args:
            override: Applied override dict.
            pre_window: N-day window before override.
            post_window: N-day window after override.
        """
        pre = self._calc_metrics(pre_window)
        post = self._calc_metrics(post_window)

        delta = {
            "mdd_delta": post["mdd"] - pre["mdd"],
            "avg_daily_pnl_delta": post["avg_daily_pnl"] - pre["avg_daily_pnl"],
            "trade_count_delta": post["trade_count"] - pre["trade_count"],
            "win_rate_delta": post["win_rate"] - pre["win_rate"],
        }

        # Verdict
        if delta["mdd_delta"] < -0.03:
            verdict = "ROLLBACK"
        elif delta["avg_daily_pnl_delta"] < -0.001:
            verdict = "REVIEW"
        else:
            verdict = "KEEP"

        return {
            "override_id": override.get("recommendation_id", ""),
            "parameter": override.get("parameter", ""),
            "checkpoint_days": post_window.valid_count,
            "pre_metrics": pre,
            "post_metrics": post,
            "delta": delta,
            "verdict": verdict,
        }

    def _calc_metrics(self, window: SnapshotWindow) -> dict:
        daily_pnls = []
        closes = []

        for s, v in zip(window.snapshots, window.valid_mask):
            if not v:
                continue
            if s.equity:
                pnl = _sf(s.equity.get("daily_pnl_pct", "0"))
                daily_pnls.append(pnl)
            for c in s.closes:
                closes.append(_sf(c.get("pnl_pct", "0")))

        n = len(daily_pnls)
        avg_pnl = sum(daily_pnls) / n if n else 0

        # Simple MDD from daily PnL series
        peak = 0
        mdd = 0
        cumulative = 1.0
        for p in daily_pnls:
            cumulative *= (1 + p)
            peak = max(peak, cumulative)
            dd = (cumulative / peak - 1) if peak > 0 else 0
            mdd = min(mdd, dd)

        wins = sum(1 for c in closes if c > 0)
        win_rate = wins / len(closes) if closes else 0

        return {
            "days": n,
            "avg_daily_pnl": avg_pnl,
            "mdd": mdd,
            "trade_count": len(closes),
            "win_rate": win_rate,
        }


def _sf(v) -> float:
    try:
        return float(str(v).strip() or "0")
    except (ValueError, TypeError):
        return 0.0
