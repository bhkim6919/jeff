"""
theme_regime.py — Theme Regime Tracker (ka90001 기반)
=====================================================
PostgreSQL 단일 DB 접근. sqlite3 사용 금지.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import List, Optional

from shared.db.pg_base import connection
from shared.db.run_id import now_utc

logger = logging.getLogger("gen4.regime.theme")

BULL_THRESHOLD = 1.0
BEAR_THRESHOLD = -1.0


class ThemeRegimeTracker:
    """Collect theme data via ka90001, classify regime, track streaks."""

    def __init__(self, provider, db_path=None):
        self._provider = provider
        # db_path 인자는 하위호환용으로 무시 (PG 사용)

    def _classify(self, change_pct: float) -> str:
        if change_pct > BULL_THRESHOLD:
            return "BULL"
        elif change_pct < BEAR_THRESHOLD:
            return "BEAR"
        return "SIDEWAYS"

    def _calc_streak(self, theme_code: str, today_regime: str, today_str: str) -> int:
        """이전 연속일 계산."""
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT regime FROM regime_theme_daily "
                "WHERE theme_code=%s AND market_date<%s "
                "ORDER BY market_date DESC LIMIT 30",
                (theme_code, today_str),
            )
            rows = cur.fetchall()
            cur.close()

        streak = 1
        for r in rows:
            if r[0] == today_regime:
                streak += 1
            else:
                break
        return streak

    def collect_and_classify(self, top_n: int = 20) -> List[dict]:
        """ka90001 호출 → 분류 → DB 저장 → 반환."""
        themes = self._provider.get_theme_groups(date_range=1)
        if not themes:
            logger.warning("[THEME_REGIME] ka90001 returned empty")
            return self._load_latest()

        themes.sort(key=lambda t: t.get("change_pct", 0), reverse=True)
        top_themes = themes[:top_n]

        today_str = date.today().strftime("%Y-%m-%d")
        run_ts = now_utc()
        results = []

        with connection() as conn:
            cur = conn.cursor()
            for t in top_themes:
                regime = self._classify(t["change_pct"])
                streak = self._calc_streak(t["code"], regime, today_str)
                entry = {
                    "code": t["code"], "name": t["name"],
                    "count": t["count"], "change_pct": t["change_pct"],
                    "regime": regime, "streak": streak,
                }
                results.append(entry)

                cur.execute("""
                    INSERT INTO regime_theme_daily
                    (market_date, theme_code, theme_name, stock_count,
                     change_pct, regime, streak_days, run_ts, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (market_date, theme_code) DO UPDATE SET
                        theme_name = EXCLUDED.theme_name,
                        stock_count = EXCLUDED.stock_count,
                        change_pct = EXCLUDED.change_pct,
                        regime = EXCLUDED.regime,
                        streak_days = EXCLUDED.streak_days,
                        run_ts = EXCLUDED.run_ts
                    WHERE regime_theme_daily.run_ts < EXCLUDED.run_ts
                """, (
                    today_str, t["code"], t["name"], t["count"],
                    t["change_pct"], regime, streak, run_ts,
                    datetime.now().isoformat(),
                ))

            conn.commit()
            cur.close()

        results.sort(key=lambda r: r["change_pct"], reverse=True)
        logger.info(
            f"[THEME_REGIME] {len(results)} themes classified "
            f"(BULL={sum(1 for r in results if r['regime']=='BULL')}, "
            f"BEAR={sum(1 for r in results if r['regime']=='BEAR')})"
        )
        return results

    def _load_latest(self) -> List[dict]:
        """PG에서 오늘 데이터 로드 (API 실패 시 fallback)."""
        today_str = date.today().strftime("%Y-%m-%d")
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT theme_code, theme_name, stock_count, change_pct, "
                "regime, streak_days FROM regime_theme_daily "
                "WHERE market_date=%s ORDER BY change_pct DESC",
                (today_str,),
            )
            rows = cur.fetchall()
            cur.close()
        return [{
            "code": r[0], "name": r[1], "count": r[2],
            "change_pct": r[3], "regime": r[4], "streak": r[5],
        } for r in rows]

    def load_history(self, theme_code: str, days: int = 30) -> List[dict]:
        """특정 테마의 최근 N일 이력."""
        with connection() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT * FROM regime_theme_daily WHERE theme_code=%s "
                "ORDER BY market_date DESC LIMIT %s",
                (theme_code, days),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            cur.close()
        return [dict(zip(cols, r)) for r in rows]
