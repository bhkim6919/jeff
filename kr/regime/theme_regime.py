"""
theme_regime.py — Theme Regime Tracker (ka90001 기반)
=====================================================
키움 REST ka90001 테마그룹조회로 실시간 테마 등락률을 수집하고
BULL/SIDEWAYS/BEAR 판정 + 연속일 추적.

Usage:
    from regime.theme_regime import ThemeRegimeTracker
    tracker = ThemeRegimeTracker(provider, db_path)
    themes = tracker.collect_and_classify()
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("gen4.regime.theme")

# 레짐 판정 기준 (등락률 %)
BULL_THRESHOLD = 1.0    # > +1% → BULL
BEAR_THRESHOLD = -1.0   # < -1% → BEAR


class ThemeRegimeTracker:
    """Collect theme data via ka90001, classify regime, track streaks."""

    def __init__(self, provider, db_path: Optional[Path] = None):
        self._provider = provider
        self._db_path = db_path or Path(__file__).parent.parent / "data" / "regime" / "theme_regime.db"
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_db()

    def _ensure_db(self):
        conn = sqlite3.connect(str(self._db_path))
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS theme_daily (
                    market_date TEXT NOT NULL,
                    theme_code TEXT NOT NULL,
                    theme_name TEXT,
                    stock_count INTEGER DEFAULT 0,
                    change_pct REAL DEFAULT 0,
                    regime TEXT DEFAULT 'SIDEWAYS',
                    streak_days INTEGER DEFAULT 0,
                    created_at TEXT,
                    PRIMARY KEY (market_date, theme_code)
                );
                CREATE INDEX IF NOT EXISTS idx_theme_date ON theme_daily(market_date);
            """)
            conn.commit()
        finally:
            conn.close()

    def _conn(self):
        c = sqlite3.connect(str(self._db_path))
        c.row_factory = sqlite3.Row
        return c

    def _classify(self, change_pct: float) -> str:
        if change_pct > BULL_THRESHOLD:
            return "BULL"
        elif change_pct < BEAR_THRESHOLD:
            return "BEAR"
        return "SIDEWAYS"

    def _calc_streak(self, theme_code: str, today_regime: str, today_str: str) -> int:
        """이전 연속일 계산. 같은 레짐이 며칠째 지속되는지."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT regime FROM theme_daily "
                "WHERE theme_code=? AND market_date<? "
                "ORDER BY market_date DESC LIMIT 30",
                (theme_code, today_str),
            ).fetchall()
            streak = 1  # 오늘 포함
            for r in rows:
                if r["regime"] == today_regime:
                    streak += 1
                else:
                    break
            return streak
        finally:
            conn.close()

    def collect_and_classify(self, top_n: int = 20) -> List[dict]:
        """
        ka90001 호출 → 상위 N개 테마 분류 → DB 저장 → 결과 반환.

        Returns:
            [{"code", "name", "count", "change_pct", "regime", "streak"}, ...]
        """
        # 1) ka90001 호출
        themes = self._provider.get_theme_groups(date_range=1)
        if not themes:
            logger.warning("[THEME_REGIME] ka90001 returned empty")
            return self._load_latest()

        # 2) 등락률 높은 순 상위 N개 (상승 테마 우선, 실시간 순서 변동)
        themes.sort(key=lambda t: t.get("change_pct", 0), reverse=True)
        top_themes = themes[:top_n]

        # 3) 분류 + 연속일 + DB 저장
        today_str = date.today().strftime("%Y-%m-%d")
        now_str = datetime.now().isoformat()
        results = []
        conn = self._conn()
        try:
            for t in top_themes:
                regime = self._classify(t["change_pct"])
                streak = self._calc_streak(t["code"], regime, today_str)
                entry = {
                    "code": t["code"],
                    "name": t["name"],
                    "count": t["count"],
                    "change_pct": t["change_pct"],
                    "regime": regime,
                    "streak": streak,
                }
                results.append(entry)

                # Upsert
                conn.execute("""
                    INSERT OR REPLACE INTO theme_daily
                    (market_date, theme_code, theme_name, stock_count,
                     change_pct, regime, streak_days, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (today_str, t["code"], t["name"], t["count"],
                      t["change_pct"], regime, streak, now_str))

            conn.commit()
        finally:
            conn.close()

        # 4) 정렬: 등락률 순 (BULL 먼저, BEAR 마지막)
        results.sort(key=lambda r: r["change_pct"], reverse=True)
        logger.info(f"[THEME_REGIME] {len(results)} themes classified "
                    f"(BULL={sum(1 for r in results if r['regime']=='BULL')}, "
                    f"BEAR={sum(1 for r in results if r['regime']=='BEAR')})")
        return results

    def _load_latest(self) -> List[dict]:
        """DB에서 오늘 데이터 로드 (API 실패 시 fallback)."""
        today_str = date.today().strftime("%Y-%m-%d")
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM theme_daily WHERE market_date=? ORDER BY change_pct DESC",
                (today_str,),
            ).fetchall()
            return [{
                "code": r["theme_code"], "name": r["theme_name"],
                "count": r["stock_count"], "change_pct": r["change_pct"],
                "regime": r["regime"], "streak": r["streak_days"],
            } for r in rows]
        finally:
            conn.close()

    def load_history(self, theme_code: str, days: int = 30) -> List[dict]:
        """특정 테마의 최근 N일 이력."""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM theme_daily WHERE theme_code=? "
                "ORDER BY market_date DESC LIMIT ?",
                (theme_code, days),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()
