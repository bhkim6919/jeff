"""
EarlyEntryLayer
===============
Gen2 v2.3 파이프라인에 Early Entry 신호를 통합하는 레이어 (P1 구현).

개발문서 5.2.2 통합 포인트:
  - 기존: signal_entry=1 종목 중 RS 상위 정렬
  - 변경: Early 활성 섹터 종목 우선 + signal_entry=1 + RS 정렬
  - Gen2 기본 진입은 유지 (Early 없을 때 fallback)

갭업 방지 규칙:
  - 시가 > 전일 종가 × 1.05 이면 진입 취소

섹터 집중 제한:
  - SECTOR_CAP=4: 동일 섹터 최대 4종목 (Early / Gen2 모두 동일 적용)
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

from stage1_market.early_signal import EarlySignalDetector, EarlySignalResult


# ── 확정 파라미터 ─────────────────────────────────────────────────────────────
SECTOR_CAP      = 4      # 동일 섹터 최대 보유 종목 수
GAP_UP_LIMIT    = 1.05   # 갭업 방지: 시가 > 전일 종가 × 1.05 → 진입 취소


class EarlyEntryLayer:
    """
    Early Entry 신호를 Gen2 파이프라인에 통합하는 레이어.

    사용법 (pipeline.py Stage2 ~ Stage3 사이):
        early_layer = EarlyEntryLayer(provider, sector_map, config)

        # Stage2 이후: 후보 종목에 Early 우선순위 적용
        prioritized = early_layer.prioritize_candidates(
            candidates=candidates,          # Stage2 통과 종목 리스트
            portfolio=self.portfolio,       # 현재 포트폴리오
        )

        # Stage3 이후: 갭업 / 섹터캡 필터
        filtered = early_layer.apply_entry_filters(
            scored=scored,
            portfolio=self.portfolio,
        )
    """

    def __init__(
        self,
        provider,
        sector_map: Dict[str, str],   # {ticker: sector_name}
        output_dir: str = "data/early_signals",
        db_path:    str = "data/early_signals.db",
        sector_cap: int = SECTOR_CAP,
        gap_up_limit: float = GAP_UP_LIMIT,
    ):
        self.provider     = provider
        self.sector_map   = sector_map
        self.sector_cap   = sector_cap
        self.gap_up_limit = gap_up_limit
        self.output_dir   = Path(output_dir)

        self.detector = EarlySignalDetector(
            provider=provider,
            sector_map=sector_map,
            output_dir=output_dir,
            db_path=db_path,
        )

        # 오늘의 활성 섹터 캐시 (탐지는 1회만)
        self._active_sectors_cache: Optional[List[str]] = None
        self._cache_date: Optional[date] = None

    # ── Public API ────────────────────────────────────────────────────────────

    def get_active_sectors(self, use_cache: bool = True) -> List[str]:
        """
        오늘 Early Entry 활성 섹터 목록 반환.
        - 당일 첫 호출 시: 저장된 JSON 로드 (장 마감 후 detect()가 이미 실행된 것으로 가정)
        - JSON 없으면: 실시간 detect() 실행
        - use_cache=True: 동일 세션 내 재호출 시 캐시 반환
        """
        today = date.today()

        if use_cache and self._cache_date == today and self._active_sectors_cache is not None:
            return self._active_sectors_cache

        # 저장된 JSON 우선 로드 (장 마감 후 early_signal.py가 실행된 결과)
        active = self._load_from_json(today)

        if active is None:
            print("[EarlyEntry] JSON 없음 → 실시간 감지 실행")
            result = self.detector.detect(today)
            active = result.active_sectors

        self._active_sectors_cache = active
        self._cache_date = today

        print(f"[EarlyEntry] 활성 섹터 {len(active)}개: {active}")
        return active

    def prioritize_candidates(
        self,
        candidates: List[str],
        portfolio=None,
    ) -> List[str]:
        """
        Stage2 후보 종목에 Early Entry 우선순위 적용.

        Early 활성 섹터 종목을 앞으로 배치하고,
        나머지는 기존 Gen2 순서 유지 (fallback).

        Args:
            candidates: Stage2 통과 종목 코드 리스트
            portfolio:  현재 포트폴리오 (보유 종목 제외용)

        Returns:
            우선순위 적용된 종목 리스트
        """
        active_sectors = self.get_active_sectors()

        if not active_sectors:
            print("[EarlyEntry] 활성 섹터 없음 → Gen2 기본 순서 유지 (fallback)")
            return candidates

        early_codes  = []
        normal_codes = []

        for code in candidates:
            sector = self.sector_map.get(code, "")
            if sector in active_sectors:
                early_codes.append(code)
            else:
                normal_codes.append(code)

        print(f"[EarlyEntry] Early 우선: {len(early_codes)}개 / Gen2 fallback: {len(normal_codes)}개")

        return early_codes + normal_codes

    def apply_entry_filters(
        self,
        scored: List[dict],
        portfolio=None,
    ) -> List[dict]:
        """
        Stage3 Q-Score 이후 진입 전 최종 필터 적용.

        ① 갭업 방지: 시가 > 전일 종가 × 1.05 → 스킵
        ② 섹터 집중 제한: 동일 섹터 최대 SECTOR_CAP 종목

        Args:
            scored:    Q-Score 정렬된 종목 dict 리스트
                       각 dict는 최소 {"code": str, ...} 포함
            portfolio: 현재 포트폴리오 (섹터 집중 체크용)

        Returns:
            필터 통과 종목 리스트
        """
        # 현재 보유 섹터 카운트 (포트폴리오에서 가져옴)
        sector_counts: Dict[str, int] = {}
        if portfolio is not None and hasattr(portfolio, "positions"):
            for pos_code, pos_info in portfolio.positions.items():
                sector = self.sector_map.get(pos_code, "")
                if sector:
                    sector_counts[sector] = sector_counts.get(sector, 0) + 1

        filtered = []
        skip_log = []

        for item in scored:
            code   = item.get("code", "")
            sector = self.sector_map.get(code, "")

            # ① 갭업 방지
            if self._is_gap_up(code):
                skip_log.append(f"{code} 갭업 진입 취소 (>5%)")
                continue

            # ② 섹터 집중 제한
            current = sector_counts.get(sector, 0)
            if sector and current >= self.sector_cap:
                skip_log.append(f"{code} 섹터캡 초과 ({sector}: {current}/{self.sector_cap})")
                continue

            filtered.append(item)

            # 섹터 카운트 증가 (이번 배치에서 추가될 종목 포함)
            if sector:
                sector_counts[sector] = current + 1

        if skip_log:
            print(f"[EarlyEntry] 진입 필터 탈락 {len(skip_log)}개:")
            for log in skip_log:
                print(f"  ✗ {log}")

        print(f"[EarlyEntry] 최종 진입 후보: {len(filtered)}개")
        return filtered

    def annotate_early_flag(self, scored: List[dict]) -> List[dict]:
        """
        scored 리스트에 is_early_entry 플래그 추가 (로깅/리포트용).
        파이프라인 변경 없이 메타데이터만 추가.
        """
        active_sectors = self.get_active_sectors()
        for item in scored:
            code   = item.get("code", "")
            sector = self.sector_map.get(code, "")
            item["is_early_entry"] = sector in active_sectors
        return scored

    # ── 내부 유틸 ─────────────────────────────────────────────────────────────

    def _is_gap_up(self, code: str) -> bool:
        """
        갭업 판단: 오늘 시가 > 전일 종가 × 1.05.
        데이터 없으면 False (진입 허용).
        """
        try:
            df = self.provider.get_stock_ohlcv(code, days=3)
            if df is None or len(df) < 2:
                return False

            prev_close = float(df["close"].iloc[-2])
            today_open = float(df["open"].iloc[-1])

            gap_ratio = today_open / prev_close
            if gap_ratio > self.gap_up_limit:
                print(f"  [갭업] {code}: 시가 {today_open:,.0f} / 전일종가 {prev_close:,.0f} = {gap_ratio:.3f}")
                return True
        except Exception:
            pass
        return False

    def _load_from_json(self, target_date: date) -> Optional[List[str]]:
        """저장된 early_signal_YYYY-MM-DD.json 로드."""
        date_str = target_date.strftime("%Y-%m-%d")
        path = self.output_dir / f"early_signal_{date_str}.json"

        if not path.exists():
            return None

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data.get("active_sectors", [])


# ── sector_map 로드 헬퍼 ──────────────────────────────────────────────────────

def load_sector_map(path: str = "data/sector_map.json") -> Dict[str, str]:
    """sector_map.json → {ticker: sector} dict 반환."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
