"""
EntrySignal
===========
Gen3 핵심 데이터 계약: signals_YYYYMMDD.csv 로드 + 진입 후보 필터링.

signals.csv 포맷:
  date,ticker,qscore,entry,tp,sl,sector
  20260310,005930,0.82,72000,76000,69000,Semiconductor

런타임 흐름:
  1. 오늘 날짜 signals_YYYYMMDD.csv 탐색
  2. qscore 내림차순 정렬
  3. 이미 보유 중인 종목 제외
  4. max_positions 슬롯 여유분만큼 선택
"""

import csv
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from config import Gen3Config


class EntrySignal:

    def __init__(self, config: Gen3Config):
        self.signals_dir = config.abs_path(config.signals_dir)

    def load_today(self) -> List[Dict[str, Any]]:
        """오늘 날짜 signals 파일 로드. 없으면 최근 3일 fallback."""
        today = date.today()
        filepath = None
        for delta in range(4):  # 오늘, 1일전, 2일전, 3일전
            candidate = self.signals_dir / f"signals_{(today - __import__('datetime').timedelta(days=delta)).strftime('%Y%m%d')}.csv"
            if candidate.exists():
                filepath = candidate
                if delta > 0:
                    print(f"[EntrySignal] {delta}일 이전 파일 사용: {filepath.name}")
                break

        if filepath is None:
            print(f"[EntrySignal] signals_{today.strftime('%Y%m%d')}.csv 없음 → 신규 진입 없음")
            return []

        signals = []
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    rs_c = float(row["rs_composite"]) if row.get("rs_composite") else float(row["qscore"])
                    signals.append({
                        "code":         row["ticker"],
                        "qscore":       float(row["qscore"]),
                        "entry":        int(row["entry"]),
                        "tp":           int(row["tp"]),
                        "sl":           int(row["sl"]),
                        "sector":       row.get("sector", "기타"),
                        "stage":        row.get("stage", "B"),
                        "date":         row.get("date", today),
                        # v7 컬럼
                        "rs_composite": rs_c,
                        "signal_entry": int(row.get("signal_entry", 1)),
                        "is_52w_high":  int(row.get("is_52w_high", 0)),
                        "above_ma20":   int(row.get("above_ma20", 0)),
                        "rs20_rank":    float(row.get("rs20_rank") or 0),
                        "rs60_rank":    float(row.get("rs60_rank") or 0),
                        "rs120_rank":   float(row.get("rs120_rank") or 0),
                        "atr20":        float(row.get("atr20") or 0),
                    })
                except (KeyError, ValueError) as e:
                    print(f"[EntrySignal] 행 파싱 오류 ({row}): {e}")

        signals.sort(key=lambda x: x["qscore"], reverse=True)
        print(f"[EntrySignal] {filepath.name} 로드 → {len(signals)}개 신호")
        return signals

    def filter_candidates(
        self,
        signals: List[Dict[str, Any]],
        portfolio,
        max_new: int,
    ) -> List[Dict[str, Any]]:
        """
        이미 보유 중인 종목 제외 후 상위 max_new개 반환.
        """
        result = []
        for sig in signals:
            code = sig["code"]
            if portfolio.has_position(code):
                continue
            result.append(sig)
            if len(result) >= max_new:
                break
        print(f"[EntrySignal] 신규 진입 후보: {len(result)}개 (보유 종목 제외)")
        return result

    def latest_signal_file(self) -> Optional[Path]:
        """signals_dir 에서 가장 최신 signals_*.csv 반환."""
        files = sorted(self.signals_dir.glob("signals_*.csv"), reverse=True)
        return files[0] if files else None
