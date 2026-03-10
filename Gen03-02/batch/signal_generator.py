"""
SignalGenerator
===============
Q-Score 계산 결과를 signals_YYYYMMDD.csv 로 저장한다.

출력 포맷 (문서 기준):
  date,ticker,qscore,entry,tp,sl,sector
  20260310,005930,0.82,72000,76000,69000,Semiconductor

런타임(runtime_engine.py)은 이 파일만 읽으면 당일 진입 후보를 파악한다.
"""

import csv
from datetime import date, datetime
from pathlib import Path
from typing import List, Dict, Any

from config import Gen3Config

# SL이 진입가 대비 이 비율 미만이면 슬리피지에 즉시 터지는 ETF/채권형 제외
MIN_SL_DISTANCE_PCT = 0.01   # 1%


class SignalGenerator:

    def __init__(self, config: Gen3Config):
        self.signals_dir = config.abs_path(config.signals_dir)
        self.signals_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        scored: List[Dict[str, Any]],
        target_date: date = None,
        top_n: int = 50,
        regime=None,
    ) -> Path:
        """
        scored   : QScorePipeline.run() 반환값
        top_n    : 상위 N개만 signals.csv에 저장
        regime   : 현재 MarketRegime (stage 분류에 사용)
        반환     : 저장된 파일 경로
        """
        if target_date is None:
            target_date = date.today()

        today_str  = target_date.strftime("%Y%m%d")
        filepath   = self.signals_dir / f"signals_{today_str}.csv"
        # 필터 1: 종목코드가 순수 6자리 숫자가 아니면 제외 (ETF/채권형 ticker 방어)
        filtered_scored = []
        ticker_filtered = 0
        for item in scored:
            code = str(item.get("code", ""))
            if not (code.isdigit() and len(code) == 6):
                ticker_filtered += 1
                continue
            filtered_scored.append(item)
        if ticker_filtered:
            print(f"[SignalGenerator] ticker 형식 불일치 제외: {ticker_filtered}개")

        # 필터 2: SL 최소 거리 필터: 진입가 대비 1% 미만이면 제외 (ETF/채권형 방어)
        sl_filtered_list = []
        sl_filtered = 0
        for item in filtered_scored:
            entry = item.get("entry", 0)
            sl    = item.get("sl", 0)
            if entry > 0 and sl > 0 and (entry - sl) / entry < MIN_SL_DISTANCE_PCT:
                sl_filtered += 1
                continue
            sl_filtered_list.append(item)
        filtered_scored = sl_filtered_list
        if sl_filtered:
            print(f"[SignalGenerator] SL 폭 1% 미만 제외: {sl_filtered}개")
        top_scored = filtered_scored[:top_n]

        # Stage 분류: BULL 레짐일 때 상위 20%를 Stage A로 태깅
        try:
            from strategy.regime_detector import MarketRegime
            is_bull = (regime == MarketRegime.BULL)
        except Exception:
            is_bull = False
        stage_a_cutoff = max(1, len(top_scored) // 5) if is_bull else 0

        fieldnames = ["date", "ticker", "qscore", "entry", "tp", "sl", "sector", "stage"]
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for i, item in enumerate(top_scored):
                stage = "A" if i < stage_a_cutoff else "B"
                writer.writerow({
                    "date":   today_str,
                    "ticker": item["code"],
                    "qscore": round(item["qscore"], 4),
                    "entry":  item.get("entry", 0),
                    "tp":     item.get("tp", 0),
                    "sl":     item.get("sl", 0),
                    "sector": item.get("sector", "기타"),
                    "stage":  stage,
                })

        cnt_a = min(stage_a_cutoff, len(top_scored))
        cnt_b = len(top_scored) - cnt_a
        print(f"[SignalGenerator] {filepath.name} 저장 완료 "
              f"({len(top_scored)}개 신호 / Stage A:{cnt_a} B:{cnt_b})")
        return filepath

    def load(self, filepath: Path) -> List[Dict[str, Any]]:
        """저장된 signals.csv 재로드 (확인 용도)."""
        signals = []
        with open(filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                signals.append(row)
        return signals
