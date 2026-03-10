"""
ReportGenerator
===============
signals.csv 분석 및 백테스트 결과 리포트 생성 헬퍼.
"""

import csv
from pathlib import Path
from typing import List, Dict, Any

from config import Gen3Config


class ReportGenerator:

    def __init__(self, config: Gen3Config):
        self.config   = config
        self.log_dir  = config.abs_path("data/logs")
        self.sig_dir  = config.abs_path(config.signals_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def signals_summary(self) -> List[Dict[str, Any]]:
        """
        signals/ 디렉토리의 모든 signals_*.csv 파일을 읽어
        날짜별 신호 수 요약 리스트 반환.
        """
        summary = []
        for f in sorted(self.sig_dir.glob("signals_*.csv")):
            date_str = f.stem.replace("signals_", "")
            with open(f, newline="", encoding="utf-8") as fp:
                count = sum(1 for _ in csv.DictReader(fp))
            summary.append({"date": date_str, "signal_count": count, "file": f.name})
        return summary

    def print_signals_summary(self) -> None:
        rows = self.signals_summary()
        if not rows:
            print("[ReportGenerator] signals/ 디렉토리가 비어 있습니다.")
            return
        print(f"\n{'날짜':12s}  {'신호수':>6s}  {'파일'}")
        print("-" * 40)
        for r in rows:
            print(f"{r['date']:12s}  {r['signal_count']:>6d}  {r['file']}")
        print(f"\n총 {len(rows)}일 / {sum(r['signal_count'] for r in rows)}개 신호\n")
