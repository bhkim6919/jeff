"""
BatchRunner
===========
Gen3 배치 파이프라인 스케줄러.
실행 시간: 18:00 ~ 20:30 (평일)

Flow:
  Batch Start
    ↓ Fetch Market Data
    ↓ Build Universe (Top Market Cap)
    ↓ Run Technical Indicators
    ↓ Run Supply-Demand Analysis
    ↓ Compute Q-Score
    ↓ Rank Stocks
    ↓ Apply Filters
    ↓ Generate signals_YYYYMMDD.csv
    ↓ Save to signals directory
  Batch End

실행 방법:
  python main.py --batch              # 즉시 실행
  python batch/batch_runner.py        # 직접 실행
  python batch/batch_runner.py --daemon  # APScheduler 데몬
"""

from __future__ import annotations

import argparse
import logging
import sys
import time as _time
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Dict, Any

# ── 로깅 ──────────────────────────────────────────────────────────────────────
LOG_DIR = Path(__file__).resolve().parent.parent / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_DIR / f"batch_{date.today().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("BATCH")


class BatchRunner:
    """Gen3 배치 파이프라인 실행기."""

    def __init__(self, config, provider=None, max_stocks: int = 0):
        self.config     = config
        self.provider   = provider
        self.max_stocks = max_stocks

    def run(self) -> Dict[str, Any]:
        today = date.today()
        t0    = _time.time()
        logger.info("━━━ Gen3 Batch 시작: %s ━━━", today)

        result = {
            "date":         today.strftime("%Y-%m-%d"),
            "signal_count": 0,
            "signal_file":  None,
            "errors":       [],
        }

        try:
            # Step 1: 레짐 감지 (전일 기준)
            regime = self._detect_regime()
            logger.info("[Step1] 레짐: %s", regime.value)

            # Step 2: 유니버스 구성
            universe = self._build_universe()
            logger.info("[Step2] 유니버스: %d개", len(universe))

            # Step 3: Q-Score 계산
            scored = self._run_qscore(universe, regime)
            logger.info("[Step3] 스코어링 완료: %d개", len(scored))

            # Step 4: signals.csv 생성
            signal_file = self._generate_signals(scored, regime)
            result["signal_count"] = len(scored[:50])
            result["signal_file"]  = str(signal_file)
            logger.info("[Step4] 신호 파일 저장: %s", signal_file.name)

        except Exception as e:
            # TrTimeoutError (연속 타임아웃) 는 별도 표시
            if "연속 타임아웃" in str(e):
                logger.critical("[Batch] Kiwoom TR 연속 차단 — 배치 조기 종료: %s", e)
            else:
                logger.error("[Batch] 예외 발생: %s", e, exc_info=True)
            result["errors"].append(str(e))

        elapsed = _time.time() - t0
        logger.info("━━━ Gen3 Batch 완료: %s (소요 %.0f초) ━━━", today, elapsed)
        return result

    # ── 내부 단계 ─────────────────────────────────────────────────────────────

    def _detect_regime(self):
        from strategy.regime_detector import RegimeDetector
        if self.provider is None:
            from strategy.regime_detector import MarketRegime
            logger.warning("[Step1] provider 없음 → SIDEWAYS 가정")
            return MarketRegime.SIDEWAYS
        detector = RegimeDetector(self.provider, self.config)
        return detector.detect()

    def _build_universe(self):
        from batch.universe_builder import UniverseBuilder
        if self.provider is None:
            logger.warning("[Step2] provider 없음 → 빈 유니버스")
            return []
        builder = UniverseBuilder(self.provider, self.config)
        return builder.build(max_stocks=self.max_stocks)

    def _run_qscore(self, universe, regime):
        from batch.qscore_pipeline import QScorePipeline
        pipeline = QScorePipeline(self.provider, self.config)
        return pipeline.run(universe, regime)

    def _generate_signals(self, scored, regime=None):
        from batch.signal_generator import SignalGenerator
        generator = SignalGenerator(self.config)
        return generator.generate(scored, regime=regime)


# ── 데몬 모드 ─────────────────────────────────────────────────────────────────

def run_daemon(config) -> None:
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error("APScheduler 미설치. pip install apscheduler")
        sys.exit(1)

    runner = BatchRunner(config)
    ap     = BlockingScheduler(timezone="Asia/Seoul")
    ap.add_job(
        runner.run,
        "cron",
        day_of_week="mon-fri",
        hour=18, minute=0,
        id="gen3_batch",
    )
    logger.info("[Daemon] 매 평일 18:00 Gen3 Batch 예약")
    try:
        ap.start()
    except KeyboardInterrupt:
        logger.info("[Daemon] 종료")


# ── 직접 실행 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from config import Gen3Config

    parser = argparse.ArgumentParser(description="Q-TRON Gen3 Batch Runner")
    parser.add_argument("--daemon", action="store_true", help="APScheduler 데몬 모드")
    args = parser.parse_args()

    cfg = Gen3Config.load()

    if args.daemon:
        run_daemon(cfg)
    else:
        runner = BatchRunner(cfg)
        res    = runner.run()
        print("\n[Batch 결과]")
        print(f"  신호 수  : {res['signal_count']}")
        print(f"  파일     : {res['signal_file']}")
        if res["errors"]:
            print(f"  에러     : {res['errors']}")
            sys.exit(1)
        sys.exit(0)
