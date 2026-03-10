"""
EOD Scheduler (End-Of-Day)
==========================
장 마감 후 자동 실행 루틴 (P1 구현).

파이프라인:
  장 마감 (15:30) → Kiwoom API 데이터 수집
  → Early 신호 계산 (early_signal.py)
  → 활성 섹터 저장 (JSON + SQLite)
  → 익일 주문 후보 리스트 생성
  → 알림 발송 (Telegram / 로그)

실행 방법:
  # 수동 실행
  python eod_scheduler.py

  # Windows 작업 스케줄러 / cron 등록
  python eod_scheduler.py --scheduled

  # APScheduler 내장 (프로세스 상시 실행)
  python eod_scheduler.py --daemon
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, time as dtime
from pathlib import Path
from typing import Dict, List, Optional

# ── 로깅 설정 ──────────────────────────────────────────────────────────────────
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / f"eod_{date.today().strftime('%Y%m%d')}.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("EOD")


# ── 설정 ──────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "sector_map_path":   "data/sector_map.json",
    "early_signal_dir":  "data/early_signals",
    "early_signal_db":   "data/early_signals.db",
    "order_output_path": "data/next_day_orders.json",
    "eod_trigger_time":  "15:35",     # 장 마감 후 5분 여유
    "gap_up_limit":      1.05,
    "sector_cap":        4,
    "max_positions":     20,
}


class EODScheduler:
    """
    장 마감 후 Early Entry 신호 감지 + 익일 주문 준비 루틴.
    """

    def __init__(self, config: dict = None, provider=None):
        self.config   = {**DEFAULT_CONFIG, **(config or {})}
        self.provider = provider

    def run(self) -> dict:
        """
        EOD 전체 루틴 실행.

        Returns:
            결과 요약 dict
        """
        today = date.today()
        logger.info(f"━━━ EOD 루틴 시작: {today} ━━━")

        results = {
            "date":            today.strftime("%Y-%m-%d"),
            "early_signals":   None,
            "next_day_orders": None,
            "errors":          [],
        }

        # Step 1: Early Entry 신호 감지
        try:
            early_result = self._run_early_signal()
            results["early_signals"] = early_result
            logger.info(f"[Step1] Early 신호 완료: {early_result.get('active_sectors', [])} 활성")
        except Exception as e:
            logger.error(f"[Step1] Early 신호 오류: {e}", exc_info=True)
            results["errors"].append(f"early_signal: {e}")

        # Step 2: 익일 주문 후보 리스트 생성
        try:
            orders = self._build_next_day_orders(results.get("early_signals"))
            results["next_day_orders"] = orders
            logger.info(f"[Step2] 익일 주문 후보: {len(orders)}건")
        except Exception as e:
            logger.error(f"[Step2] 주문 준비 오류: {e}", exc_info=True)
            results["errors"].append(f"next_day_orders: {e}")

        # Step 3: 알림 발송
        try:
            self._send_notification(results)
        except Exception as e:
            logger.warning(f"[Step3] 알림 발송 실패 (비치명): {e}")

        logger.info(f"━━━ EOD 루틴 완료: {today} ━━━")
        return results

    # ── Step 1: Early Entry 신호 감지 ────────────────────────────────────────

    def _run_early_signal(self) -> dict:
        """Early Entry 신호 감지 실행."""
        sector_map_path = self.config["sector_map_path"]

        if not os.path.exists(sector_map_path):
            raise FileNotFoundError(f"sector_map.json 없음: {sector_map_path}")

        with open(sector_map_path, "r", encoding="utf-8") as f:
            sector_map = json.load(f)

        from stage1_market.early_signal import EarlySignalDetector
        detector = EarlySignalDetector(
            provider=self.provider,
            sector_map=sector_map,
            output_dir=self.config["early_signal_dir"],
            db_path=self.config["early_signal_db"],
        )

        result = detector.detect()
        return result.to_dict()

    # ── Step 2: 익일 주문 후보 생성 ──────────────────────────────────────────

    def _build_next_day_orders(self, early_result: Optional[dict]) -> List[dict]:
        """
        Early 신호 기반 익일 시가 매수 후보 리스트 생성.

        조건:
          - Early 활성 섹터 종목 우선
          - signal_entry=1 (52주 신고가 + RS 조건) 확인
          - BULL 레짐 확인
          - RS 순위 상위 순 정렬
          - 갭업 방지는 익일 시가 확인 후 최종 판단 (런타임에서 처리)
        """
        if early_result is None:
            return []

        active_sectors = early_result.get("active_sectors", [])

        if not active_sectors:
            logger.info("[Step2] 활성 섹터 없음 → 익일 주문 후보 없음")
            # Gen2 기본 신호 기반 후보는 pipeline.py에서 처리 (fallback)
            order_list = []
        else:
            # 활성 섹터 종목들로 익일 주문 후보 구성
            # (실제 signal_entry=1 + RS 필터는 파이프라인 런타임에서 적용)
            order_list = []
            sector_signals = early_result.get("sector_signals", [])

            for sig in sector_signals:
                if not sig.get("signal", False):
                    continue
                sector = sig.get("sector", "")
                codes  = sig.get("active_codes", [])

                for code in codes:
                    order_list.append({
                        "code":        code,
                        "sector":      sector,
                        "signal_type": "EARLY_ENTRY",
                        "entry_time":  "익일 시가",
                        "gap_check":   True,   # 익일 시가 갭업 체크 필요
                        "note":        f"Early 신호: {sector} (조건 {sig.get('cond_count', 0)}/3)",
                    })

        # JSON 저장
        output_path = self.config["order_output_path"]
        os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

        output = {
            "date":            date.today().strftime("%Y-%m-%d"),
            "active_sectors":  active_sectors,
            "candidates":      order_list,
            "generated_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "note":            "익일 시가 진입 후보. 실제 진입은 파이프라인 런타임에서 갭업 체크 후 결정.",
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)

        logger.info(f"[Step2] 익일 주문 후보 저장: {output_path}")
        return order_list

    # ── Step 3: 알림 발송 ────────────────────────────────────────────────────

    def _send_notification(self, results: dict) -> None:
        """
        결과 알림 발송.
        현재: 로그 출력 (Telegram 연동은 P2에서 구현).
        """
        date_str = results.get("date", "")
        early    = results.get("early_signals") or {}
        orders   = results.get("next_day_orders") or []
        errors   = results.get("errors") or []

        active_sectors = early.get("active_sectors", [])

        msg_lines = [
            f"📊 Q-TRON EOD 리포트 [{date_str}]",
            f"",
            f"🎯 Early Entry 활성 섹터: {len(active_sectors)}개",
        ]

        if active_sectors:
            for s in active_sectors:
                msg_lines.append(f"  ▪ {s}")

        msg_lines += [
            f"",
            f"📋 익일 주문 후보: {len(orders)}건",
        ]

        if orders:
            for o in orders[:5]:  # 최대 5건만 표시
                msg_lines.append(f"  ▪ {o['code']} ({o['sector']})")
            if len(orders) > 5:
                msg_lines.append(f"  ... 외 {len(orders)-5}건")

        if errors:
            msg_lines += [f"", f"⚠️ 오류: {len(errors)}건"]
            for e in errors:
                msg_lines.append(f"  ✗ {e}")

        msg = "\n".join(msg_lines)
        logger.info("\n" + msg)

        # TODO (P2): Telegram 발송
        # self._send_telegram(msg)

    def _send_telegram(self, message: str) -> None:
        """Telegram Bot API 발송 (P2 구현 예정)."""
        # import requests
        # token   = os.environ.get("TELEGRAM_BOT_TOKEN")
        # chat_id = os.environ.get("TELEGRAM_CHAT_ID")
        # if token and chat_id:
        #     requests.post(
        #         f"https://api.telegram.org/bot{token}/sendMessage",
        #         json={"chat_id": chat_id, "text": message}
        #     )
        pass


# ── 스케줄러 모드 ─────────────────────────────────────────────────────────────

def run_daemon(config: dict) -> None:
    """APScheduler 데몬 모드 (상시 실행)."""
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        logger.error("APScheduler 미설치. pip install apscheduler")
        sys.exit(1)

    trigger_time = config.get("eod_trigger_time", "15:35")
    hour, minute = map(int, trigger_time.split(":"))

    scheduler = EODScheduler(config)
    ap = BlockingScheduler(timezone="Asia/Seoul")

    ap.add_job(
        scheduler.run,
        "cron",
        day_of_week="mon-fri",
        hour=hour,
        minute=minute,
        id="eod_routine",
    )

    logger.info(f"[Daemon] 매일 {trigger_time} (평일) EOD 루틴 예약 완료")
    logger.info("[Daemon] Ctrl+C로 종료")

    try:
        ap.start()
    except KeyboardInterrupt:
        logger.info("[Daemon] 종료")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Q-TRON EOD 자동 루틴")
    parser.add_argument("--daemon",    action="store_true", help="APScheduler 데몬 모드")
    parser.add_argument("--scheduled", action="store_true", help="단발 실행 (Windows 작업 스케줄러용)")
    parser.add_argument("--config",    default=None,        help="config JSON 파일 경로")
    args = parser.parse_args()

    # config 로드
    cfg = DEFAULT_CONFIG.copy()
    if args.config and os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as f:
            cfg.update(json.load(f))

    if args.daemon:
        run_daemon(cfg)
    else:
        # 즉시 실행 (수동 or 작업 스케줄러)
        scheduler = EODScheduler(cfg)
        result    = scheduler.run()

        if result["errors"]:
            sys.exit(1)  # 오류 시 비정상 종료 (스케줄러 재시도 트리거)
        sys.exit(0)
