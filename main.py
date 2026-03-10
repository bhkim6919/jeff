"""
Q-TRON main.py

실행 모드:
  python main.py              → LIVE 모드 (Kiwoom 로그인 필요, 장중만 실행)
  python main.py --pykrx      → PYKRX 모드 (장외 시간 테스트, 실데이터)
  python main.py --mock       → MOCK 모드 (API 없이 구조 확인용)

PYKRX 모드 특징:
  - Kiwoom 불필요, pykrx 실데이터 사용
  - 장외 시간 체크 없음 → 언제든 실행 가능
  - Early Entry 신호 포함 전체 파이프라인 검증 가능
  - paper_trading=True 강제 (실주문 없음)
"""

import sys
import os
import argparse
import traceback
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Dict, Optional
import time as time_module

from config import QTronConfig
from data.mock_provider import MockProvider
from core.pipeline import QTronPipeline

try:
    from data.pykrx_provider import PykrxProvider
except ImportError:
    try:
        from pykrx_provider import PykrxProvider  # data/ 폴더 직접 실행 대비
    except ImportError:
        PykrxProvider = None

try:
    from data.kiwoom_provider import KiwoomProvider
    from PyQt5.QtWidgets import QApplication
    from kiwoom_wrapper import create_loggedin_kiwoom
except ImportError:
    KiwoomProvider = None
    QApplication   = None
    create_loggedin_kiwoom = None


# ── 로그 설정 ─────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR  = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

ERROR_FLAG_PATH = LOG_DIR / "LAST_ERROR.flag"


# ── 에러 리포트 ───────────────────────────────────────────────────────────────

def save_error_report(mode: str, exc: Exception,
                      pipeline_result: Optional[Dict[str, Any]] = None) -> str:
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"error_{mode.lower()}_{ts}.html"
    path     = LOG_DIR / filename

    title    = f"Q-TRON ERROR REPORT ({mode}) - {ts}"
    exc_type = type(exc).__name__
    exc_msg  = str(exc)
    tb_text  = traceback.format_exc()

    raw_code   = getattr(exc, "errno", None) or getattr(exc, "code", None)
    error_code = str(raw_code) if raw_code is not None else exc_type

    portfolio_html = ""
    if pipeline_result and isinstance(pipeline_result, dict):
        status = pipeline_result.get("status", "UNKNOWN")
        msg    = pipeline_result.get("message", "")
        port   = pipeline_result.get("portfolio", {})
        portfolio_html = f"""
<section>
  <h3>Pipeline Result</h3>
  <pre>
status: {status}
message: {msg}
portfolio: {port}
  </pre>
</section>
"""

    html = f"""<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8"/>
<title>{title}</title>
<style>
body{{font-family:'Malgun Gothic',sans-serif;background:#f5f5f5;color:#111;padding:16px;}}
h1{{color:#b91c1c;}}
pre{{background:#111;color:#f5f5f5;padding:12px;white-space:pre-wrap;}}
section{{background:#fff;padding:12px 16px;margin-bottom:12px;border-radius:6px;box-shadow:0 1px 4px rgba(0,0,0,0.06);}}
</style>
</head>
<body>
<section>
  <h1>Q-TRON 에러 리포트</h1>
  <p><strong>발생시각:</strong> {ts}</p>
  <p><strong>실행모드:</strong> {mode}</p>
  <p><strong>에러코드:</strong> {error_code}</p>
  <p><strong>예외타입:</strong> {exc_type}</p>
  <p><strong>메시지:</strong> {exc_msg}</p>
</section>
{portfolio_html}
<section>
  <h3>Traceback</h3>
  <pre>{tb_text}</pre>
</section>
</body>
</html>
"""
    path.write_text(html, encoding="utf-8")
    print(f"[Reporter] ERROR 리포트 저장 → {path}")
    return str(path)


def set_last_error_flag(report_path: str) -> None:
    try:
        ERROR_FLAG_PATH.write_text(report_path, encoding="utf-8")
        print(f"[Reporter] LAST_ERROR.flag 업데이트 → {ERROR_FLAG_PATH}")
    except Exception as e:
        print(f"[WARN] LAST_ERROR.flag 기록 중 에러: {e}")


def clear_last_error_flag() -> None:
    try:
        if ERROR_FLAG_PATH.exists():
            ERROR_FLAG_PATH.unlink()
            print(f"[Reporter] LAST_ERROR.flag 삭제 → {ERROR_FLAG_PATH}")
    except Exception as e:
        print(f"[WARN] LAST_ERROR.flag 삭제 중 에러: {e}")


def print_last_error_warning_if_any() -> None:
    if not ERROR_FLAG_PATH.exists():
        return
    try:
        last = ERROR_FLAG_PATH.read_text(encoding="utf-8").strip()
    except Exception:
        last = "(읽기 실패)"
    print("=" * 50)
    print("[경고] 이전 Q-TRON 실행에서 에러가 발생했습니다.")
    print(f"       마지막 에러 리포트: {last}")
    print("       내용 확인 후 조치하는 것이 좋습니다.")
    print("=" * 50)


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _load_config() -> QTronConfig:
    if hasattr(QTronConfig, "load"):
        return QTronConfig.load()
    if hasattr(QTronConfig, "from_env"):
        return QTronConfig.from_env()
    return QTronConfig()


def _print_header(mode: str) -> None:
    print("=" * 50)
    print(f"Q-TRON 시작 [{mode} 모드]")
    print("=" * 50)


def _print_result(result: Dict[str, Any]) -> None:
    print("\n[Pipeline 결과]")
    print("  status :", result.get("status"))
    print("  message:", result.get("message"))
    print("  portfolio 요약:")
    for k, v in result.get("portfolio", {}).items():
        print(f"    - {k}: {v}")


def wait_until(target_hour: int = 9, target_minute: int = 0) -> None:
    """
    target_hour:target_minute 까지 루프 대기.
    - 1분마다 남은 시간 출력
    - KeyboardInterrupt → 대기 취소하지 않고 계속 대기 (Ctrl+C 오조작 방지)
    - 이미 지난 시각이면 즉시 리턴
    """
    now    = datetime.now()
    target = datetime.combine(now.date(), dtime(target_hour, target_minute))
    if now >= target:
        print(f"[Scheduler] 현재 시각 {now.strftime('%H:%M')} → 즉시 실행")
        return

    print("=" * 50)
    print(f"[Scheduler] {target_hour:02d}:{target_minute:02d} 장 시작까지 대기")
    print(f"[Scheduler] Ctrl+C 를 눌러도 대기가 취소되지 않습니다.")
    print("=" * 50)

    while True:
        now = datetime.now()
        if now >= target:
            print(f"[Scheduler] {target_hour:02d}:{target_minute:02d} 도달 → 파이프라인 실행")
            return
        remaining = (target - now).total_seconds()
        print(f"[Scheduler] 대기 중... {now.strftime('%H:%M:%S')} | 남은 시간: {remaining/60:.1f}분")
        try:
            # 60초 단위로 체크, 마지막 1분은 10초 단위
            sleep_sec = 60 if remaining > 60 else 10
            time_module.sleep(sleep_sec)
        except KeyboardInterrupt:
            print("[Scheduler] Ctrl+C 감지 — 대기를 유지합니다. (강제 종료는 창을 닫으세요)")
            time_module.sleep(3)  # 연타 방지


def _run_pipeline(provider, mode_label: str,
                  skip_market_hours: bool = False) -> None:
    """공통 파이프라인 실행 (provider만 다름)."""
    config = _load_config()
    config.paper_trading = True  # 항상 페이퍼 트레이딩

    pipeline = QTronPipeline(config, provider, skip_market_hours=skip_market_hours)
    pipeline_result: Optional[Dict[str, Any]] = None

    try:
        pipeline_result = pipeline.run()
        _print_result(pipeline_result)

        print("\n[장 종료 처리]")
        pipeline.end_of_day(open_browser=False)

        if pipeline_result.get("status") == "ERROR":
            exc = RuntimeError(pipeline_result.get("message", "Pipeline status == ERROR"))
            report_path = save_error_report(mode_label, exc, pipeline_result)
            set_last_error_flag(report_path)
        else:
            clear_last_error_flag()

    except Exception as e:
        print(f"\n[CRITICAL] {mode_label} 실행 중 예외:")
        print(f"  {type(e).__name__}: {e}")
        report_path = save_error_report(mode_label, e, pipeline_result)
        set_last_error_flag(report_path)


# ── MOCK 모드 ─────────────────────────────────────────────────────────────────

def run_mock() -> None:
    """MOCK 모드: MockProvider 사용 (API 없이 구조 확인용)."""
    print_last_error_warning_if_any()
    _print_header("MOCK")
    print("[MOCK] 랜덤 가짜 데이터 사용 — Early Entry 신호 의미 없음")
    _run_pipeline(MockProvider(), "MOCK", skip_market_hours=True)


# ── PYKRX 모드 ───────────────────────────────────────────────────────────────

def run_pykrx() -> None:
    """
    PYKRX 모드: 실데이터 + 장외 시간 체크 없음.
    Early Entry 포함 전체 파이프라인 검증용.
    """
    if PykrxProvider is None:
        print("[ERROR] data/pykrx_provider.py 를 찾을 수 없습니다.")
        sys.exit(1)

    print_last_error_warning_if_any()
    _print_header("PYKRX (실데이터 · 장외 테스트)")
    print("[PYKRX] Kiwoom 불필요 / 장외 시간 무관 / paper_trading=True")
    print("[PYKRX] KRX 데이터 수집 중 (첫 실행 시 약 20~30초)...")

    _run_pipeline(PykrxProvider(), "PYKRX", skip_market_hours=True)


# ── LIVE 모드 ─────────────────────────────────────────────────────────────────

def run_live() -> None:
    """LIVE 모드: Kiwoom OpenAPI+ / 장중만 실행."""
    if QApplication is None or KiwoomProvider is None:
        print("[ERROR] PyQt5 또는 Kiwoom 모듈을 찾을 수 없습니다.")
        sys.exit(1)

    if create_loggedin_kiwoom is None:
        print("[ERROR] kiwoom_wrapper.create_loggedin_kiwoom 을 찾을 수 없습니다.")
        sys.exit(1)

    print_last_error_warning_if_any()
    _print_header("LIVE (Kiwoom / PAPER)")

    # 15:30 이후면 자동 종료
    if datetime.now().time() >= dtime(15, 30):
        print("[INFO] 장 종료 이후 실행 → 자동 종료")
        print("       장외 테스트는 --pykrx 옵션을 사용하세요.")
        sys.exit(0)

    # ── 로그인은 즉시 수행 (07~08시 출근 전 실행 대응) ───────────────────────
    app    = QApplication.instance() or QApplication(sys.argv)
    config = _load_config()

    print("[Kiwoom] 로그인 시작 (장 시작 전 미리 로그인)...")
    kiwoom   = create_loggedin_kiwoom()
    provider = KiwoomProvider(kiwoom)
    print("[Kiwoom] 로그인 완료. 장 시작(09:00)까지 대기합니다.")

    # ── 09:00까지 루프 대기 (Ctrl+C 로 취소 불가) ────────────────────────────
    wait_until(9, 0)

    pipeline = QTronPipeline(config, provider)
    pipeline_result: Optional[Dict[str, Any]] = None

    try:
        pipeline_result = pipeline.run()
        _print_result(pipeline_result)

        print("\n[장 종료 처리]")
        pipeline.end_of_day(open_browser=False)

        if pipeline_result.get("status") == "ERROR":
            exc = RuntimeError(pipeline_result.get("message", "Pipeline status == ERROR"))
            report_path = save_error_report("LIVE", exc, pipeline_result)
            set_last_error_flag(report_path)
        else:
            clear_last_error_flag()

    except Exception as e:
        print(f"\n[CRITICAL] LIVE 실행 중 예외:")
        print(f"  {type(e).__name__}: {e}")
        print("  → HTS/네트워크/키움 서버 상태를 수동으로 점검하세요.")
        report_path = save_error_report("LIVE", e, pipeline_result)
        set_last_error_flag(report_path)

    sys.exit(app.exec_())


# ── 엔트리 포인트 ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Q-TRON Gen2 트레이딩 시스템",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
실행 예시:
  python main.py              LIVE 모드 (Kiwoom, 장중)
  python main.py --pykrx      PYKRX 모드 (실데이터, 장외 테스트)
  python main.py --mock       MOCK 모드 (랜덤 데이터, 구조 확인)
        """
    )
    parser.add_argument("--pykrx", action="store_true", help="pykrx 실데이터 모드 (장외 테스트)")
    parser.add_argument("--mock",  action="store_true", help="MockProvider 모드 (구조 확인용)")
    args = parser.parse_args()

    if args.mock:
        run_mock()
    elif args.pykrx:
        run_pykrx()
    else:
        run_live()