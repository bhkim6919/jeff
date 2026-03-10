"""
ForwardTest
===========
Mock / PykrxProvider를 사용해 전체 파이프라인을 장외 시간에 검증한다.

사용법:
  python tests/forward_test.py              # MockProvider
  python tests/forward_test.py --pykrx     # PykrxProvider (실데이터)
  python tests/forward_test.py --batch     # Batch 파이프라인만 테스트
"""

import sys
import argparse
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from config import Gen3Config


def test_runtime_mock() -> None:
    """MockProvider로 전체 런타임 파이프라인 검증."""
    print("=" * 60)
    print("  ForwardTest: MOCK 런타임 파이프라인")
    print("=" * 60)

    from data.mock_provider import MockProvider
    from runtime.runtime_engine import RuntimeEngine

    config = Gen3Config.load()
    config.paper_trading = True
    provider = MockProvider()

    engine = RuntimeEngine(config, provider, skip_market_hours=True)
    result = engine.run()

    print("\n[ForwardTest 결과]")
    print(f"  status : {result.get('status')}")
    print(f"  message: {result.get('message')}")
    for k, v in result.get("portfolio", {}).items():
        print(f"    {k}: {v}")

    engine.end_of_day()
    print("\n[ForwardTest] MOCK 런타임 완료")


def test_runtime_pykrx() -> None:
    """PykrxProvider로 실데이터 런타임 파이프라인 검증."""
    print("=" * 60)
    print("  ForwardTest: PYKRX 런타임 파이프라인")
    print("=" * 60)

    from data.pykrx_provider import PykrxProvider
    from runtime.runtime_engine import RuntimeEngine

    config = Gen3Config.load()
    config.paper_trading = True
    provider = PykrxProvider()

    print("[ForwardTest] KRX 데이터 수집 중...")
    engine = RuntimeEngine(config, provider, skip_market_hours=True)
    result = engine.run()

    print("\n[ForwardTest 결과]")
    print(f"  status : {result.get('status')}")
    print(f"  message: {result.get('message')}")
    for k, v in result.get("portfolio", {}).items():
        print(f"    {k}: {v}")

    engine.end_of_day()
    print("\n[ForwardTest] PYKRX 런타임 완료")


def test_batch_mock() -> None:
    """MockProvider로 배치 파이프라인 검증."""
    print("=" * 60)
    print("  ForwardTest: MOCK 배치 파이프라인")
    print("=" * 60)

    from data.mock_provider import MockProvider
    from batch.batch_runner import BatchRunner

    config   = Gen3Config.load()
    provider = MockProvider()
    runner   = BatchRunner(config, provider)
    result   = runner.run()

    print("\n[Batch 결과]")
    print(f"  신호 수: {result.get('signal_count', 0)}")
    print(f"  파일   : {result.get('signal_file', 'N/A')}")
    if result.get("errors"):
        print(f"  에러   : {result['errors']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Q-TRON Gen3 Forward Test")
    parser.add_argument("--pykrx", action="store_true", help="PykrxProvider 모드")
    parser.add_argument("--batch", action="store_true", help="Batch 파이프라인 테스트")
    args = parser.parse_args()

    if args.pykrx:
        test_runtime_pykrx()
    elif args.batch:
        test_batch_mock()
    else:
        test_runtime_mock()
