import sys
sys.path.insert(0, 'C:\\Q-TRON')

from config import QTronConfig
from core.portfolio import Portfolio

def test_6gate():
    config = QTronConfig()
    p = Portfolio(config)

    # 1. 정상 진입
    ok, msg = p.can_enter("005930", 1_000_000, "반도체")
    assert ok, f"실패: {msg}"
    print(f"✅ 정상 진입: {msg}")

    # 2. 종목당 비중 초과 (20% = 2,000,000원 초과)
    ok, msg = p.can_enter("005930", 3_000_000, "반도체")
    assert not ok
    print(f"✅ 비중 초과 차단: {msg}")

    # 3. 실제 매수 후 포지션 확인
    p.update_position("005930", "반도체", 10, 70_000, "BUY")
    assert "005930" in p.positions
    print(f"✅ 포지션 등록: {p.positions['005930'].market_value:,.0f}원")

    # 4. HARD_STOP 테스트
    p2 = Portfolio(config)
    p2.monthly_dd_limit = -0.001  # 강제 유발
    p2.update_position("005930", "반도체", 10, 70_000, "BUY")
    p2.update_prices({"005930": 60_000})
    assert p2.risk_mode() == "HARD_STOP"
    print(f"✅ HARD_STOP 정상 감지: {p2.get_monthly_dd_pct():.2%}")

    print("\n✅ 모든 테스트 통과")

if __name__ == "__main__":
    test_6gate()