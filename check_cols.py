from pykrx import stock as krx
import time

# 거래대금 상위 종목 확인 (유니버스 대체용)
print("=== get_market_trading_volume_by_ticker ===")
try:
    df = krx.get_market_trading_volume_by_ticker("20231229", market="KOSPI")
    print("컬럼:", df.columns.tolist())
    print(df.head(3))
except Exception as e:
    print(f"실패: {e}")

time.sleep(0.3)

print("\n=== get_market_price_change_by_ticker ===")
try:
    df = krx.get_market_price_change_by_ticker("20231227", "20231229", market="KOSPI")
    print("컬럼:", df.columns.tolist())
    print(df.head(3))
except Exception as e:
    print(f"실패: {e}")