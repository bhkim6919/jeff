# C:\Q-TRON-32\test_opt20006.py

import sys
from PyQt5.QtWidgets import QApplication
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from data.kiwoom_provider import KiwoomProvider

app = QApplication(sys.argv)

# 1️⃣ 컨트롤 생성
k = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

# 2️⃣ 로그인 이벤트 루프
login_loop = QEventLoop()

def on_login(err_code):
    if err_code == 0:
        print("✅ 로그인 성공")
    else:
        print("❌ 로그인 실패:", err_code)
    login_loop.quit()

k.OnEventConnect.connect(on_login)

print("🔐 로그인 시도")
k.dynamicCall("CommConnect()")
login_loop.exec_()

# 3️⃣ Provider 생성
provider = KiwoomProvider(k)

# 4️⃣ TR 테스트
print("📡 opt20006 호출 시작")
df = provider.get_index_ohlcv("KOSPI", 60)

print("결과:")
print(df.tail())