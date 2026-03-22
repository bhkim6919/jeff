# -*- coding: utf-8 -*-
"""
KiwoomApiWrapper
================
Kiwoom OpenAPI+ 로그인 헬퍼.

  - kiwoom_api_wrapper.py : 로그인 + QApplication 생명주기 관리
  - data/kiwoom_provider.py : 실제 TR 조회 DataProvider 구현

사용 예:
  from api.kiwoom_api_wrapper import create_loggedin_kiwoom
  kiwoom = create_loggedin_kiwoom()
  provider = KiwoomProvider(kiwoom)

전제:
  - Kiwoom OpenAPI+ 설치 및 서버 연결 상태
  - Python 3.9 32비트 환경 (Kiwoom API 요구사항)
  - QApplication은 main.py 에서 미리 생성되어 있어야 함
"""

import sys


def create_loggedin_kiwoom():
    """
    Kiwoom OpenAPI+ QEventLoop 기반 로그인 후 kiwoom 객체 반환.

    - QApplication 인스턴스가 이미 존재해야 한다 (main.py 에서 생성).
    - 로그인 팝업 완료까지 QEventLoop 로 블록킹 대기.
    - 로그인 실패 시 RuntimeError, COM 컨트롤 미로드 시 RuntimeError.
    """
    try:
        from PyQt5.QtWidgets import QApplication
        from PyQt5.QAxContainer import QAxWidget
        from PyQt5.QtCore import QEventLoop
    except ImportError as e:
        raise ImportError(f"PyQt5 또는 QAxContainer 없음: {e}") from e

    app = QApplication.instance()
    if app is None:
        raise RuntimeError(
            "QApplication 인스턴스가 없습니다. "
            "main.py에서 먼저 QApplication(sys.argv)을 생성한 후 호출해야 합니다."
        )

    # 1) QAxWidget 생성 + COM 컨트롤 로딩 확인
    kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

    ctrl_name = kiwoom.control()
    if not ctrl_name:
        raise RuntimeError(
            "Kiwoom OCX 컨트롤 로드에 실패했습니다.\n"
            "- Kiwoom OpenAPI+가 32bit로 설치되었는지\n"
            "- 현재 32bit Python 환경에서 실행 중인지\n"
            "- 관리자 권한으로 regsvr32 등록이 완료되었는지 확인하세요."
        )

    if not hasattr(kiwoom, "OnEventConnect"):
        raise RuntimeError(
            "Kiwoom QAxWidget에 OnEventConnect 이벤트가 없습니다.\n"
            "Kiwoom OpenAPI+ 설치 또는 COM 등록 문제가 원인일 수 있습니다."
        )

    # 2) QEventLoop 기반 로그인 (busy-wait 없이 블록킹 대기)
    login_loop = QEventLoop()
    login_ok   = {"value": False}

    def on_event_connect(err_code):
        if err_code == 0:
            print("[Kiwoom] 로그인 성공")
            login_ok["value"] = True
        else:
            print(f"[Kiwoom] 로그인 실패 (코드: {err_code})")
        login_loop.quit()

    kiwoom.OnEventConnect.connect(on_event_connect)

    print("[Kiwoom] CommConnect() 호출 - 로그인 팝업을 완료하세요.")
    kiwoom.dynamicCall("CommConnect()")

    # 로그인 완료까지 블록
    login_loop.exec_()

    if not login_ok["value"]:
        raise RuntimeError("[Kiwoom] 로그인에 실패했습니다. 계정/비밀번호 또는 인증서를 확인하세요.")

    # 3) 서버 구분 확인
    server_gubun = str(kiwoom.dynamicCall(
        'KOA_Functions(QString,QString)', "GetServerGubun", ""
    )).strip()
    if server_gubun == "1":
        print("[Kiwoom] *** MOCK SERVER (test) ***")
    else:
        print("[Kiwoom] REAL server confirmed.")

    # 4) 계좌비밀번호 입력창 표시 (로그인 비밀번호와 별도)
    #    이 창에서 계좌 거래 비밀번호를 입력해야 TR 조회(opw00018 등)가 가능
    print("[Kiwoom] 계좌비밀번호 입력창을 표시합니다...")
    print("[Kiwoom]   ※ 비밀번호 입력 후 [등록] → 창 닫기")
    kiwoom.dynamicCall('KOA_Functions(QString,QString)', "ShowAccountWindow", "")

    return kiwoom
