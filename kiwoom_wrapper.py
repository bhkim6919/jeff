# kiwoom_wrapper.py (교체 버전 제안)

from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtWidgets import QApplication


def create_loggedin_kiwoom():
    """
    Kiwoom OpenAPI+ QAxWidget 로그인 래퍼
    - 전제: QApplication은 main.py에서 이미 생성되어 있어야 함
    """
    app = QApplication.instance()
    if app is None:
        raise RuntimeError(
            "QApplication 인스턴스가 없습니다. "
            "main.py에서 먼저 QApplication을 생성한 후 호출해야 합니다."
        )

    # 1) QAxWidget 생성 + 컨트롤 로딩
    kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

    ctrl_name = kiwoom.control()
    print(f"[Kiwoom] control() = {ctrl_name!r}")

    if not ctrl_name:
        # COM 컨트롤이 실제로 로드되지 않음
        raise RuntimeError(
            "Kiwoom OCX 컨트롤 로드에 실패했습니다.\n"
            "- Kiwoom OpenAPI+ 가 32bit로 설치되었는지,\n"
            "- 현재 32bit Python 환경에서 실행 중인지,\n"
            "- 관리자 권한으로 설치/등록(regsvr32)이 되었는지\n"
            "를 다시 확인해 주세요."
        )

    if not hasattr(kiwoom, "OnEventConnect"):
        # 이벤트 메타 정보가 붙지 않은 상태
        raise RuntimeError(
            "Kiwoom QAxWidget에 OnEventConnect 이벤트가 없습니다.\n"
            "대부분의 경우 Kiwoom OpenAPI+ 설치/등록 문제가 원인입니다."
        )

    # 2) 로그인 처리
    login_loop = QEventLoop()

    def on_login(err_code):
        if err_code == 0:
            print("[✅] Kiwoom 로그인 성공")
        else:
            print(f"[❌] 로그인 실패 (코드: {err_code})")
        login_loop.quit()

    kiwoom.OnEventConnect.connect(on_login)

    print("[Kiwoom] CommConnect() 호출")
    kiwoom.dynamicCall("CommConnect()")

    # 로그인 완료까지 블록
    login_loop.exec_()

    return kiwoom