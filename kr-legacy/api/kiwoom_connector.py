# -*- coding: utf-8 -*-
"""
kiwoom_connector.py — Kiwoom OpenAPI+ login helper
====================================================
Adapted from Gen3 api/kiwoom_api_wrapper.py.

Usage:
  from PyQt5.QtWidgets import QApplication
  app = QApplication(sys.argv)
  kiwoom, server_type = create_loggedin_kiwoom()
  provider = Gen4KiwoomProvider(kiwoom)

Requirements:
  - Kiwoom OpenAPI+ installed (32-bit)
  - Python 3.9 32-bit
  - QApplication must be created BEFORE calling this
"""
import sys
import logging
from typing import Tuple

logger = logging.getLogger("gen4.kiwoom")


def create_loggedin_kiwoom() -> Tuple:
    """
    Kiwoom OpenAPI+ QEventLoop login.

    Returns:
        (kiwoom: QAxWidget, server_type: "MOCK" | "REAL")

    Raises:
        ImportError: PyQt5 not installed
        RuntimeError: COM load failure or login failure
    """
    try:
        from PyQt5.QtWidgets import QApplication
        from PyQt5.QAxContainer import QAxWidget
        from PyQt5.QtCore import QEventLoop
    except ImportError as e:
        raise ImportError(f"PyQt5 / QAxContainer not available: {e}") from e

    app = QApplication.instance()
    if app is None:
        raise RuntimeError(
            "QApplication not found. "
            "Create QApplication(sys.argv) before calling this function."
        )

    # 1) QAxWidget + COM control
    kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

    ctrl_name = kiwoom.control()
    if not ctrl_name:
        raise RuntimeError(
            "Kiwoom OCX control load failed.\n"
            "Check: 32-bit Kiwoom OpenAPI+ installed, "
            "32-bit Python, regsvr32 registration."
        )

    if not hasattr(kiwoom, "OnEventConnect"):
        raise RuntimeError(
            "OnEventConnect event missing. "
            "Kiwoom OpenAPI+ installation or COM registration issue."
        )

    # 2) QEventLoop login
    login_loop = QEventLoop()
    login_ok = {"value": False}

    def on_event_connect(err_code):
        if err_code == 0:
            logger.info("Kiwoom login SUCCESS")
            login_ok["value"] = True
        else:
            logger.error(f"Kiwoom login FAILED (code: {err_code})")
        login_loop.quit()

    kiwoom.OnEventConnect.connect(on_event_connect)

    logger.info("CommConnect() — complete the login popup...")
    kiwoom.dynamicCall("CommConnect()")
    login_loop.exec_()

    if not login_ok["value"]:
        raise RuntimeError("Kiwoom login failed. Check credentials/certificate.")

    # 3) Server type
    server_gubun = str(kiwoom.dynamicCall(
        'KOA_Functions(QString,QString)', "GetServerGubun", ""
    )).strip()

    if server_gubun == "1":
        server_type = "MOCK"
        logger.info("*** MOCK SERVER (paper trading) ***")
    else:
        server_type = "REAL"
        logger.info("REAL SERVER confirmed")

    # 4) Account password window
    logger.info("Account password window — enter password and click [Register]")
    kiwoom.dynamicCall('KOA_Functions(QString,QString)', "ShowAccountWindow", "")

    return kiwoom, server_type
