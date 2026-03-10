@echo off
chcp 65001 > nul
title Q-TRON Gen3 -- LIVE 진단
cd /d C:\Q-TRON-32_ARCHIVE\Gen03-02

set PYTHON32=C:\Users\User\AppData\Local\Programs\Python\Python39-32\python.exe
set LOGFILE=C:\Q-TRON-32_ARCHIVE\Gen03-02\data\logs\diagnose_live.log

echo ============================================================
echo  Q-TRON LIVE 진단 스크립트  (%DATE% %TIME%)
echo ============================================================
echo.

:: 1) 32비트 Python 존재 확인
echo [1] 32비트 Python 경로 확인...
if not exist "%PYTHON32%" (
    echo     [FAIL] 없음: %PYTHON32%
    goto END
)
echo     [OK] %PYTHON32%
echo.

:: 2) Python 버전 + 아키텍처
echo [2] Python 버전 및 아키텍처...
"%PYTHON32%" -c "import sys, struct; print('  버전:', sys.version[:6]); print('  아키텍처:', struct.calcsize('P')*8, 'bit')"
echo.

:: 3) PyQt5 임포트
echo [3] PyQt5 임포트...
"%PYTHON32%" -c "from PyQt5.QtWidgets import QApplication; from PyQt5.QAxContainer import QAxWidget; print('  [OK] PyQt5')" 2>&1
echo.

:: 4) pandas 임포트
echo [4] pandas 임포트...
"%PYTHON32%" -c "import pandas; print('  [OK] pandas', pandas.__version__)" 2>&1
echo.

:: 5) Gen3 config 임포트
echo [5] Gen3Config 임포트...
"%PYTHON32%" -c "import sys; sys.path.insert(0,'.'); from config import Gen3Config; print('  [OK] Gen3Config')" 2>&1
echo.

:: 6) KiwoomProvider 임포트
echo [6] KiwoomProvider 임포트...
"%PYTHON32%" -c "import sys; sys.path.insert(0,'.'); from data.kiwoom_provider import KiwoomProvider; print('  [OK] KiwoomProvider')" 2>&1
echo.

:: 7) 현재 시각 + 15:30 게이트 확인  ← 핵심 진단
echo [7] 현재 시각 / 장중 게이트...
"%PYTHON32%" -c "from datetime import datetime,time as dtime; n=datetime.now(); gate=(n.time()>=dtime(15,30)); print('  현재시각:', n.strftime('%%H:%%M:%%S')); print('  장 종료 후(15:30이후)?', gate); print('  --> 해결:', '지금은 실행 불가. 내일 09:00 전에 실행하세요.' if gate else 'OK - 실행 가능한 시간대')"
echo.

:: 8) main.py 전체 실행 (로그 저장)
echo [8] main.py 실행 중... (결과 → %LOGFILE%)
"%PYTHON32%" main.py > "%LOGFILE%" 2>&1
echo     종료코드: %ERRORLEVEL%
echo.
echo     --- main.py 출력 내용 ---
type "%LOGFILE%"
echo     --- 끝 ---
echo.

:END
echo ============================================================
echo  진단 완료. 위 내용을 Claude 에 복사-붙여넣기 하세요.
echo ============================================================
pause
