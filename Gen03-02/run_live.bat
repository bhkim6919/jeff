@echo off
chcp 65001 > nul
cd /d C:\Q-TRON-32_ARCHIVE\Gen03-02

set PYTHON32=C:\Users\User\AppData\Local\Programs\Python\Python39-32\python.exe
set LOGFILE=C:\Q-TRON-32_ARCHIVE\Gen03-02\data\logs\run_live.log

if not exist "%PYTHON32%" (
    echo [ERROR] Python32 not found: %PYTHON32%
    pause
    exit /b 1
)

echo [QTRON] Starting LIVE mode...
echo [QTRON] Log: %LOGFILE%
echo [QTRON] Kiwoom login popup - check taskbar if not visible
echo.

"%PYTHON32%" -u main.py > "%LOGFILE%" 2>&1

echo.
echo [QTRON] Done. Exit code: %ERRORLEVEL%
echo [QTRON] --- Log output ---
type "%LOGFILE%"
echo [QTRON] --- End ---
echo.
pause
