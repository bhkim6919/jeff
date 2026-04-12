@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe
set LOGFILE=C:\Q-TRON-32_ARCHIVE\Gen04\data\logs\trading_day_check.log

echo [BOOT_CHECK] %DATE% %TIME% >> "%LOGFILE%"

:: Check if today is a trading day (pykrx + weekday fallback)
"%PYTHON%" -c "import sys; from datetime import date; today=date.today(); wd=today.weekday(); exec('if wd>=5:\n sys.exit(1)'); from pykrx.stock import get_market_ohlcv_by_date as g; import pandas as pd; df=g(today.strftime('%%Y%%m%%d'),today.strftime('%%Y%%m%%d'),'005930'); sys.exit(0 if len(df)>0 else 1)" 2>>"%LOGFILE%"
if errorlevel 1 (
    echo [NON_TRADING_DAY] %DATE% — shutting down in 120s >> "%LOGFILE%"
    shutdown /s /t 120 /c "Q-TRON: Non-trading day — auto shutdown"
    exit /b 0
)

echo [TRADING_DAY] %DATE% — proceeding >> "%LOGFILE%"
exit /b 0
