@echo off
chcp 65001 > nul
echo ============================================================
echo  Gen4 Fundamental Data Collector (Daily Only)
echo  %date% %time%
echo ============================================================
echo.

cd /d "C:\Q-TRON-32_ARCHIVE\kr-legacy"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo [1] Daily snapshot (today)
echo     -> backtest\data_full\fundamental\fundamental_YYYYMMDD.csv
echo.
"%PYTHON%" -u -m data.fundamental_collector --mode daily
echo.

echo ============================================================
echo  Done.
echo ============================================================
pause
