@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe
set LOGFILE=C:\Q-TRON-32_ARCHIVE\Gen04\data\logs\batch_scheduled.log

:: ── Skip check: if today's target already exists, skip batch ──
:: This allows manual evening batch to prevent redundant morning run
"%PYTHON%" -c "from datetime import date; from pathlib import Path; p=Path('data/signals/target_portfolio_'+date.today().strftime('%%Y%%m%%d')+'.json'); exit(0 if p.exists() else 1)"
if not errorlevel 1 (
    echo [BATCH_SKIP] %DATE% %TIME% — target already exists >> "%LOGFILE%"
    exit /b 0
)

echo ============================================================ >> "%LOGFILE%"
echo  [SCHEDULED_BATCH] %DATE% %TIME% >> "%LOGFILE%"
echo ============================================================ >> "%LOGFILE%"

:: Morning batch: --fast mode (OHLCV + scoring only, ~18min)
:: Fundamental/reports skipped to minimize runtime before market open
"%PYTHON%" -u main.py --batch --fast >> "%LOGFILE%" 2>&1
if errorlevel 1 (
    echo [BATCH_FAIL] %DATE% %TIME% errorlevel=%errorlevel% >> "%LOGFILE%"
    exit /b 1
)

echo [BATCH_OK] %DATE% %TIME% >> "%LOGFILE%"
exit /b 0
