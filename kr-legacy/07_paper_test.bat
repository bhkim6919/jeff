@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\kr-legacy"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

:: Usage: 07_paper_test.bat [cycle] [fresh]
::   cycle: full (default), sell_only, buy_only
::   fresh: add "fresh" to reset state from broker
set CYCLE=%1
if "%CYCLE%"=="" set CYCLE=full

set FRESH_FLAG=
if /i "%2"=="fresh" set FRESH_FLAG=--fresh
if /i "%1"=="fresh" (
    set CYCLE=full
    set FRESH_FLAG=--fresh
)

echo ============================================================
echo  [7] Gen4 Paper Test - Cycle: %CYCLE% %FRESH_FLAG%
echo  %DATE% %TIME%
echo ============================================================

"%PYTHON%" -u main.py --paper-test --cycle %CYCLE% %FRESH_FLAG%

echo ============================================================
echo  Paper test session ended.
echo ============================================================
pause
