@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\kr-legacy"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  [6] Gen4 Test Suite
echo  %DATE% %TIME%
echo ============================================================
echo.

echo [1/4] Pre-launch tests...
"%PYTHON%" -u test_prelaunch.py
if errorlevel 1 goto :fail
echo.

echo [2/4] Forensic logging tests...
"%PYTHON%" -u test_forensic.py
if errorlevel 1 goto :fail
echo.

echo [3/4] Daily report v2 tests...
"%PYTHON%" -u test_daily_v2.py
if errorlevel 1 goto :fail
echo.

echo [4/4] Reports v3 integration tests...
"%PYTHON%" -u test_reports_v3.py
if errorlevel 1 goto :fail
echo.

echo ============================================================
echo  ALL TESTS PASSED
echo ============================================================
pause
exit /b 0

:fail
echo.
echo === TEST FAILED ===
pause
exit /b 1
