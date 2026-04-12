@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  [4] Gen4 Reports - Generate All
echo  %DATE% %TIME%
echo ============================================================
echo.

echo [Daily Report]
"%PYTHON%" -u -m report.daily_report
echo.

echo [Weekly Report]
"%PYTHON%" -u -m report.weekly_report
echo.

echo [Monthly Report]
"%PYTHON%" -u -m report.monthly_report
echo.

echo ============================================================
echo  Reports generated in: report\output\
echo ============================================================
pause
