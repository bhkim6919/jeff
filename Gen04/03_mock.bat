@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  [3] Gen4 Mock - State Check (no broker)
echo  %DATE% %TIME%
echo ============================================================
echo.

"%PYTHON%" -u main.py --mock
pause
