@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04"

set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe

echo ============================================================
echo  Q-TRON Gen4 Core - Mock Test
echo  %DATE% %TIME%
echo ============================================================

"%PYTHON%" -u main.py --mock

echo.
pause
