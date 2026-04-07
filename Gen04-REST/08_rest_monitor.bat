@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\Gen04-REST"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe

echo ============================================================
echo  Q-TRON REST Monitor Dashboard
echo  http://localhost:8080
echo  %DATE% %TIME%
echo ============================================================
echo.

"%PYTHON%" -m uvicorn web.app:app --host 0.0.0.0 --port 8080

pause
