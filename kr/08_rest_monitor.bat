@echo off
chcp 65001 > nul
cd /d "C:\Q-TRON-32_ARCHIVE\kr"
set PYTHON=C:\Q-TRON-32_ARCHIVE\.venv64\Scripts\python.exe
set PORT=8080

:: Kill existing process on port
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
    echo [PORT] %PORT% occupied by PID %%a - killing...
    taskkill /PID %%a /F >nul 2>&1
    timeout /t 1 /nobreak >nul
)

echo ============================================================
echo  Q-TRON REST Monitor Dashboard
echo  http://localhost:%PORT%
echo  %DATE% %TIME%
echo ============================================================
echo.

"%PYTHON%" main.py --server

pause
