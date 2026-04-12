@echo off
:: LIVE 직접 실행 (Task Scheduler 경유로 UAC 우회)
schtasks /run /tn "Q-TRON_AutoStart"
if errorlevel 1 (
    echo [FAIL] Task not found. Run setup_scheduler.bat first.
    pause
)
