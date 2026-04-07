@echo off
chcp 65001 > nul
echo ============================================================
echo  Q-TRON Task Scheduler Setup
echo  - 07:52 Trading Day Check (daily, non-trading → shutdown)
echo  - 07:55 Morning Batch (--fast, ~18min)
echo  - 08:30 Auto Start (Kiwoom + LIVE + Win+M)
echo  - 08:35 REST Monitor Dashboard
echo  - 16:00 EOD Cleanup + PC Shutdown
echo ============================================================
echo.

:: 0. Trading Day Check at 07:52 (DAILY — including weekends/holidays)
::    BIOS wakes every day; this shuts down if not a trading day
schtasks /create /tn "Q-TRON_TradingDayCheck" /tr "C:\Q-TRON-32_ARCHIVE\Gen04\00_check_trading_day.bat" /sc daily /st 07:52 /rl HIGHEST /f
if errorlevel 1 (
    echo [FAIL] Q-TRON_TradingDayCheck registration failed
) else (
    echo [OK] Q-TRON_TradingDayCheck registered: daily 07:52
)
echo.

:: 1. Morning Batch at 07:55 (weekdays only)
schtasks /create /tn "Q-TRON_MorningBatch" /tr "C:\Q-TRON-32_ARCHIVE\Gen04\01_batch_scheduled.bat" /sc weekly /d MON,TUE,WED,THU,FRI /st 07:55 /rl HIGHEST /f
if errorlevel 1 (
    echo [FAIL] Q-TRON_MorningBatch registration failed
) else (
    echo [OK] Q-TRON_MorningBatch registered: weekdays 07:55
)
echo.

:: 2. Auto Start at 08:30 (weekdays only)
::    Kiwoom login + password + LIVE engine + Win+M
::    /rl HIGHEST = bypasses UAC prompt
schtasks /create /tn "Q-TRON_AutoStart" /tr "C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe C:\Q-TRON-32_ARCHIVE\Gen04\auto_start.py" /sc weekly /d MON,TUE,WED,THU,FRI /st 08:30 /rl HIGHEST /f
if errorlevel 1 (
    echo [FAIL] Q-TRON_AutoStart registration failed
) else (
    echo [OK] Q-TRON_AutoStart registered: weekdays 08:30
)
echo.

:: 2.5 REST Monitor Dashboard at 08:35 (weekdays only)
::     Starts after LIVE to ensure COM is running first
schtasks /create /tn "Q-TRON_RESTMonitor" /tr "C:\Q-TRON-32_ARCHIVE\Gen04-REST\08_rest_monitor.bat" /sc weekly /d MON,TUE,WED,THU,FRI /st 08:35 /rl HIGHEST /f
if errorlevel 1 (
    echo [FAIL] Q-TRON_RESTMonitor registration failed
) else (
    echo [OK] Q-TRON_RESTMonitor registered: weekdays 08:35
)
echo.

:: 3. EOD Cleanup at 16:00 (weekdays only)
schtasks /create /tn "Q-TRON_EOD_Cleanup" /tr "C:\Q-TRON-32_ARCHIVE\Gen04\99_eod_shutdown.bat" /sc weekly /d MON,TUE,WED,THU,FRI /st 16:00 /rl HIGHEST /f
if errorlevel 1 (
    echo [FAIL] Q-TRON_EOD_Cleanup registration failed
) else (
    echo [OK] Q-TRON_EOD_Cleanup registered: weekdays 16:00
)
echo.

:: Verify
echo ============================================================
echo  Registered Tasks:
echo ============================================================
schtasks /query /tn "Q-TRON_TradingDayCheck" /fo TABLE 2>nul
schtasks /query /tn "Q-TRON_MorningBatch" /fo TABLE 2>nul
schtasks /query /tn "Q-TRON_AutoStart" /fo TABLE 2>nul
schtasks /query /tn "Q-TRON_RESTMonitor" /fo TABLE 2>nul
schtasks /query /tn "Q-TRON_EOD_Cleanup" /fo TABLE 2>nul
echo.
pause
