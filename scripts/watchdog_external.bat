@echo off
REM scripts/watchdog_external.bat — Standalone dead-man switch wrapper
REM
REM Invoked by Windows Task Scheduler every 15 minutes. Executes one
REM watchdog pass against kr/data/pipeline/ and exits.
REM
REM This wrapper uses the project's .venv64 Python 3.12 interpreter but
REM the Python script itself has ZERO Q-TRON imports — the venv is used
REM only to get a known Python runtime.
REM
REM Environment variables required (set in Task Scheduler or user env):
REM   QTRON_TELEGRAM_TOKEN_DEADMAN
REM   QTRON_TELEGRAM_CHAT_ID_DEADMAN
REM
REM Optional:
REM   QTRON_PIPELINE_DATA_DIR     (default: <repo>/kr/data/pipeline)
REM   QTRON_WATCHDOG_INCIDENT_DIR (default: <repo>/backup/reports/incidents)
REM
REM 2026-04-24 Jeff 보고건: Task Scheduler 가 이 .bat 을 호출하면서
REM python.exe 가 15분마다 빈 cmd 창을 띄움. pythonw.exe 로 전환하여
REM console 자체를 만들지 않게 한다. stdout(json summary) 는 로그파일로
REM 보존해 운영자가 필요할 때 tail 가능.

setlocal

REM Resolve repo root from this script's location (scripts/ sibling)
set "SCRIPT_DIR=%~dp0"
set "REPO_ROOT=%SCRIPT_DIR%.."
pushd "%REPO_ROOT%"

REM Prefer the 64-bit venv Python (pythonw = console-less);
REM fall back to console python only if the windowed variant is missing.
set "PY_EXE=%REPO_ROOT%\.venv64\Scripts\pythonw.exe"
if not exist "%PY_EXE%" (
    set "PY_EXE=%REPO_ROOT%\.venv64\Scripts\python.exe"
)
if not exist "%PY_EXE%" (
    set "PY_EXE=pythonw"
)

REM Log dir for stdout capture (best-effort — non-fatal if creation fails).
set "LOG_DIR=%REPO_ROOT%\backup\reports\incidents"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%" 2>nul

"%PY_EXE%" "%SCRIPT_DIR%watchdog_external.py" %* >> "%LOG_DIR%\watchdog_stdout.log" 2>&1
set "EXIT_CODE=%ERRORLEVEL%"

popd
endlocal & exit /b %EXIT_CODE%
