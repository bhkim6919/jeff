@echo off
chcp 65001 > nul
title Q-TRON Gen3 -- pykrx Runtime
cd /d C:\Q-TRON-32_ARCHIVE\Gen03-02
C:\Q-TRON-32_ARCHIVE\.venv\Scripts\python.exe main.py --pykrx
pause
