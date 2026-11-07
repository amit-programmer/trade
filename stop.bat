@echo off
title Stop All Trading Bot Instances
cd /d "%~dp0"
echo ====================================================
echo Stopping all trading bot and server processes...
echo ====================================================

if exist venv\Scripts\python.exe (
    set "PYTHON_EXE=venv\Scripts\python.exe"
) else (
    set "PYTHON_EXE=python"
)

%PYTHON_EXE% stop_all.py
pause
