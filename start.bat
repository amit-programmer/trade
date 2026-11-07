@echo off
title 24/7 SMC Trading System (Server + Bot)
cd /d "%~dp0"
echo ====================================================
echo Starting 24/7 SMC Trading System (Server + Bot)...
echo ====================================================

if exist venv\Scripts\python.exe (
    set "PYTHON_EXE=venv\Scripts\python.exe"
) else (
    set "PYTHON_EXE=python"
)

%PYTHON_EXE% run_all.py
pause

