@echo off
cd /d "%~dp0"

setlocal enableextensions enabledelayedexpansion


:: Run unit tests only
echo Running tests...
python -m pytest -q test.py
if errorlevel 1 (
    echo [ABORTED] Tests failed. Aborting.
    pause
    exit /b 1
)

:: Run continuous scan
echo Starting continuous_scan.py...
python continuous_scan.py

echo Press Ctrl+C to stop scanning.

echo Done.
pause
