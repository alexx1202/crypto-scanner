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

:: Run main script
echo Running scan.py...
python scan.py

echo Done.
pause
