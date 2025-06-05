@echo off
cd /d "%~dp0"

setlocal enableextensions enabledelayedexpansion

:: Run style checks and unit tests
echo Running lint and tests...
python run_checks.py
if errorlevel 1 (
    echo [ABORTED] Lint or tests failed. Aborting.
    pause
    exit /b 1
)

:: Run main script
echo Running scan.py...
python scan.py

echo Done.
pause
