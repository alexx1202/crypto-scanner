@echo off
cd /d "%~dp0"

setlocal enableextensions enabledelayedexpansion

:: Email alert settings - replace with your SMTP details
set SMTP_HOST=smtp.example.com
set SMTP_PORT=587
set SMTP_USER=my_user
set SMTP_PASS=my_pass
set EMAIL_TO=user@example.com
set EMAIL_FROM=scanner@example.com

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
