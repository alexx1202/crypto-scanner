@echo off
cd /d "%~dp0"

setlocal enableextensions enabledelayedexpansion

:: Create logs directory if it doesn't exist
if not exist "logs" mkdir logs

:: Set paths
set PYLINT_LOG=logs\pylint.log
set PYTEST_LOG=logs\pytest.log

:: Run Pylint
echo Running pylint on all .py files...
pylint core.py scan.py test.py volume_math.py > "!PYLINT_LOG!"
type "!PYLINT_LOG!"

:: Check for exact 10.00/10 rating
findstr /R /C:"Your code has been rated at 10.00/10" "!PYLINT_LOG!" >nul
if errorlevel 1 (
    echo [ABORTED] Pylint score is less than 10.00/10. Aborting.
    pause
    exit /b 1
)

:: Run Pytest (with -s to allow print debugging output)
echo Running pytest...
pytest -s test.py > "!PYTEST_LOG!"
if errorlevel 1 (
    type "!PYTEST_LOG!"
    echo [ABORTED] Pytest failures detected. Aborting.
    pause
    exit /b 1
) else (
    type "!PYTEST_LOG!"
    echo All tests passed.
)

:: Run main script
echo Running scan.py...
python scan.py

echo Done.
pause
