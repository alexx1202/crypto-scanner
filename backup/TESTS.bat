@echo off
setlocal

:: Set working directory and log directory
set "WORKDIR=C:\Users\User\OneDrive\Documents\CRYPTO\PYTHON\WORK_IN_PROGRESS\cryptoscanner"
set "LOGDIR=%WORKDIR%\logs"

cd /d "%WORKDIR%"

:: Ensure log directory exists and is clean
if exist "%LOGDIR%" rd /s /q "%LOGDIR%"
mkdir "%LOGDIR%"

echo.
echo Running pylint...
pylint scan.py > "%LOGDIR%\pylint.log"
type "%LOGDIR%\pylint.log"

echo.
echo Running pytest...
pytest test.py > "%LOGDIR%\pytest.log"
type "%LOGDIR%\pytest.log"

echo.
echo ✅ Tests and linting complete.
pause
