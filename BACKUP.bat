@echo off
set "SRC=C:\Users\User\OneDrive\Documents\CRYPTO\PYTHON\WORK_IN_PROGRESS\cryptoscanner"
set "DEST=%SRC%\backup"

echo Creating backup folder if it doesn't exist...
if not exist "%DEST%" mkdir "%DEST%"

echo Copying .py and .bat files...
copy "%SRC%\*.py" "%DEST%\" /Y >nul
copy "%SRC%\*.bat" "%DEST%\" /Y >nul

echo ✅ Backup complete. Files copied to %DEST%
pause
