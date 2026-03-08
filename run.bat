@echo off
echo Starting JPK Auditor...
setlocal

:: Sprawdź czy działa 'py'
py --version >nul 2>&1
if %errorlevel% == 0 (
    set PY_CMD=py
    goto :start
)

:: Sprawdź czy działa 'python'
python --version >nul 2>&1
if %errorlevel% == 0 (
    set PY_CMD=python
    goto :start
)

echo BŁĄD: Nie znaleziono polecenia 'py' ani 'python'. 
echo Upewnij się, że Python jest zainstalowany i dodany do PATH.
pause
exit /b

:start
echo Uruchamianie aplikacji...
%PY_CMD% -m uvicorn app.main:app --host 127.0.0.1 --port 8000
echo Serwer został zatrzymany. Restartowanie za 2 sekundy...
timeout /t 2 >nul
goto :start
