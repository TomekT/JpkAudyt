import multiprocessing
import uvicorn
import webbrowser
import time
import sys
import os
import signal
from threading import Thread

# Import psutil for port management
import psutil

# Fix 'isatty' error in PyInstaller --noconsole mode
if sys.stdout is None:
    sys.stdout = open(os.devnull, 'w')
if sys.stderr is None:
    sys.stderr = open(os.devnull, 'w')

# PyInstaller Splash Screen logic
try:
    import pyi_splash
except ImportError:
    pyi_splash = None

def kill_process_on_port(port: int):
    """Sprawdza czy port jest zajęty i zabija proces przed startem."""
    print(f"Sprawdzanie portu {port}...")
    try:
        for conn in psutil.net_connections(kind='inet'):
            if conn.laddr.port == port and conn.status == 'LISTEN':
                pid = conn.pid
                if pid:
                    try:
                        proc = psutil.Process(pid)
                        print(f"Znaleziono proces trzymający port {port}: {proc.name()} (PID: {pid}). Zabijanie...")
                        proc.terminate()
                        time.sleep(0.5)
                        if proc.is_running():
                            proc.kill()
                        print(f"Port {port} został zwolniony.")
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        print(f"Nie można zamknąć procesu PID {pid} (brak uprawnień).")
    except Exception as e:
        print(f"Błąd podczas sprawdzania portów: {e}")

def open_browser():
    """Otwiera przeglądarkę automatycznie po uruchomieniu serwera."""
    time.sleep(2.5)
    webbrowser.open("http://127.0.0.1:8000")

if __name__ == "__main__":
    # Obsługa procesów w wersji skompilowanej
    multiprocessing.freeze_support()
    
    # Uruchomienie przeglądarki w osobnym wątku
    Thread(target=open_browser, daemon=True).start()
    
    # Uvicorn configuration
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["default"]["fmt"] = "%(levelname)s: %(message)s"
    log_config["formatters"]["access"]["fmt"] = "%(levelname)s: %(message)s"
    
    while True:
        kill_process_on_port(8000)
        print("Uruchamianie serwera...")
        
        # Uruchomienie uvicorn w procesie potomnym
        server_process = multiprocessing.Process(
            target=uvicorn.run,
            args=("app.main:app",),
            kwargs={
                "host": "127.0.0.1",
                "port": 8000,
                "workers": 1,
                "loop": "asyncio",
                "log_config": log_config
            }
        )
        
        try:
            server_process.start()
            server_process.join()  # Czeka, aż proces uvicorn zakończy się (np. przez os._exit)
        except Exception as e:
            print(f"Błąd procesu serwera: {e}")
        
        print("Restartowanie serwera za 2 sekundy...")
        time.sleep(2)
