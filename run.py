import sys, os
from pathlib import Path
import ctypes

# Dodanie folderu aplikacji do sys.path (poprawia importy w wersji binarnej)
current_dir = Path(sys.executable).parent if getattr(sys, 'frozen', False) else Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))
os.chdir(str(current_dir))

import multiprocessing
import uvicorn
import webbrowser
import time
import signal
import traceback
import logging
import urllib.request
from threading import Thread

# Import psutil for port management
import psutil

# PyInstaller Splash Screen logic
try:
    import pyi_splash
except ImportError:
    pyi_splash = None

def hide_console():
    """Ukrywa okno konsoli (Windows)."""
    try:
        # SW_HIDE = 0
        hWnd = ctypes.windll.kernel32.GetConsoleWindow()
        if hWnd:
            ctypes.windll.user32.ShowWindow(hWnd, 0)
            print("Konsola została ukryta.")
    except Exception as e:
        print(f"Nie udało się ukryć konsoli: {e}")

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
    """Otwiera przeglądarkę po upewnieniu się, że serwer żyje."""
    MAX_ATTEMPTS = 40  # 40 prób co 500ms = 20s
    for i in range(1, MAX_ATTEMPTS + 1):
        msg = f"Oczekiwanie na uruchomienie serwera (próba {i})..."
        print(msg)
        if pyi_splash:
            pyi_splash.update_text(msg)
            
        try:
            with urllib.request.urlopen("http://127.0.0.1:8000/health") as response:
                if response.getcode() == 200:
                    print("Serwer gotowy (status 200).")
                    if pyi_splash:
                        pyi_splash.update_text("Serwer gotowy. Otwieranie przeglądarki...")
                        time.sleep(0.2)
                        pyi_splash.close()
                        print("Splash screen zamknięty.")
                    
                    # Ukrycie konsoli sekundę po zamknięciu splasha
                    time.sleep(1.0)
                    hide_console()
                    
                    webbrowser.open("http://127.0.0.1:8000")
                    return
        except Exception:
            pass
        time.sleep(0.5)
    
    error_msg = "Błąd krytyczny: Serwer nie odpowiada (timeout 20s)."
    print(error_msg)
    if pyi_splash:
        pyi_splash.update_text(error_msg)
        time.sleep(5)
    webbrowser.open("http://127.0.0.1:8000")

if __name__ == "__main__":
    # 4. multiprocessing.freeze_support() musi być wywołane jako pierwsze
    multiprocessing.freeze_support()
    
    try:
        print(f"\n--- URUCHOMIENIE APLIKACJI: {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
        print(f"Katalog roboczy: {os.getcwd()}")
        print("Launcher JPK Audytor: multiprocessing.freeze_support() OK.")
        
        # Uruchomienie przeglądarki w osobnym wątku
        print("Startowanie wątku przeglądarki...")
        Thread(target=open_browser, daemon=True).start()
        
        # Uvicorn configuration
        log_config = uvicorn.config.LOGGING_CONFIG
        log_config["formatters"]["default"]["fmt"] = "%(levelname)s: %(message)s"
        log_config["formatters"]["access"]["fmt"] = "%(levelname)s: %(message)s"
        
        while True:
            if pyi_splash:
                pyi_splash.update_text(f"Próba startu systemu... (Katalog: {current_dir.name})")
            
            kill_process_on_port(8000)
            
            print("Przygotowywanie procesu serwera uvicorn...")
            server_process = multiprocessing.Process(
                target=uvicorn.run,
                args=("app.main:app",),
                kwargs={
                    "host": "127.0.0.1",
                    "port": 8000,
                    "workers": 1,
                    "loop": "asyncio",
                    "log_config": log_config,
                    "log_level": "info"
                }
            )
            
            try:
                print("Uruchamianie procesu serwera...")
                server_process.start()
                
                last_splash_update = time.time()
                while server_process.is_alive():
                    time.sleep(0.1)
                    if time.time() - last_splash_update > 3:
                        if pyi_splash:
                            try:
                                pyi_splash.update_text("Serwer pracuje...")
                            except: pass
                        last_splash_update = time.time()
                
                print(f"Serwer zakończył pracę z kodem: {server_process.exitcode}")
                if server_process.exitcode == 0:
                    print("Zamykanie launchera (inactivity timeout).")
                    break
                elif server_process.exitcode == 5:
                    print("Restartowanie serwera (hard reset)...")
                else:
                    print(f"Nagłe zakończenie pracy serwera (kod: {server_process.exitcode}). Oczekiwanie 5s...")
                    time.sleep(5)
                    
            except Exception as e:
                print(f"Błąd podczas pracy procesu serwera: {e}")
                print(traceback.format_exc())
                time.sleep(2)
            
            time.sleep(1)
            
    except Exception:
        error_detail = traceback.format_exc()
        print(f"KRYTYCZNY BŁĄD LAUNCHERA:\n{error_detail}")
        if pyi_splash:
            try:
                pyi_splash.update_text("BŁĄD KRYTYCZNY! Sprawdź logi w konsoli.")
                time.sleep(10)
            except: pass
        sys.exit(1)
