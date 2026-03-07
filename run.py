import multiprocessing
import uvicorn
import webbrowser
import time
import sys
import os
from threading import Thread

# Fix 'isatty' error in PyInstaller --noconsole mode
if sys.stdout is None:
    sys.stdout = open(os.devnull, 'w')
if sys.stderr is None:
    sys.stderr = open(os.devnull, 'w')

# PyInstaller Splash Screen logic (optional here, but good practice)
try:
    import pyi_splash
except ImportError:
    pyi_splash = None

def open_browser():
    """Otwiera przeglądarkę automatycznie po uruchomieniu serwera."""
    time.sleep(2.0)
    webbrowser.open("http://127.0.0.1:8000")

if __name__ == "__main__":
    # Obsługa procesów w wersji skompilowanej
    multiprocessing.freeze_support()
    
    # Uruchomienie przeglądarki w osobnym wątku
    Thread(target=open_browser, daemon=True).start()
    
    # Uvicorn configuration to avoid interactive terminal issues
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["default"]["fmt"] = "%(levelname)s: %(message)s"
    log_config["formatters"]["access"]["fmt"] = "%(levelname)s: %(message)s"
    
    # Import aplikacji
    from app.main import app
    
    # Uruchomienie serwera
    uvicorn.run(app, host="127.0.0.1", port=8000, log_config=log_config)
