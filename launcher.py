import uvicorn
import threading
import webbrowser
import time
import os
import sys
from app.main import app

def open_browser():
    """
    Waits a moment for the server to start, then opens the default web browser.
    """
def open_browser():
    """Otwiera przeglądarkkę po upewnieniu się, że serwer żyje."""
    import urllib.request
    for _ in range(10):  # 10 prób co 500ms
        try:
            with urllib.request.urlopen("http://127.0.0.1:8000/health") as response:
                if response.getcode() == 200:
                    webbrowser.open("http://127.0.0.1:8000")
                    return
        except Exception:
            pass
        time.sleep(0.5)
    webbrowser.open("http://127.0.0.1:8000")

def main():
    # If we are in a frozen bundle, we might need to adjust some paths or env vars if needed.
    # But usually uvicorn.run with app object works fine.
    
    # Start browser in a separate thread
    threading.Thread(target=open_browser, daemon=True).start()
    
    # Patch stdout/stderr for --noconsole mode (where they are None)
    if sys.stdout is None:
        sys.stdout = open(os.devnull, "w")
    if sys.stderr is None:
        sys.stderr = open(os.devnull, "w")
        
    # Run the server
    # Note: reload=False for production/frozen app
    # workers=1 is standard for this type of local app
    # log_config=None prevents uvicorn from trying to configure its default logger which causes the creation error
    # causing the "AttributeError: 'NoneType' object has no attribute 'isatty'"
    # We can also pass use_colors=False
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="info", use_colors=False)

if __name__ == "__main__":
    main()
