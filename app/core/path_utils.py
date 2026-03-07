import sys
import os
from pathlib import Path

def resource_path(relative_path: str) -> Path:
    """
    Get absolute path to resource, works for dev and for PyInstaller.
    """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        # In dev mode, we assume resources are at the project root
        # This file is in app/core/path_utils.py
        # Project root is ../../ compared to this file
        base_path = Path(__file__).resolve().parent.parent.parent
        
        # However, for simplicity in dev, we often run from root. 
        # But let's fallback to current working directory if relative resolution looks weird?
        # Actually, best practice: 
        # If relative_path is "schema.sql" (in root), and we are in dev:
        # base_path should be root.
        pass

    if hasattr(sys, '_MEIPASS'):
        return Path(sys._MEIPASS) / relative_path
    else:
        # In development, return path relative to CWD (assuming run from root)
        # OR relative to file location? 
        # The user seems to run "uvicorn app.main:app" from root.
        # So a simple Path(relative_path).resolve() usually works if CWD is correct.
        # But to be safer, we can try to anchor it.
        # Let's stick to CWD for dev as it's standard for "schema.sql" being in root.
        return Path(relative_path).resolve()
