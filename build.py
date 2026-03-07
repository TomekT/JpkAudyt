import PyInstaller.__main__
import os
import shutil

def build_exe():
    print("Building JPK Audyt EXE...")
    
    # Define paths
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Clean previous build
    if os.path.exists("build"):
        shutil.rmtree("build")
    if os.path.exists("dist"):
        shutil.rmtree("dist")
    
    # PyInstaller arguments
    args = [
        'launcher.py',                  # Entry point
        '--name=JPKAudyt',              # Executable name
        '--onefile',                    # Single EXE
        '--noconsole',                  # No terminal window
        '--clean',                      # Clean cache
        
        # Data files (source;dest)
        # Windows uses semicolon ; separator for --add-data
        '--add-data=app/templates;app/templates',
        '--add-data=app/static;app/static',
        '--add-data=schema.sql;.',
        '--add-data=insert_slownik.sql;.',
        
        # Hidden imports often missed by PyInstaller analysis
        '--hidden-import=uvicorn',
        '--hidden-import=uvicorn.loops',
        '--hidden-import=uvicorn.loops.auto',
        '--hidden-import=uvicorn.protocols',
        '--hidden-import=uvicorn.protocols.http',
        '--hidden-import=uvicorn.protocols.http.auto',
        '--hidden-import=uvicorn.lifespan.on',
        '--hidden-import=fastapi',
        '--hidden-import=jinja2',
        '--hidden-import=python-multipart',
        '--hidden-import=sqlite3',
        '--hidden-import=lxml',
        '--hidden-import=pandas', # Just in case
        '--hidden-import=email_validator', # Often used by pydantic
    ]
    
    # Icon if exists
    if os.path.exists("app/static/favicon.ico"):
        args.append(f'--icon=app/static/favicon.ico')
    
    print(f"Running PyInstaller with args: {args}")
    
    PyInstaller.__main__.run(args)
    
    print("Build complete. Check 'dist/JPKAudyt.exe'")

if __name__ == "__main__":
    build_exe()
