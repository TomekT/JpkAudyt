import os
import shutil
import subprocess
import sys

def build():
    print("JPK Audytor AI - Rozpoczynanie procesu budowy...")

    # 1. Czyszczenie poprzednich kompilacji
    folders_to_clean = ['build', 'dist']
    for folder in folders_to_clean:
        if os.path.exists(folder):
            print(f"Usuwanie folderu: {folder}...")
            shutil.rmtree(folder)

    # 2. Definiowanie danych dodatkowych (--add-data)
    # Format na Windows: "źródło;miejsce_docelowe"
    data_files = [
        ('app/templates', 'app/templates'),
        ('app/static', 'app/static'),
        ('schema.sql', '.'),
        ('insert_slownik.sql', '.'),
    ]

    # Dodaj pliki konfiguracyjne jeśli istnieją
    for config_file in ['config.json', 'configAI.json']:
        if os.path.exists(config_file):
            data_files.append((config_file, '.'))

    # Budowanie argumentów --add-data
    add_data_args = []
    for src, dst in data_files:
        add_data_args.extend(['--add-data', f"{src};{dst}"])

    # 3. Konfiguracja PyInstallera
    # OPCJE BUDOWANIA:
    # A) Wersja z konsolą (do testów z automatycznym ukrywaniem okna):
    #    '--console'
    # B) Wersja okienkowa (produkcyjna, bez okna konsoli w tle):
    #    '--windowed' (lub '--noconsole')
    
    cmd = [
        'pyinstaller',
        '--noconfirm',
        '--console',  # Domyślnie używamy konsoli, launcher ją ukryje po starcie
        '--name', 'JpkAudytor',
        '--icon', 'app/static/favicon.ico',
        '--splash', 'splash.png',
        '--hidden-import=app.main',
        '--collect-submodules=app',
        *add_data_args,
        'run.py'
    ]

    print(f"Uruchamianie komendy: {' '.join(cmd)}")
    
    try:
        subprocess.run(cmd, check=True)
        print("\n" + "="*50)
        print("BUDOWA ZAKOŃCZONA SUKCESEM!")
        print(f"Pliki aplikacji znajdują się w: {os.path.abspath('dist/JpkAudytor')}")
        print("="*50)
    except subprocess.CalledProcessError as e:
        print(f"\nBłąd podczas budowania: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print("\nBłąd: PyInstaller nie jest zainstalowany. Zainstaluj go komendą: pip install pyinstaller")
        sys.exit(1)

if __name__ == "__main__":
    build()
