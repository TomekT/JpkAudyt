# JPK Audyt - FastAPI Application

Lokalna aplikacja webowa zbudowana w FastAPI do analizy plików JPK_KR (Księgi Rachunkowe). 

## Główne Funkcje
- **Import JPK_KR**: Pełne przetwarzanie plików XML do lokalnej bazy SQLite.
- **ZOiS (Zestawienie Obrotów i Sald)**: Hierarchiczna prezentacja planu kont z możliwością filtrowania.
- **Analiza Zapisów**: Szybki przegląd księgowań z inteligentnym wyszukiwaniem.
- **Integracja AI**: Moduł do automatycznej analizy ryzyk i anomalii na podstawie danych księgowych.
- **Wykresy**: Wizualizacja dynamiki obrotów na kontach.

## Architektura
- **Backend**: FastAPI (Python 3.10+)
- **Frontend**: HTML5, Vanilla CSS, TailwindCSS, DaisyUI, HTMX
- **Baza danych**: SQLite3
- **Pakowanie**: PyInstaller (kompilacja do samodzielnego pliku .exe)

## Szybki Start (Development)
1. Zainstaluj zależności:
   ```bash
   pip install -r requirements.txt
   ```
2. Uruchom serwer:
   ```bash
   python run.py
   ```
3. Aplikacja otworzy się automatycznie w domyślnej przeglądarce.

## Struktura Projektu
- `app/`: Kod źródłowy aplikacji FastAPI.
- `schema.sql`: Definicja schematu bazy danych.
- `insert_slownik.sql`: Dane słownikowe (kategorie kont).
- `build.py`: Skrypt budujący wersję produkcyjną.

## Dystrybucja dla Windows (bez Pythona)

Aplikację można skompilować do samodzielnego folderu, który nie wymaga zainstalowanego środowiska Python na komputerze docelowym. Proces ten tworzy w folderze `dist/JpkAudytor` kompletną paczkę plików, którą można skompresować (ZIP) i przesłać do użytkownika końcowego.

### Kompilacja przy użyciu PyInstaller

Upewnij się, że masz zainstalowany PyInstaller (`pip install pyinstaller`), a następnie wykonaj poniższą komendę w głównym katalogu projektu:

```bash
pyinstaller --noconfirm --onedir --windowed --name "JpkAudytor" --add-data "app/templates;app/templates" --add-data "app/static;app/static" --add-data "schema.sql;." --add-data "insert_slownik.sql;." --icon "app/static/favicon.ico" run.py
```

### Użytkowanie wersji binarnej

1. Po zakończeniu budowania przejdź do folderu `dist/JpkAudytor`.
2. Uruchom plik `JpkAudytor.exe`.
3. Aplikacja uruchomi serwer lokalny w tle i otworzy interfejs w Twojej domyślnej przeglądarce.

