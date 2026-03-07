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
