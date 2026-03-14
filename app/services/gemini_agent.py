import os
import sqlite3
from typing import List, Dict, Any, Optional
from google import genai
from google.genai import types
from app.core.config import GOOGLE_API_KEY
from pathlib import Path

class JpkAgent:
    """
    Agent AI oparty na modelu Gemini 2.5 Flash z mechanizmem Function Calling.
    Używa nowoczesnego SDK google-genai i poprawnej konfiguracji typów.
    """
    
    def __init__(self, db_path: str):
        """
        Inicjalizacja agenta.
        
        Args:
            db_path: Ścieżka do pliku bazy danych SQLite.
        """
        self.db_path = db_path
        self.client = None
        self.chat = None
        
        if not GOOGLE_API_KEY:
            return

        try:
            # Inicjalizacja klienta SDK google-genai
            self.client = genai.Client(api_key=GOOGLE_API_KEY)
            
            # Diagnostyka: Wypisanie dostępnych modeli
            print("--- Diagnostyka AI: Dostępne modele ---")
            try:
                for m in self.client.models.list():
                    print(f"Model ID: {m.name} | Display: {m.display_name}")
            except Exception as diag_err:
                print(f"Nie udało się pobrać listy modeli: {diag_err}")
            print("---------------------------------------")
            
            # Wczytanie schematu bazy danych
            schema_content = self._load_schema()
            
            system_instruction = (
                "Jesteś ekspertem ds. analizy JPK (Jednolity Plik Kontrolny) i audytu finansowego. "
                "Twoim zadaniem jest odpowiadanie na pytania użytkownika dotyczące danych w bazie SQLite. "
                "Zawsze odpowiadaj po polsku w sposób profesjonalny i pomocny. "
                "\n\nMASZ DOSTĘP DO NARZĘDZIA 'execute_sql'. STOSUJ SIĘ DO PONIŻSZYCH ZASAD:\n"
                "1. Możesz wykonywać WYŁĄCZNIE zapytania typu SELECT. Instrukcje modyfikujące (INSERT, UPDATE, DELETE, DROP itp.) są ZAKAZANE.\n"
                "2. Jeśli użytkownik prosi o analizę, spróbuj sformułować odpowiednie zapytanie SQL, aby pobrać dane.\n"
                "3. Wyniki prezentuj w czytelny sposób, najlepiej używając tabel Markdown.\n"
                "4. Jeśli zapytanie SQL zwróci błąd, poinformuj o tym i spróbuj je poprawić.\n"
                "5. Użytkownik najczęściej będzie podawał numer konta sytnetyccznego, ale interesują go wszystkie podrzędne konta analityczne. Uwzględnij przeszukiwanie kont podrzędnych, czyli np. dla konta 400 użyj warunku S_1 LIKE '400%' oraz is_analytical = 1.\n"
                "6. Schemat bazy danych SQL:\n"
                f"{schema_content}"
            )

            # Poprawna inicjalizacja czatu z użyciem modelu gemini-2.0-flash
            self.chat = self.client.chats.create(
                model="gemini-2.5-flash",
                config=types.GenerateContentConfig(
                    tools=[self.execute_sql],
                    automatic_function_calling=types.AutomaticFunctionCallingConfig(
                        disable=False
                    ),
                    system_instruction=system_instruction
                )
            )
        except Exception as e:
            print(f"Błąd inicjalizacji klienta Gemini: {e}")
            self.client = None

    def _load_schema(self) -> str:
        """Wczytuje treść pliku schema.sql."""
        schema_path = Path("schema.sql")
        if schema_path.exists():
            try:
                with open(schema_path, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception as e:
                return f"-- Błąd wczytywania schematu: {e}"
        return "-- Brak pliku schema.sql"

    def execute_sql(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Narzędzie do wykonywania zapytań SQL SELECT na bazie danych JPK.
        
        Args:
            sql_query: Treść zapytania SQL SELECT.
            
        Returns:
            Lista słowników z wynikami zapytania lub błędem.
        """
        # Dodatkowe zabezpieczenie przed zapytaniami innymi niż SELECT
        query_strip = sql_query.strip().lower()
        if not query_strip.startswith("select"):
            return [{"error": "Dozwolone są tylko zapytania typu SELECT."}]
            
        if not self.db_path or not Path(self.db_path).exists():
            return [{"error": "Baza danych nie jest podłączona lub nie istnieje."}]

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql_query)
            rows = cursor.fetchall()
            results = [dict(row) for row in rows]
            conn.close()
            return results
        except sqlite3.Error as e:
            return [{"error": f"Błąd SQLite: {str(e)}"}]
        except Exception as e:
            return [{"error": f"Błąd krytyczny narzędzia SQL: {str(e)}"}]

    def ask(self, message: str) -> str:
        """
        Przesyła wiadomość do agenta i zwraca odpowiedź tekstową.
        
        Args:
            message: Wiadomość od użytkownika.
            
        Returns:
            Odpowiedź tekstowa od modelu AI.
        """
        if not GOOGLE_API_KEY:
            return "Błąd: Brak klucza API (GOOGLE_API_KEY) w pliku .env."
        
        if not self.client or not self.chat:
            return "Błąd: Nie udało się zainicjalizować sesji AI. Sprawdź klucz API i połączenie internetowe."

        try:
            response = self.chat.send_message(message)
            return response.text
        except genai.errors.APIError as e:
            # Obsługa błędu 429 (Limit Quota)
            if e.code == 429:
                return "System jest teraz przeciążony. Spróbuj ponownie za około minutę (Limit Quota API)."
            return f"Błąd usługi AI: {str(e)}"
        except Exception as e:
            return f"Wystąpił nieoczekiwany błąd: {str(e)}"
