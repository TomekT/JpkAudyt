import os
import sqlite3
import json
from typing import List, Dict, Any, Optional
from google import genai
from google.genai import types
from app.core.config import GOOGLE_API_KEY, config_ai_manager
from pathlib import Path
from app.services.chat_service import ChatService

class AgentChat:
    """
    Agent AI oparty na modelu Gemini 2.5 Flash z mechanizmem Function Calling.
    Używa nowoczesnego SDK google-genai i poprawnej konfiguracji typów.
    """
    
    DEFAULT_SYSTEM_INSTRUCTION = (
    "Jesteś 'Audit Intelligence' – wysokiej klasy ekspertem ds. audytu finansowego i analizy JPK-KR. "
    "Twoim celem jest wsparcie biegłego rewidenta w weryfikacji ksiąg rachunkowych poprzez precyzyjne operacje na bazie danych SQLite.\n\n"
    
    "STRATEGIA WYBORU NARZĘDZI (ZASADA AGGREGATION-FIRST):\n"
    "1. Do pytań o sumy, wartości łączne, liczebność operacji lub statystyki ZAWSZE używaj narzędzia 'get_accounting_summary'. "
    "Nigdy nie sumuj danych samodzielnie na podstawie listy rekordów.\n"
    "2. Do analizy sald, obrotów kont i bilansu otwarcia (tabela ZOiS) używaj 'get_account_balance'.\n"
    "3. Narzędzie 'search_accounting_entries' stosuj WYŁĄCZNIE, gdy użytkownik potrzebuje listy konkretnych dokumentów, szczegółów operacji lub przykładów.To narzędzie automatycznie sortuje wyniki od NAJWYŻSZEJ KWOTY."
    "Jeśli użytkownik pyta o 'najważniejsze', 'największe' lub 'top' pozycje, ustaw parametr 'limit' na żądaną liczbę (np. 5 lub 10).\n"
    "4. Weryfikacja: Przy pytaniach o istotność, najpierw pobierz sumę (summary), a potem 5-10 największych pozycji (entries), aby przedstawić je jako przykłady.\n"
    "5. 'execute_sql' to ostateczność dla zapytań niemożliwych do obsłużenia dedykowanymi metodami. Dopuszczalne tylko operacje SELECT.\n\n"
    
    "WYTYCZNE ANALITYCZNE I AUDYTORSKIE:\n"
    "- Zawsze odpowiadaj po polsku w profesjonalnym, technicznym tonie.\n"
    "- Jeśli limit nie został określony, domyślnie pokazuj 5-10 najważniejszych pozycji.\n"
    "- Interpretuj wyniki pod kątem ryzyka badania (np. nietypowe kwoty, zapisy ręczne, brak ciągłości numeracji).\n"
    "- Wyniki liczbowe prezentuj w tabelach Markdown, dbając o czytelność kwot (np. 1 234,56 zł).\n\n"
    
    "Działaj precyzyjnie. Pamiętaj, że w audycie liczy się kompletność i wiarygodność źródła danych (SQL)."
    )
    
    def __init__(self, db_path: str, model_name: str = "gemini-2.5-flash-lite"):
        """
        Inicjalizacja agenta.
        """
        self.db_path = db_path
        self.model_name = model_name
        self.client = None
        self.chat = None
        self.chat_service = ChatService(self.db_path)
        
        # Load config from centralized manager
        ai_config = config_ai_manager.get_config()
        effective_api_key = ai_config.api_key or GOOGLE_API_KEY
        
        # Save key back to config for consistency if needed, but via manager
        if effective_api_key and not ai_config.api_key:
            config_ai_manager.update_config({"api_key": effective_api_key})
        
        # Compatibility with main.py which expects self.config
        self.config = {"api_key": effective_api_key or ""}
        
        if not effective_api_key:
            return
 
        try:
            self.client = genai.Client(api_key=effective_api_key)
            schema_content = self._load_schema()
            
            # Instrukcja systemowa jest teraz stała i pobierana wyłącznie z klasy constant
            system_instruction = self.DEFAULT_SYSTEM_INSTRUCTION
            
            # Dołączanie schematu bazy danych (zgodnie z wymogiem 4 poprzedniego zadania i zachowując logikę)
            full_instruction = f"{system_instruction}\n6. Schemat bazy danych SQL:\n{schema_content}"
 
            self.chat = self.client.chats.create(
                model=model_name,
                config=types.GenerateContentConfig(
                    tools=[
                        self.chat_service.get_account_balance, 
                        self.chat_service.get_accounting_summary,
                        self.chat_service.search_accounting_entries,
                        self.execute_sql],
                    automatic_function_calling=types.AutomaticFunctionCallingConfig(
                        disable=False
                    ),
                    system_instruction=full_instruction
                )
            )
        except Exception as e:
            print(f"Błąd inicjalizacji klienta Gemini: {e}")
            self.client = None

    def update_config(self, api_key: str):
        """
        Aktualizuje tylko klucz API w centralnym pliku konfiguracyjnym.
        """
        config_ai_manager.update_config({"api_key": api_key})
        
        # Reinicjalizacja z nowym kluczem
        self.__init__(self.db_path, self.model_name)

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
        print(f"[AI-TOOL] Wywołano funkcję execute_sql z zapytaniem: {sql_query}")
        
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
