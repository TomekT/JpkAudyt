import os
import sqlite3
import json
from typing import List, Dict, Any, Optional
from google import genai
from google.genai import types
from app.core.config import GOOGLE_API_KEY
from pathlib import Path
from app.services.chat_service import ChatService

class AgentChat:
    """
    Agent AI oparty na modelu Gemini 2.5 Flash z mechanizmem Function Calling.
    Używa nowoczesnego SDK google-genai i poprawnej konfiguracji typów.
    """
    
    CONFIG_FILE = "configAI.json"
    DEFAULT_SYSTEM_INSTRUCTION = (
        "Jesteś ekspertem ds. analizy JPK (Jednolity Plik Kontrolny) i audytu finansowego. "
        "Twoim zadaniem jest odpowiadanie na pytania użytkownika dotyczące danych w bazie SQLite. "
        "Zawsze odpowiadaj po polsku w sposób profesjonalny i pomocny. "
        "\n\nMASZ DOSTĘP DO NARZĘDZI (Function Calling). STOSUJ SIĘ DO PONIŻSZYCH ZASAD:\n"
        "1. Używaj narzędzia 'get_account_balance' jako pierwszego wyboru do szybkiego pobierania danych o kontach (S_1, S_2) oraz wartości księgowych z bazy ZOiS.\n"
        "2. W tabeli ZOiS m.in. znajdują się kolumny: S_4 (BO Wn), S_5 (BO Ma), S_8 (Obroty Wn), S_9 (Obroty Ma), S_10 (Saldo końcowe Wn), S_11 (Saldo końcowe Ma).\n"
        "3. Narzędzia 'execute_sql' używaj TYLKO W OSTATECZNOŚCI, gdy 'get_account_balance' i inne narzędzia nie wystarczą. Możesz w nim wykonywać WYŁĄCZNIE zapytania typu SELECT.\n"
        "4. Wyniki prezentuj w czytelny sposób, najlepiej używając tabel Markdown.\n"
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
        
        # Load config from file or environment
        self.config = self._load_persistent_config()
        effective_api_key = self.config.get("api_key") or GOOGLE_API_KEY
        
        # Zapisujemy tylko api_key w self.config dla spójności z UI
        self.config["api_key"] = effective_api_key or ""
        
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

    def _load_persistent_config(self) -> Dict[str, str]:
        if os.path.exists(self.CONFIG_FILE):
            try:
                with open(self.CONFIG_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        return {}

    def update_config(self, api_key: str):
        """
        Aktualizuje tylko klucz API. Instrukcja systemowa pozostaje niezmienna.
        """
        config_data = {
            "api_key": api_key
        }
        with open(self.CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config_data, f, indent=4, ensure_ascii=False)
        
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
