import sqlite3
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from google import genai
from google.genai import types

# Zależności projektowe
from app.core.config import config_ai_manager
from app.services.chat_service import ChatService

logger = logging.getLogger(__name__)

class AgentChat:
    """
    Agent AI wspierający audyt finansowy JPK-KR.
    Zaimplementowany całkowicie natywnie w oparciu o Google GenAI SDK.
    Wykorzystuje Function Calling do precyzyjnego odpytywania bazy SQLite.
    """
    
    SYSTEM_INSTRUCTION = (
        "Jesteś 'Audit Intelligence' – wysokiej klasy ekspertem ds. audytu finansowego i analizy JPK-KR. "
        "Twoim celem jest wsparcie biegłego rewidenta w weryfikacji ksiąg rachunkowych poprzez precyzyjne operacje na bazie danych SQLite.\n\n"
        
        "STRATEGIA WYBORU NARZĘDZI (ZASADA AGGREGATION-FIRST):\n"
        "1. Do pytań o sumy, wartości łączne, liczebność operacji lub statystyki ZAWSZE używaj narzędzia 'get_accounting_summary'. "
        "Nigdy nie sumuj danych samodzielnie na podstawie listy rekordów.\n"
        "2. Do analizy sald, obrotów kont i bilansu otwarcia używaj 'get_account_balance'.\n"
        "3. Narzędzie 'search_accounting_entries' stosuj WYŁĄCZNIE, gdy użytkownik potrzebuje listy konkretnych dokumentów, szczegółów operacji lub przykładów. "
        "Automatycznie sortuje ono wyniki od największej kwoty. Jeśli limit nie został określony przez użytkownika, ustaw 'limit' na 5 lub 10.\n"
        "4. 'execute_sql' to ostateczność dla niestandardowych zapytań (dopuszczalne tylko operacje SELECT).\n\n"
        
        "WYTYCZNE:\n"
        "- Zawsze odpowiadaj po polsku w profesjonalnym, technicznym tonie.\n"
        "- Analizuj wyniki pod kątem ryzyka audytorskiego (np. nietypowe kwoty, luki w numeracji).\n"
        "- Dane liczbowe formatuj czytelnie (np. 1 234,56 zł) i prezentuj w tabelach Markdown.\n"
        "- Jeśli otrzymasz błąd SQL (error), przeanalizuj zapytanie, popraw błąd składni i wywołaj narzędzie ponownie."
    )

    def __init__(self, db_path: str, model_name: str = "gemini-2.0-flash"):
        self.db_path = db_path
        self.model_name = model_name
        self.client: Optional[genai.Client] = None
        self.chat = None
        self.chat_service = ChatService(self.db_path)
        
        # Compatibility with main.py which might expect self.config or self.model_name
        ai_config = config_ai_manager.get_config()
        self.config = {"api_key": ai_config.api_key or ""}
        
        self._initialize_client()

    def _initialize_client(self) -> None:
        """Inicjalizuje klienta GenAI i konfiguruje natywną sesję czatu z narzędziami."""
        ai_config = config_ai_manager.get_config()
        api_key = ai_config.api_key
        
        if not api_key:
            logger.warning("Brak klucza API. Inicjalizacja Agenta AI wstrzymana.")
            return

        try:
            self.client = genai.Client(api_key=api_key)
            schema_content = self._load_schema()
            
            # Łączenie instrukcji systemowej ze schematem bazy danych
            full_instruction = f"{self.SYSTEM_INSTRUCTION}\n\nSCHEMAT BAZY DANYCH SQL:\n{schema_content}"

            # Natywna konfiguracja GenerateContentConfig
            config = types.GenerateContentConfig(
                system_instruction=full_instruction,
                tools=[
                    self.chat_service.get_account_balance, 
                    self.chat_service.get_accounting_summary,
                    self.chat_service.search_accounting_entries,
                    self.execute_sql
                ],
                automatic_function_calling=types.AutomaticFunctionCallingConfig(
                    disable=False
                )
            )

            # Inicjalizacja natywnej sesji czatu
            self.chat = self.client.chats.create(
                model=self.model_name,
                config=config
            )
            logger.info(f"Agent AI ({self.model_name}) został pomyślnie zainicjalizowany.")
            
        except Exception as e:
            logger.error(f"Błąd krytyczny podczas inicjalizacji klienta Gemini: {e}")
            self.client = None
            self.chat = None

    def update_config(self, api_key: str):
        """
        Aktualizuje klucz API i reinicjalizuje klienta.
        """
        config_ai_manager.update_config({"api_key": api_key})
        self.config["api_key"] = api_key
        self._initialize_client()

    def _load_schema(self) -> str:
        """Pobiera schemat bazy danych jako kontekst dla LLM."""
        schema_path = Path("schema.sql")
        try:
            if schema_path.exists():
                return schema_path.read_text(encoding="utf-8")
        except Exception as e:
            logger.error(f"Błąd odczytu schema.sql: {e}")
        return "-- Schemat bazy danych niedostępny."

    def execute_sql(self, sql_query: str) -> List[Dict[str, Any]]:
        """
        Narzędzie awaryjne do wykonywania bezpośrednich zapytań SQL SELECT na bazie JPK.
        Używaj tego narzędzia tylko wtedy, gdy pozostałe dedykowane funkcje są niewystarczające.
        
        Args:
            sql_query: Precyzyjne zapytanie SQL (wymagany jest wyłącznie SELECT).
            
        Returns:
            Lista słowników reprezentujących wiersze wyników lub słownik z polem 'error' w przypadku błędu.
        """
        logger.info(f"AI Tool Wywołanie: execute_sql | Query: {sql_query}")
        
        if not sql_query.strip().lower().startswith("select"):
            return [{"error": "Odmowa dostępu: Dozwolone są wyłącznie operacje odczytu (SELECT)."}]
            
        if not self.db_path or not Path(self.db_path).exists():
            return [{"error": "Błąd systemu: Baza danych jest niedostępna."}]

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(sql_query)
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
                
        except sqlite3.Error as e:
            # Kluczowe: Zwracamy błąd do LLM, aby mógł samodzielnie poprawić zapytanie
            return [{"error": f"Błąd składni/wykonania SQLite: {e}"}]
        except Exception as e:
            return [{"error": f"Nieoczekiwany błąd środowiska: {e}"}]

    def ask(self, message: str) -> str:
        """
        Interfejs wymiany wiadomości z użytkownikiem. Natywne wywołanie Google GenAI.
        
        Args:
            message: Zapytanie od użytkownika.
            
        Returns:
            Czysty tekst odpowiedzi (str).
        """
        if not self.client or not self.chat:
            return "Błąd: Agent AI nie jest aktywny. Skonfiguruj poprawny klucz API w ustawieniach."

        try:
            # Natywne wywołanie SDK, funkcja automatycznie zarządza ewentualnym Function Callingiem
            response = self.chat.send_message(message)
            return response.text
            
        except genai.errors.APIError as e:
            if e.code == 429:
                return "Osiągnięto limit odpytań do API (Quota Exceeded). Odczekaj chwilę i spróbuj ponownie."
            return f"Błąd komunikacji z API Google: {e.message}"
        except Exception as e:
            return f"Wystąpił nieoczekiwany błąd przetwarzania żądania: {e}"
