import json
import logging
from typing import Dict, Any
from pathlib import Path

from google import genai
from google.genai import types
from google.genai.errors import APIError

from app.core.config import GOOGLE_API_KEY
from app.services.agent_chat import AgentChat

logger = logging.getLogger(__name__)

class AIConnectionError(Exception):
    """Wyjątek rzucany w przypadku problemów z połączeniem lub konfiguracją AI."""
    pass

class ZoisNamingAgent:
    def __init__(self, model_name: str = "gemini-2.5-flash-lite"):
        self.model_name = model_name
        self._client = None
        self._init_client()

    def _init_client(self):
        # Próba odczytu klucza z konfiguracji UI lub pliku .env
        temp_agent = AgentChat(db_path="", model_name=self.model_name)
        effective_api_key = temp_agent.config.get("api_key") or GOOGLE_API_KEY
        
        if not effective_api_key:
            raise AIConnectionError("Brak klucza API (API_KEY)")
            
        try:
            self._client = genai.Client(api_key=effective_api_key)
        except Exception as e:
            raise AIConnectionError(f"Błąd inicjalizacji klienta: {e}")

    def _load_knowledge(self) -> str:
        """
        Wczytuje treść pliku wiedzy z planem kont.
        Bezpiecznie odczytuje plik resources/knowledge/wzorcowy_plan_kont.md przy użyciu pathlib.
        """
        # Próba odnalezienia pliku relatywnie do korzenia projektu
        # Plik znajduje się w app/resources/knowledge/wzorcowy_plan_kont.md
        current_file = Path(__file__).resolve()
        # agent_group_zois.py jest w app/services/, więc resources jest w parent.parent / "resources"
        potential_path = current_file.parent.parent / "resources" / "knowledge" / "wzorcowy_plan_kont.md"
        
        try:
            if potential_path.exists():
                return potential_path.read_text(encoding="utf-8")
            else:
                logger.warning(f"Plik wiedzy nie został znaleziony: {potential_path}")
                return ""
        except Exception as e:
            logger.error(f"Błąd podczas wczytywania pliku wiedzy: {e}")
            return ""

    def generate_names(self, groups_data: list) -> Dict[str, str]:
        """
        Pobiera z serwera AI propozycje nazw dla list grup analitycznych.
        Słownik wejściowy musi zawierać 'numer_grupy', 'saldo', 'analityka'.
        Działa z użyciem formatu JSON.
        """
        if not groups_data:
            return {}
            
        if not self._client:
            raise AIConnectionError("Klient AI nie jest zainicjalizowany")

        # Wczytanie wiedzy pomocniczej
        knowledge = self._load_knowledge()

        system_prompt = (
            "Jesteś ekspertem od planów kont w polskiej rachunkowości dla spółek handlowych i produkcyjnych. Otrzymasz JSON z listą grup kont. Twoim zadaniem jest zaproponowanie profesjonalnej "
            "nazwy dla każdej grupy (syntetyki). Sugeruj się wzorcowymi planami kont i nazwami kont analitycznych. "
            "Zwróć wyłącznie płaski obiekt JSON, gdzie kluczem jest numer grupy (string), a wartością nowa nazwa (string). "
            "Przykład: {'100': 'Kasa', '130': 'Rachunek bieżący'}."
        )

        if knowledge:
            system_prompt += (
                f"\n\nWIEDZA:\n{knowledge}\n\n"
                "Instrukcja: W przypadku wątpliwości przy interpretacji opisów kont analitycznych, odnieś się do sekcji WIEDZA. "
                "Traktuj pierwsze dwie cyfry numeru konta jako klucz do identyfikacji poprawnej nazwy grupy zgodnie ze wzorcem."
            )

        try:
            config = types.GenerateContentConfig(
                system_instruction=system_prompt,
                response_mime_type="application/json"
            )
            
            # Zastosowanie batchingu
            prompt = json.dumps(groups_data, ensure_ascii=False)
            response = self._client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=config
            )
            
            # Sprawdzenie pustej lub błędnej odpowiedzi
            if not response or not response.text:
                raise AIConnectionError("AI zwróciło pustą odpowiedź")
                
            result = json.loads(response.text)
            return result
        except APIError as e:
            logger.error(f"AI API Error: {e}")
            raise AIConnectionError(f"Błąd sieci API: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {response.text}")
            raise AIConnectionError("Błąd przetwarzania odpowiedzi od AI")
        except Exception as e:
            logger.error(f"Unexpected AI Error: {e}")
            raise AIConnectionError(f"Nieoczekiwany błąd AI: {e}")
