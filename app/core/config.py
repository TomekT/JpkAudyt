import os
import json
from pathlib import Path
from typing import Optional, List
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

DEFAULT_PROMPTS = {
    "Produkcyjna": "Rola: Działaj jako doświadczony biegły rewident (senior manager/partner) przeprowadzający badanie sprawozdania finansowego polskiej firmy produkcyjnej. Twoim zadaniem jest przeprowadzenie analitycznych procedur przeglądowych na dostarczonym Zestawieniu Obrotów i Sald (ZOiS) w celu identyfikacji ryzyk istotnego zniekształcenia.\n\nKontekst:\n- Podmiot: Polska spółka produkcyjna.\n- Dane: Plik CSV zawiera obroty i salda (BO, obroty Wn/Ma, Saldo Końcowe).\n- Waluta: PLN.\n\nZadania analityczne:\n1. Analiza spójności logicznej: Sprawdź, czy salda wykazują typowe dla danej grupy kont znaki (np. czy nie występują nienaturalne salda kredytowe na należnościach lub debetowe na zobowiązaniach bez widocznych zaliczek).\n2. Identyfikacja ryzyk specyficznych dla produkcji:\n   - Analizuj strukturę kosztów wytworzenia i zapasów (materiały, półprodukty, wyroby gotowe) do kosztów rodzajowych i przychodów.\n   - Zwróć uwagę na konta odchyleń od cen ewidencyjnych i rozliczenia kosztów (zespół 5) pod kątem potencjalnych błędów w wycenie produkcji.\n3. Wychwycenie anomalii w obrotach: Zidentyfikuj konta o nienaturalnie wysokiej dynamice lub nietypowych relacjach (np. wysokie koszty remontów przy niskich środkach trwałych, brak kosztów energii przy wysokiej produkcji itp.).\n4. Weryfikacja podatkowa i rezerwy: Sprawdź kompletność ujęcia rezerw, rozliczeń międzyokresowych oraz poprawność sald z tytułu podatków i ZUS.\n5. Analiza \"Cut-off\" i \"Window Dressing\": Poszukaj śladów sugerujących manipulację wynikiem na koniec okresu (np. nietypowe salda na kontach przejściowych).\n\nOczekiwany format raportu: Podziel odpowiedź na sekcje:\n- Kluczowe ryzyka: (Tabela z kolumnami: Konto, Opis ryzyka, Sugerowana procedura badania).\n- Anomalie kwotowe: Lista pozycji wymagających natychmiastowego wyjaśnienia przez Zarząd.\n- Wskaźniki: Krótka analiza podstawowych wskaźników (rentowność sprzedaży, rotacja zapasów, płynność), jeśli dane na to pozwalają.\n\nUwaga: Jeśli dane w CSV są nieczytelne lub brakuje nazw kont, poproś o doprecyzowanie przed rozpoczęciem analizy.",
    "Handlowa": "Rola: Działaj jako doświadczony biegły rewident (senior manager/partner) przeprowadzający badanie sprawozdania finansowego polskiej firmy handlowej. Twoim zadaniem jest przeprowadzenie analitycznych procedur przeglądowych na dostarczonym Zestawieniu Obrotów i Sald (ZOiS) w celu identyfikacji ryzyk istotnego zniekształcenia.\n\nKontekst:\n- Podmiot: Polska spółka handlowa.\n- Dane: Plik CSV zawiera obroty i salda (BO, obroty Wn/Ma, Saldo Końcowe).\n- Waluta: PLN.\n\nZadania analityczne:\n1. Analiza spójności logicznej: Sprawdź, czy salda wykazują typowe dla danej grupy kont znaki (np. czy nie występują nienaturalne salda kredytowe na należnościach lub debetowe na zobowiązaniach bez widocznych zaliczek).\n2. Identyfikacja ryzyk specyficznych dla handlu:\n   - Skup się na marży brutto i rotacji towarów.\n3. Wychwycenie anomalii w obrotach: Zidentyfikuj konta o nienaturalnie wysokiej dynamice lub nietypowych relacjach (np. wysokie koszty remontów przy niskich środkach trwałych, brak kosztów energii itp.).\n4. Weryfikacja podatkowa i rezerwy: Sprawdź kompletność ujęcia rezerw, rozliczeń międzyokresowych oraz poprawność sald z tytułu podatków i ZUS.\n5. Analiza \"Cut-off\" i \"Window Dressing\": Poszukaj śladów sugerujących manipulację wynikiem na koniec okresu.\n\nOczekiwany format raportu: Podziel odpowiedź na sekcje:\n- Kluczowe ryzyka: (Tabela z kolumnami: Konto, Opis ryzyka, Sugerowana procedura badania).\n- Anomalie kwotowe: Lista pozycji wymagających natychmiastowego wyjaśnienia przez Zarząd.\n- Wskaźniki: Krótka analiza podstawowych wskaźników (rentowność sprzedaży, rotacja zapasów, płynność), jeśli dane na to pozwalają.",
    "Usługowa": "Rola: Działaj jako doświadczony biegły rewident (senior manager/partner) przeprowadzający badanie sprawozdania finansowego polskiej firmy usługowej. Twoim zadaniem jest przeprowadzenie analitycznych procedur przeglądowych na dostarczonym Zestawieniu Obrotów i Sald (ZOiS) w celu identyfikacji ryzyk istotnego zniekształcenia.\n\nKontekst:\n- Podmiot: Polska spółka usługowa.\n- Dane: Plik CSV zawiera obroty i salda (BO, obroty Wn/Ma, Saldo Końcowe).\n- Waluta: PLN.\n\nZadania analityczne:\n1. Analiza spójności logicznej: Sprawdź, czy salda wykazują typowe dla danej grupy kont znaki (np. czy nie występują nienaturalne salda kredytowe na należnościach lub debetowe na zobowiązaniach bez widocznych zaliczek).\n2. Identyfikacja ryzyk specyficznych dla usług:\n   - Analizuj strukturę kosztów operacyjnych i rentowności sprzedaży usług.\n3. Wychwycenie anomalii w obrotach: Zidentyfikuj konta o nienaturalnie wysokiej dynamice.\n4. Weryfikacja podatkowa i rezerwy: Sprawdź kompletność ujęcia rezerw, rozliczeń międzyokresowych oraz poprawność sald z tytułu podatków i ZUS.\n5. Analiza \"Cut-off\" i \"Window Dressing\": Poszukaj śladów sugerujących manipulację wynikiem na koniec okresu.\n\nOczekiwany format raportu: Podziel odpowiedź na sekcje:\n- Kluczowe ryzyka: (Tabela z kolumnami: Konto, Opis ryzyka, Sugerowana procedura badania).\n- Anomalie kwotowe: Lista pozycji wymagających natychmiastowego wyjaśnienia przez Zarząd.\n- Wskaźniki: Krótka analiza podstawowych wskaźników (rentowność sprzedaży, płynność), jeśli dane na to pozwalają."
}

class AiConfig(BaseModel):
    company_type: str = "Produkcyjna"
    multiplier: float = 1.90
    deviation: float = 5.00
    threshold: float = 0.50
    normalization_100: bool = True
    prompts: dict = DEFAULT_PROMPTS

class AppConfig(BaseModel):
    last_opened_db: Optional[str] = None
    recent_dbs: List[str] = []
    theme: str = "light"
    heartbeat_interval: int = 5
    server_timeout: int = 30

class ConfigAIManager:
    config_file: Path

    def __init__(self, config_filename: str = "configAI.json"):
        self.config_file = Path(config_filename)
        self.config = self._load_config()

    def _load_config(self) -> AiConfig:
        if not self.config_file.exists():
            return AiConfig()
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return AiConfig(**data)
        except Exception:
            return AiConfig()

    def save_config(self):
        with open(self.config_file, 'w', encoding='utf-8') as f:
            f.write(self.config.model_dump_json(indent=4))

    def get_config(self) -> AiConfig:
        return self.config
    
    def update_config(self, ai_data: dict):
        # Update specific fields
        if "company_type" in ai_data:
            self.config.company_type = ai_data["company_type"]
        if "multiplier" in ai_data:
            self.config.multiplier = float(ai_data["multiplier"])
        if "deviation" in ai_data:
            self.config.deviation = float(ai_data["deviation"])
        if "threshold" in ai_data:
            self.config.threshold = float(ai_data["threshold"])
        if "normalization_100" in ai_data:
            val = ai_data["normalization_100"]
            if isinstance(val, str):
                self.config.normalization_100 = val.lower() in ["true", "on", "yes", "1"]
            else:
                self.config.normalization_100 = bool(val)
        if "prompts" in ai_data:
            self.config.prompts.update(ai_data["prompts"])
        
        self.save_config()

    def get_prompt_for_type(self, company_type: str = None) -> str:
        if not company_type:
            company_type = self.config.company_type
        return self.config.prompts.get(company_type, self.config.prompts.get("Produkcyjna", ""))

class ConfigManager:
    config_file: Path

    def __init__(self, config_filename: str = "config.json"):
        # Use user's home directory for config storage to persist across sessions
        # or local directory if portability is preferred. 
        # User requested "local folder user", interpreting as app execution dir or user home.
        # Let's simple store it in the app root for now, or user home.
        # Request says: "config.json w lokalnym folderze użytkownika".
        # Let's use the current working directory for simplicity in this portable-like app 
        # unless 'user folder' strictly means C:\Users\User.
        # Given "lokalna aplikacja Windows", usually means AppData or Documents. 
        # Re-reading: "config.json w lokalnym folderze użytkownika". 
        # I'll put it in the root of the workspace for now.
        self.config_file = Path(config_filename)
        self.config = self._load_config()

    def _load_config(self) -> AppConfig:
        if not self.config_file.exists():
            return AppConfig()
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return AppConfig(**data)
        except Exception:
            return AppConfig()

    def update_system_config(self, heartbeat_interval: int, server_timeout: int):
        self.config.heartbeat_interval = heartbeat_interval
        self.config.server_timeout = server_timeout
        self.save_config()

    def save_config(self):
        with open(self.config_file, 'w', encoding='utf-8') as f:
            f.write(self.config.model_dump_json(indent=4))

    def set_last_db(self, db_path: str):
        if not db_path:
            self.config.last_opened_db = None
        else:
            try:
                p = Path(db_path).resolve()
                self.config.last_opened_db = str(p).replace("\\", "/")
            except Exception:
                self.config.last_opened_db = db_path.replace("\\", "/")
        self.add_to_recent(db_path)
        self.save_config()

    def add_to_recent(self, db_path: str):
        if not db_path:
            return
            
        # Try to get absolute resolved path to deduplicate effectively
        try:
            p = Path(db_path).resolve()
            norm_path = str(p).replace("\\", "/")
        except Exception:
            norm_path = db_path.replace("\\", "/")
            
        # Case-insensitive comparison for Windows
        norm_path_lower = norm_path.lower()
        
        # Remove if already exists (checking both resolved string and lowercased)
        self.config.recent_dbs = [
            d for d in self.config.recent_dbs 
            if d.replace("\\", "/").lower() != norm_path_lower
        ]
        
        # Add to front
        self.config.recent_dbs.insert(0, norm_path)
        # Limit to 10
        self.config.recent_dbs = self.config.recent_dbs[:10]

    def get_last_db(self) -> Optional[str]:
        return self.config.last_opened_db

    def get_recent_dbs(self) -> List[str]:
        # Filter non-existent files and remove duplicates after normalization
        seen_lower = set()
        clean_list = []
        original_count = len(self.config.recent_dbs)
        
        for d in self.config.recent_dbs:
            try:
                # Try to resolve to catch duplicates like relative vs absolute
                p = Path(d).resolve()
                if p.exists():
                    res_path = str(p).replace("\\", "/")
                    if res_path.lower() not in seen_lower:
                        clean_list.append(res_path)
                        seen_lower.add(res_path.lower())
                # If file doesn't exist, we don't add it to clean_list
            except Exception:
                # Fallback if resolve fails
                if Path(d).exists():
                    norm = d.replace("\\", "/")
                    if norm.lower() not in seen_lower:
                        clean_list.append(norm)
                        seen_lower.add(norm.lower())
        
        if len(clean_list) != original_count:
            self.config.recent_dbs = clean_list
            self.save_config()
            
        return self.config.recent_dbs

    def remove_from_recent(self, db_path: str):
        if not db_path:
            return
        try:
            p = Path(db_path).resolve()
            norm_path = str(p).replace("\\", "/")
        except Exception:
            norm_path = db_path.replace("\\", "/")
            
        norm_path_lower = norm_path.lower()
        
        self.config.recent_dbs = [
            d for d in self.config.recent_dbs 
            if d.replace("\\", "/").lower() != norm_path_lower
        ]
        
        if self.config.last_opened_db:
            if self.config.last_opened_db.replace("\\", "/").lower() == norm_path_lower:
                self.config.last_opened_db = None
        self.save_config()



config_manager = ConfigManager()
config_ai_manager = ConfigAIManager()
