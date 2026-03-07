import sqlite3
from typing import Optional, List, Any, Dict, AsyncIterator
from contextlib import asynccontextmanager

from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseService:
    def __init__(self):
        self.current_db_path: Optional[Path] = None
        self._connection: Optional[sqlite3.Connection] = None

    def connect(self, db_path: str):
        """Connects to a specific SQLite database file."""
        path = Path(db_path)
        if not path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        if self._connection:
            self._connection.close()
        
        self.current_db_path = path
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        logger.info(f"Connected to database: {path}")

    from contextlib import contextmanager

    @contextmanager
    def transaction(self):
        """Context manager for database transactions."""
        conn = self.get_connection()
        try:
            with conn:
                yield conn
        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            raise e


    def get_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("No active database connection")
        return self._connection

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Database connection closed")

    def init_db(self, db_path: str, schema_path: str = "schema.sql"):
        """Creates a new database file and applies the schema."""
        if Path(db_path).exists():
            logger.warning(f"Database already exists at {db_path}. Using existing.")
             # Optionally we could choose to overwrite or just connect.
             # For safety, let's just connect if it exists, or maybe raise error?
             # Task says "Tworzy bazę .db". If exists, we probably overwrite or append.
             # Let's assume overwrite if we are importing a JPK? 
             # Or maybe just clear it. For now, let's assume we create fresh if import calls this.
            pass
            return

        # We connect (creating file if not exists)
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_script = f.read()
            
            cursor.executescript(schema_script)
            conn.commit()
            logger.info(f"Initialized new database at {db_path} using schema from {schema_path}")
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            conn.close()
            raise e
        finally:
            conn.close()

    def execute_script_file(self, script_path: str):
        """Executes a SQL script from a file on the current connection."""
        if not self._connection:
             # If no connection, maybe we should connect? 
             # But this uses current connection usually? 
             # Or we want to run it on a specific path?
             # Let's assume we use get_connection()
             pass
             
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            with open(script_path, 'r', encoding='utf-8') as f:
                script = f.read()
            cursor.executescript(script)
            conn.commit()
            logger.info(f"Executed script from {script_path}")
        except Exception as e:
            logger.error(f"Failed to execute script {script_path}: {e}")
            raise e

    def execute_query(self, query: str, params: tuple = ()) -> List[dict]:
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def execute_non_query(self, query: str, params: tuple = ()):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()

    def verify_db(self, db_path: str) -> bool:
        """
        Verifies if the database at db_path is a compatible JPK_KR_PD Audytor database.
        Returns True if valid, raises ValueError with descriptive message if invalid.
        """
        path = Path(db_path)
        if not path.exists():
            raise FileNotFoundError(f"Nie znaleziono pliku bazy: {db_path}")

        # Connect temporarily to verify
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            # Check if Config table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Config'")
            if not cursor.fetchone():
                raise ValueError("To nie jest baza programu JPK_KR_PD Audytor (brak tabeli Config)")

            # Check values
            cursor.execute("SELECT Klucz, Wartosc FROM Config")
            config = {row['Klucz']: row['Wartosc'] for row in cursor.fetchall()}
            
            if config.get('Program') != 'JPK_KR_PD Audytor':
                raise ValueError("Niezgodność programu (Baza nie pochodzi z JPK_KR_PD Audytor)")
            
            if config.get('Wersja') != '1.0':
                raise ValueError(f"Niezgodność wersji programu (Wymagana: 1.0, Znaleziono: {config.get('Wersja', 'brak')})")
            
            return True
        except Exception as e:
            if isinstance(e, ValueError): raise e
            logger.error(f"Error verifying database: {e}")
            return False
        finally:
            conn.close()

    def get_import_consistency_check(self) -> Dict[str, Any]:
        """
        Performs consistency check between Ctrl table and actual data in Dziennik and Zapisy.
        Returns a dictionary with comparison results.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 1. Get Control values
        try:
            cursor.execute("SELECT C_1, C_2, C_3, C_4, C_5 FROM Ctrl LIMIT 1")
            ctrl = cursor.fetchone()
        except sqlite3.OperationalError:
            raise ValueError("Brak tabeli kontrolnej (Ctrl) w bazie danych.")

        if not ctrl:
            raise ValueError("Brak danych w tabeli kontrolnej (Ctrl).")
        
        # 2. Get Form type (KR vs KR_PD)
        try:
            cursor.execute("SELECT KodFormularza FROM Naglowek LIMIT 1")
            header = cursor.fetchone()
            kod_formularza = header['KodFormularza'] if header else ""
        except sqlite3.OperationalError:
            kod_formularza = ""
            
        is_kr_pd = "KR_PD" in (kod_formularza or "").upper()
        
        # 3. Get Actual values
        # Dziennik
        cursor.execute("SELECT COUNT(*) as count, ROUND(SUM(D_11), 2) as total FROM Dziennik")
        dziennik_stats = cursor.fetchone()
        
        # Zapisy
        cursor.execute("SELECT COUNT(*) as count, ROUND(SUM(Z_4), 2) as sum_wn, ROUND(SUM(Z_7), 2) as sum_ma FROM Zapisy")
        zapisy_stats = cursor.fetchone()
        
        # Technical Integrity Checks
        cursor.execute("SELECT COUNT(*) FROM Zapisy WHERE Z_3 IS NULL OR Z_3 = ''")
        null_z3 = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM Zapisy WHERE Dziennik_Id IS NULL")
        null_dziennik = cursor.fetchone()[0]

        checks_list = [
            {
                "label": "Liczba wpisów w Dzienniku (C_1)",
                "ctrl": ctrl['C_1'],
                "actual": dziennik_stats['count'] or 0,
                "type": "int"
            },
            {
                "label": "Wartość Dziennika (C_2)",
                "ctrl": ctrl['C_2'],
                "actual": dziennik_stats['total'] or 0,
                "type": "decimal"
            }
        ]
        
        # C_3 is mandatory only for JPK_KR_PD
        if is_kr_pd:
            checks_list.append({
                "label": "Liczba zapisów (C_3)",
                "ctrl": ctrl['C_3'],
                "actual": zapisy_stats['count'] or 0,
                "type": "int"
            })
            
        # C_4 and C_5 are mandatory for both JPK_KR and JPK_KR_PD
        checks_list.extend([
            {
                "label": "Suma Winien (C_4)",
                "ctrl": ctrl['C_4'],
                "actual": zapisy_stats['sum_wn'] or 0,
                "type": "decimal"
            },
            {
                "label": "Suma Ma (C_5)",
                "ctrl": ctrl['C_5'],
                "actual": zapisy_stats['sum_ma'] or 0,
                "type": "decimal"
            }
        ])

        results = {
            "is_kr_pd": is_kr_pd,
            "checks": checks_list,
            "integrity": [
                {
                    "id": "z3_null",
                    "label": "Brak identyfikatora zapisu (Z_3)",
                    "count": null_z3,
                    "tip": "Wykryto rekordy w tabeli Zapisy bez wymaganego numeru identyfikacyjnego konta (Z_3)."
                },
                {
                    "id": "dziennik_null",
                    "label": "Zapisy bez powiązania z Dziennikiem",
                    "count": null_dziennik,
                    "tip": "Wykryto rekordy w tabeli Zapisy, które nie są poprawnie przypisane do żadnego nagłówka w Dzienniku."
                }
            ]
        }
        
        return results

db_service = DatabaseService()
