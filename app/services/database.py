import sqlite3
import re
from typing import Optional, List, Any, Dict, AsyncIterator
from contextlib import asynccontextmanager

from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseService:
    @staticmethod
    def parse_smart_query(q: str):
        if not q:
            return []
        # Split by common separators: LUB (case insensitive), comma, pipe
        parts = re.split(r'\s+LUB\s+|[,|]', q, flags=re.IGNORECASE)
        return [p.strip() for p in parts if p.strip()]

    def build_zois_where(self, q: str = "", type: str = "", label_id: str = "", forced_ids: Optional[List[str]] = None, synthetic: bool = True, empty: bool = True):
        where_clauses = []
        params = {}
        
        # Filtr kont analitycznych (jeśli nie syntetyka)
        if not synthetic:
            where_clauses.append("IsAnalytical = 1")
            
        # Filtr kont niepustych
        if not empty:
            where_clauses.append("(ABS(COALESCE(S_4,0)) + ABS(COALESCE(S_5,0)) + ABS(COALESCE(S_6,0)) + ABS(COALESCE(S_7,0)) + ABS(COALESCE(S_8,0)) + ABS(COALESCE(S_9,0)) + ABS(COALESCE(S_10,0)) + ABS(COALESCE(S_11,0))) > 0")

        if forced_ids:
            placeholders = ", ".join(f":fid_{i}" for i in range(len(forced_ids)))
            where_clauses.append(f"S_1 IN ({placeholders})")
            for i, fid in enumerate(forced_ids):
                params[f"fid_{i}"] = fid
        if q:
            terms = self.parse_smart_query(q)
            if terms:
                term_clauses = []
                for i, term in enumerate(terms):
                    key = f"q{i}"
                    if term.startswith("konto:"):
                        val = term[6:]
                        term_clauses.append(f"(S_1 = :{key} OR S_1 LIKE :{key} || '-%')")
                        params[key] = val
                    else:
                        term_clauses.append(f"(S_1 LIKE :{key} OR S_2 LIKE :{key})")
                        params[key] = f"%{term}%"
                where_clauses.append("(" + " OR ".join(term_clauses) + ")")
        if type:
            where_clauses.append("TypKonta = :type")
            params["type"] = type
        if label_id:
            if label_id == "__NONE__":
                where_clauses.append("S_1 NOT IN (SELECT ZOiS_S1 FROM Etykiety_ZOiS)")
            elif label_id == "__MULTIPLE__":
                where_clauses.append("S_1 IN (SELECT ZOiS_S1 FROM Etykiety_ZOiS GROUP BY ZOiS_S1 HAVING COUNT(*) > 1)")
            else:
                where_clauses.append("S_1 IN (SELECT ZOiS_S1 FROM Etykiety_ZOiS WHERE EtykietaId = :label_id)")
                params["label_id"] = label_id
            
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        return where_sql, params

    def get_zois_data(self, q: str = "", type: str = "", label_id: str = "", forced_ids: Optional[List[str]] = None, synthetic: bool = True, empty: bool = True):
        where_sql, params = self.build_zois_where(q, type, label_id, forced_ids, synthetic, empty)
        sql = f"SELECT * FROM ZOiS {where_sql} ORDER BY S_1"
        return self.get_connection().execute(sql, params).fetchall()

    def build_zapisy_where(self, q: str = "", type: str = "", zq: str = "", month: str = "", label_id: str = "", label_zapisy_id: str = ""):
        where_clauses = []
        params = {}
        if q:
            terms = self.parse_smart_query(q)
            if terms:
                term_clauses = []
                for i, term in enumerate(terms):
                    key = f"q{i}"
                    if term.startswith("konto:"):
                        val = term[6:]
                        term_clauses.append(f"(z.Z_3 = :{key} OR z.Z_3 LIKE :{key} || '-%')")
                        params[key] = val
                    else:
                        term_clauses.append(f"(z.Z_3 LIKE :{key} OR s.S_2 LIKE :{key})")
                        params[key] = f"%{term}%"
                where_clauses.append("(" + " OR ".join(term_clauses) + ")")
        if type:
            where_clauses.append("s.TypKonta = :type")
            params["type"] = type
        if label_id:
            where_clauses.append("z.Z_3 IN (SELECT ZOiS_S1 FROM Etykiety_ZOiS WHERE EtykietaId = :label_id)")
            params["label_id"] = label_id
        if label_zapisy_id:
            where_clauses.append("z.Id IN (SELECT ZapisyId FROM Etykiety_Zapisy WHERE EtykietaId = :label_zapisy_id)")
            params["label_zapisy_id"] = label_zapisy_id
        if zq:
            terms = self.parse_smart_query(zq)
            if terms:
                term_clauses = []
                for i, term in enumerate(terms):
                    key = f"zq{i}"
                    if term.startswith("assoc:"):
                        val = term[6:]
                        # Filtrujemy do zapisów z dzienników, które zawierają dane konto syntetyczne
                        term_clauses.append(f"z.Dziennik_Id IN (SELECT DISTINCT sub_z.Dziennik_Id FROM Zapisy sub_z WHERE sub_z.Z_Syntetyka = :{key})")
                        params[key] = val
                    else:
                        term_clauses.append(f"(z.Z_3 LIKE :{key} OR z.Z_2 LIKE :{key})")
                        params[key] = f"%{term}%"
                where_clauses.append("(" + " OR ".join(term_clauses) + ")")
        if month and month.isdigit():
            where_clauses.append("z.Z_DataMiesiac = :month")
            params["month"] = int(month)
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        return where_sql, params

    def __init__(self):
        self.current_db_path: Optional[Path] = None
        self._connection: Optional[sqlite3.Connection] = None

    def connect(self, db_path: str):
        """Connects to a specific SQLite database file with high-performance READ pragma settings."""
        path = Path(db_path)
        if not path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        
        if self._connection:
            self._connection.close()
        
        # check_same_thread=False is essential for FastAPI/Uvicorn multi-threading
        try:
            path = Path(db_path).resolve()
            logger.info(f"Connecting to database: {path}")
            self._connection = sqlite3.connect(str(path), check_same_thread=False)
            self._connection.row_factory = sqlite3.Row
            self.current_db_path = path
            
            # Use the connection directly here
            self.ensure_tagging_tables()
            logger.info("Database connected and tagging tables ensured.")
        except Exception as e:
            logger.error(f"Failed to connect to database {db_path}: {e}")
            self._connection = None
            raise e

        # Read-heavy optimizations
        cursor = self._connection.cursor()
        cursor.execute("PRAGMA temp_store = MEMORY;")      # Keep temp tables/indexes in RAM
        cursor.execute("PRAGMA mmap_size = 3000000000;")   # Map up to 3GB into memory (OS level)
        cursor.execute("PRAGMA cache_size = -64000;")      # 64MB Application cache
        cursor.execute("PRAGMA synchronous = NORMAL;")     # Faster writes while maintaining safety
        
        logger.info(f"Connected to database: {path} (Absolute: {path.absolute()})")
        
        # Automatyczne dodanie tabel etykiet jeśli ich nie ma (migracja w locie)
        # This call is now inside the try block above.

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
            if self.current_db_path:
                self.connect(str(self.current_db_path))
            
            if self._connection is None:
                raise RuntimeError("No active database connection and could not re-establish one.")
        
        return self._connection

    def close(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except:
                pass
            self._connection = None
            logger.info("Database connection closed")

    def ensure_tagging_tables(self):
        """Upewnia się, że tabele systemu etykietowania istnieją w aktualnej bazie."""
        if not self._connection: 
            logger.error("Migration attempted with no active connection.")
            return
        
        logger.info(f"Running tagging tables migration for: {self.current_db_path}")
        cursor = self._connection.cursor()
        try:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Etykiety (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Nazwa TEXT NOT NULL,
                    Obszar TEXT NOT NULL CHECK (Obszar IN ('ZOiS', 'Zapisy', 'Dziennik')),
                    UNIQUE(Nazwa, Obszar)
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Etykiety_ZOiS (
                    EtykietaId INTEGER NOT NULL,
                    ZOiS_S1 TEXT NOT NULL,
                    PRIMARY KEY (EtykietaId, ZOiS_S1),
                    FOREIGN KEY (EtykietaId) REFERENCES Etykiety (Id) ON DELETE CASCADE,
                    FOREIGN KEY (ZOiS_S1) REFERENCES ZOiS (S_1) ON DELETE CASCADE
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_etykiety_zois_s1 ON Etykiety_ZOiS(ZOiS_S1);")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Etykiety_Dziennik (
                    EtykietaId INTEGER NOT NULL,
                    DziennikId INTEGER NOT NULL,
                    PRIMARY KEY (EtykietaId, DziennikId),
                    FOREIGN KEY (EtykietaId) REFERENCES Etykiety (Id) ON DELETE CASCADE,
                    FOREIGN KEY (DziennikId) REFERENCES Dziennik (Id) ON DELETE CASCADE
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_etykiety_dziennik_id ON Etykiety_Dziennik(DziennikId);")
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS Etykiety_Zapisy (
                    EtykietaId INTEGER NOT NULL,
                    ZapisyId INTEGER NOT NULL,
                    PRIMARY KEY (EtykietaId, ZapisyId),
                    FOREIGN KEY (EtykietaId) REFERENCES Etykiety (Id) ON DELETE CASCADE,
                    FOREIGN KEY (ZapisyId) REFERENCES Zapisy (Id) ON DELETE CASCADE
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_etykiety_zapisy_id ON Etykiety_Zapisy(ZapisyId);")
            self._connection.commit()
        except Exception as e:
            logger.error(f"Error ensuring tagging tables: {e}")

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

    def is_database_populated(self, db_path: str) -> bool:
        """Sprawdza czy baza pod ścieżką db_path zawiera już jakiekolwiek rekordy w kluczowych tabelach."""
        path = Path(db_path)
        if not path.exists():
            return False
            
        conn = sqlite3.connect(path)
        try:
            cursor = conn.cursor()
            # Sprawdzamy kluczowe tabele
            for table in ['ZOiS', 'Dziennik', 'Zapisy']:
                try:
                    cursor.execute(f"SELECT 1 FROM {table} LIMIT 1")
                    if cursor.fetchone():
                        return True
                except sqlite3.OperationalError:
                    # Tabela może nie istnieć
                    continue
            return False
        except Exception:
            return False
        finally:
            conn.close()

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

    def get_trivial_materiality(self) -> float:
        """Pobiera wartość Istotnosc_Trywialna z tabeli ParametryBadania."""
        try:
            conn = self.get_connection()
            row = conn.execute("SELECT Kwota FROM ParametryBadania WHERE Klucz = 'Istotnosc_Trywialna'").fetchone()
            if row and row['Kwota'] is not None:
                return float(row['Kwota'])
        except Exception:
            pass
        return 0.0

    def get_zscore_anomaly_ids(self, record_ids: list, min_amount: float) -> list:
        """
        Wykrywa anomalie statystyczne (Z-Score) z uwzględnieniem progu istotności.
        - Robust Z-Score (Mediana, MAD, K=0.6745)
        - Filtracja: |Z_Score| > 3.0 ORAZ max(Wn, Ma) >= min_amount
        """
        if not record_ids or len(record_ids) < 3:
            return []
            
        try:
            import pandas as pd
        except ImportError:
            logger.error("Brak biblioteki pandas.")
            return []
            
        placeholders = ','.join('?' for _ in record_ids)
        query = f"SELECT Id, Z_4 as Wn, Z_7 as Ma FROM Zapisy WHERE Id IN ({placeholders})"
        
        conn = self.get_connection()
        df = pd.read_sql_query(query, conn, params=record_ids)
        
        # Obliczamy max(Wn, Ma) dla filtracji końcowej
        df['max_amount'] = df[['Wn', 'Ma']].max(axis=1)
        
        anomaly_ids = set()
        
        for col in ['Wn', 'Ma']:
            # Pomiń wpisy z zerami dla celów obliczania odchyleń statystycznych
            series = df[df[col] != 0][col].dropna()
            
            if len(series) < 3:
                continue
                
            median = series.median()
            mad = (series - median).abs().median()
            
            if mad == 0:
                continue
                
            K = 0.6745
            # Obliczanie Robust Z-Score
            z_scores = (K * (series - median)) / mad
            
            # Identyfikacja anomalii zgodnie z wymaganiami:
            # (|Z_Score| > 3.0) ORAZ (max(Wn, Ma) >= min_amount)
            # Musimy odnieść się do oryginalnego DataFrame, aby sprawdzić max_amount
            for idx in series.index:
                z_score = z_scores[idx]
                max_val = df.loc[idx, 'max_amount']
                
                if abs(z_score) > 3.0 and max_val >= min_amount:
                    anomaly_ids.add(int(df.loc[idx, 'Id']))
                
        return sorted(list(anomaly_ids))

    def detect_zscore_anomalies(self, record_ids: list) -> list:
        """Legacy alias or simple version if needed, redirected to new logic with 0 threshold."""
        return self.get_zscore_anomaly_ids(record_ids, 0.0)

    def get_labels(self, area: str) -> List[str]:
        """Pobiera listę unikalnych etykiet dla danego obszaru."""
        try:
            conn = self.get_connection()
            rows = conn.execute("SELECT Nazwa FROM Etykiety WHERE Obszar = ? ORDER BY Nazwa", (area,)).fetchall()
            return [row['Nazwa'] for row in rows]
        except sqlite3.OperationalError as e:
            if "no such table: Etykiety" in str(e):
                logger.error(f"KRYTYCZNY BŁĄD: Tabela Etykiety nie została znaleziona w połączeniu!")
                # Re-run migration just in case something went wrong with the session
                self.ensure_tagging_tables()
                # Try one more time
                conn = self.get_connection()
                rows = conn.execute("SELECT Nazwa FROM Etykiety WHERE Obszar = ? ORDER BY Nazwa", (area,)).fetchall()
                return [row['Nazwa'] for row in rows]
            raise e
        except Exception as e:
            logger.error(f"Error getting labels for area {area}: {e}")
            return []

    def get_assigned_labels_for_ids(self, area: str, ids: List[Any]) -> List[str]:
        """Pobiera listę etykiet przypisanych do konkretnych rekordów."""
        if not ids: return []
        table_map = {
            'ZOiS': ('Etykiety_ZOiS', 'ZOiS_S1'),
            'Dziennik': ('Etykiety_Dziennik', 'DziennikId'),
            'Zapisy': ('Etykiety_Zapisy', 'ZapisyId')
        }
        tbl, col = table_map[area]
        placeholders = ','.join('?' for _ in ids)
        query = f"""
            SELECT DISTINCT e.Nazwa 
            FROM Etykiety e 
            JOIN {tbl} rel ON e.Id = rel.EtykietaId 
            WHERE rel.{col} IN ({placeholders}) AND e.Obszar = ?
            ORDER BY e.Nazwa
        """
        conn = self.get_connection()
        rows = conn.execute(query, (*ids, area)).fetchall()
        return [row['Nazwa'] for row in rows]

    def get_assigned_labels_by_filter(self, area: str, filters: Dict[str, Any]) -> List[str]:
        """Pobiera etykiety przypisane do rekordów pasujących do filtrów."""
        table_map = {
            'ZOiS': ('Etykiety_ZOiS', 'ZOiS_S1', 'ZOiS', 'S_1'),
            'Dziennik': ('Etykiety_Dziennik', 'DziennikId', 'Zapisy', 'Dziennik_Id'),
            'Zapisy': ('Etykiety_Zapisy', 'ZapisyId', 'Zapisy', 'Id')
        }
        tbl, col, base_tbl, base_col = table_map[area]
        
        q = filters.get("q", "")
        type_ = filters.get("type", "")
        zq = filters.get("zq", "")
        month = filters.get("month", "")
        label_id = filters.get("label_id", "")
        label_zapisy_id = filters.get("label_zapisy_id", "")
        
        if area == 'ZOiS':
            where_sql, params = self.build_zois_where(q, type_, label_id)
            join_sql = ""
        else:
            where_sql, params = self.build_zapisy_where(q, type_, zq, month, label_id, label_zapisy_id)
            join_sql = "LEFT JOIN ZOiS s ON z.Z_3 = s.S_1"
            base_tbl = "Zapisy z"

        query = f"""
            SELECT DISTINCT e.Nazwa 
            FROM Etykiety e 
            JOIN {tbl} m ON e.Id = m.EtykietaId 
            WHERE m.{col} IN (
                SELECT {base_col} FROM {base_tbl} {join_sql} {where_sql}
            ) AND e.Obszar = ?
        """
        params['area_param'] = area
        # Note: params is a dict, we need to pass it safely.
        # SQLite execute with dict params works!
        query = query.replace("?", ":area_param") 
        conn = self.get_connection()
        rows = conn.execute(query, params).fetchall()
        return [row['Nazwa'] for row in rows]

    def bulk_add_labels(self, area: str, ids: List[Any], label_name: str):
        """Masowe dodawanie etykiety do listy ID w jednej transakcji."""
        if not ids or not label_name: return
        with self.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR IGNORE INTO Etykiety (Nazwa, Obszar) VALUES (?, ?)", (label_name, area))
            label_id = conn.execute("SELECT Id FROM Etykiety WHERE Nazwa = ? AND Obszar = ?", (label_name, area)).fetchone()[0]
            
            table_map = {'ZOiS': ('Etykiety_ZOiS', 'ZOiS_S1'), 'Dziennik': ('Etykiety_Dziennik', 'DziennikId'), 'Zapisy': ('Etykiety_Zapisy', 'ZapisyId')}
            tbl, col = table_map[area]
            data = [(label_id, i) for i in ids]
            cursor.executemany(f"INSERT OR IGNORE INTO {tbl} (EtykietaId, {col}) VALUES (?, ?)", data)

    def bulk_add_labels_by_filter(self, area: str, filters: Dict[str, Any], label_name: str) -> int:
        """Dodaje etykietę do rekordów pasujących do filtrów."""
        table_map = {
            'ZOiS': ('Etykiety_ZOiS', 'ZOiS_S1', 'ZOiS', 'S_1'),
            'Dziennik': ('Etykiety_Dziennik', 'DziennikId', 'Zapisy', 'Dziennik_Id'),
            'Zapisy': ('Etykiety_Zapisy', 'ZapisyId', 'Zapisy', 'Id')
        }
        tbl, col, base_tbl, base_col = table_map[area]
        
        q = filters.get("q", "")
        type_ = filters.get("type", "")
        zq = filters.get("zq", "")
        month = filters.get("month", "")
        label_id = filters.get("label_id", "")
        label_zapisy_id = filters.get("label_zapisy_id", "")
        
        if area == 'ZOiS':
            where_sql, params = self.build_zois_where(q, type_, label_id)
            join_sql = ""
        else:
            where_sql, params = self.build_zapisy_where(q, type_, zq, month, label_id, label_zapisy_id)
            join_sql = "LEFT JOIN ZOiS s ON z.Z_3 = s.S_1"
            base_tbl = "Zapisy z"

        with self.transaction() as conn:
            conn.execute("INSERT OR IGNORE INTO Etykiety (Nazwa, Obszar) VALUES (?, ?)", (label_name, area))
            label_id = conn.execute("SELECT Id FROM Etykiety WHERE Nazwa = ? AND Obszar = ?", (label_name, area)).fetchone()[0]
            
            insert_query = f"""
                INSERT OR IGNORE INTO {tbl} (EtykietaId, {col})
                SELECT :label_id, {base_col} FROM {base_tbl} {join_sql} {where_sql}
            """
            params['label_id'] = label_id
            cursor = conn.execute(insert_query, params)
            return cursor.rowcount

    def bulk_remove_labels(self, area: str, ids: List[Any], label_name: str):
        """Masowe usuwanie powiązania etykiety z listą ID."""
        if not ids or not label_name: return
        with self.transaction() as conn:
            cursor = conn.cursor()
            label_row = cursor.execute("SELECT Id FROM Etykiety WHERE Nazwa = ? AND Obszar = ?", (label_name, area)).fetchone()
            if not label_row: return
            label_id = label_row[0]
            
            table_map = {'ZOiS': ('Etykiety_ZOiS', 'ZOiS_S1'), 'Dziennik': ('Etykiety_Dziennik', 'DziennikId'), 'Zapisy': ('Etykiety_Zapisy', 'ZapisyId')}
            tbl, col = table_map[area]
            placeholders = ','.join('?' for _ in ids)
            cursor.execute(f"DELETE FROM {tbl} WHERE EtykietaId = ? AND {col} IN ({placeholders})", (label_id, *ids))

    def bulk_remove_labels_by_filter(self, area: str, filters: Dict[str, Any], label_name: str) -> int:
        """Usuwa etykietę z rekordów pasujących do filtrów."""
        table_map = {
            'ZOiS': ('Etykiety_ZOiS', 'ZOiS_S1', 'ZOiS', 'S_1'),
            'Dziennik': ('Etykiety_Dziennik', 'DziennikId', 'Zapisy', 'Dziennik_Id'),
            'Zapisy': ('Etykiety_Zapisy', 'ZapisyId', 'Zapisy', 'Id')
        }
        tbl, col, base_tbl, base_col = table_map[area]
        
        q = filters.get("q", "")
        type_ = filters.get("type", "")
        zq = filters.get("zq", "")
        month = filters.get("month", "")
        label_id = filters.get("label_id", "")
        label_zapisy_id = filters.get("label_zapisy_id", "")
        
        if area == 'ZOiS':
            where_sql, params = self.build_zois_where(q, type_, label_id)
            join_sql = ""
        else:
            where_sql, params = self.build_zapisy_where(q, type_, zq, month, label_id, label_zapisy_id)
            join_sql = "LEFT JOIN ZOiS s ON z.Z_3 = s.S_1"
            base_tbl = "Zapisy z"

        with self.transaction() as conn:
            label_row = conn.execute("SELECT Id FROM Etykiety WHERE Nazwa = ? AND Obszar = ?", (label_name, area)).fetchone()
            if not label_row: return 0
            label_id = label_row[0]
            
            delete_query = f"""
                DELETE FROM {tbl} WHERE EtykietaId = :label_id AND {col} IN (
                    SELECT {base_col} FROM {base_tbl} {join_sql} {where_sql}
                )
            """
            params['label_id'] = label_id
            cursor = conn.execute(delete_query, params)
            return cursor.rowcount

    def find_account_mapping(self, bz_target: float, bo_target: Optional[float], side: str, prefixes: str, timeout: int = 30) -> Optional[List[str]]:
        import time
        start_time = time.perf_counter()
        
        prefix_clauses = []
        params = {}
        if prefixes:
            prefix_list = self.parse_smart_query(prefixes)
            for i, p in enumerate(prefix_list):
                key = f"p{i}"
                prefix_clauses.append(f"S_1 LIKE :{key} || '%'")
                params[key] = p
        
        where_prefix = (" AND (" + " OR ".join(prefix_clauses) + ")") if prefix_clauses else ""
        
        conn = self.get_connection()
        sql = f"SELECT S_1, S_4, S_5, S_10, S_11 FROM ZOiS WHERE IsAnalytical = 1 {where_prefix} ORDER BY S_1"
        rows = conn.execute(sql, params).fetchall()
        
        accounts = []
        for row in rows:
            bz = (row['S_10'] - row['S_11']) if side == "Wn" else (row['S_11'] - row['S_10'])
            bo = (row['S_4'] - row['S_5']) if side == "Wn" else (row['S_5'] - row['S_4'])
            accounts.append({
                's1': row['S_1'],
                'bz': float(bz or 0.0),
                'bo': float(bo or 0.0)
            })
            
        # Optymalizacja: filtrowanie kont z zerowym saldem (chyba że szukamy dokładnie 0)
        if bo_target is None:
            accounts = [a for a in accounts if abs(a['bz']) > 0.001]
        else:
            accounts = [a for a in accounts if abs(a['bz']) > 0.001 or abs(a['bo']) > 0.001]
            
        best_match = None
        
        def backtrack(index, current_bz, current_bo, path):
            nonlocal best_match
            if time.perf_counter() - start_time > timeout:
                raise TimeoutError("Przekroczono czas wyszukiwania")
                
            if abs(current_bz - bz_target) < 0.001:
                if bo_target is None or abs(current_bo - bo_target) < 0.001:
                    best_match = list(path)
                    return True
                    
            if index >= len(accounts):
                return False
                
            acc = accounts[index]
            path.append(acc['s1'])
            if backtrack(index + 1, current_bz + acc['bz'], current_bo + acc['bo'], path):
                return True
            path.pop()
            
            if backtrack(index + 1, current_bz, current_bo, path):
                return True
                
            return False

        try:
            backtrack(0, 0.0, 0.0, [])
        except TimeoutError as e:
            raise e
            
        return best_match

db_service = DatabaseService()


