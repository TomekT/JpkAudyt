import sqlite3
import re
from typing import Optional, List, Any, Dict, AsyncIterator
from contextlib import asynccontextmanager
import threading

from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
from app.services.zapisy_service import ZapisyService

logger = logging.getLogger(__name__)

def parse_smart_query(q: str):
    """Pomocnicza funkcja parsująca zapytania smart (LUB, przecinki, itp.)"""
    if not q:
        return []
    # Split by common separators: LUB (case insensitive), comma, pipe
    parts = re.split(r'\s+LUB\s+|[,|]', q, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p.strip()]

class DatabaseService:

    def build_zois_where(self, q: str = "", type: str = "", forced_ids: Optional[List[str]] = None, synthetic: bool = True, empty: bool = True, obszar_id: Optional[str] = None):
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
            terms = parse_smart_query(q)
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
            
        if obszar_id:
            try:
                target_obszar = int(obszar_id)
                where_clauses.append("S_1 IN (SELECT S_1 FROM v_zois_resolved_mapping WHERE Obszar_Id = :obszar_id)")
                params["obszar_id"] = target_obszar
            except ValueError:
                pass
                
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        return where_sql, params

    def get_zois_data(self, q: str = "", type: str = "", forced_ids: Optional[List[str]] = None, synthetic: bool = True, empty: bool = True, obszar_id: Optional[str] = None):
        where_sql, params = self.build_zois_where(q, type, forced_ids, synthetic, empty, obszar_id)
        sql = f"SELECT * FROM ZOiS {where_sql} ORDER BY S_1"
        return self.get_connection().execute(sql, params).fetchall()


    def __init__(self):
        self.thread_local = threading.local()
        self.current_db_path: Optional[Path] = None
        self._all_connections_lock = threading.Lock()
        self._all_connections = [] # List of (path, conn)

    def connect(self, db_path: str):
        """Sets the current active database path and initializes it for the current thread."""
        try:
            path = Path(db_path)
            if not path.exists():
                raise FileNotFoundError(f"Database file not found: {db_path}")
                
            self.current_db_path = path
            
            # Reset current thread connection
            if hasattr(self.thread_local, 'conn') and self.thread_local.conn:
                try:
                    self.thread_local.conn.close()
                except:
                    pass
                self.thread_local.conn = None
                self.thread_local.db_path = None
            
            # Initial connection to verify
            conn = self.get_connection()
            
            # Automatyczna optymalizacja i migracja schematu dla wydajności
            self._ensure_performance_schema(conn)
            
            logger.info("Database path set and initialized for current thread.")
        except Exception as e:
            logger.error(f"Failed to connect to database {db_path}: {e}")
            raise e

    def close_all_connections_to_path(self, path: Path):
        """Closes all tracked connections to a specific path across all threads."""
        target_path = path.resolve()
        with self._all_connections_lock:
            remaining = []
            for p, conn in self._all_connections:
                try:
                    if p.resolve() == target_path:
                        logger.info(f"Force closing connection to {p}")
                        conn.close()
                    else:
                        remaining.append((p, conn))
                except Exception as e:
                    logger.warning(f"Error closing tracked connection: {e}")
            self._all_connections = remaining

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


    def _unregister_connection(self, conn: sqlite3.Connection):
        """Removes a connection from the global tracking list."""
        with self._all_connections_lock:
            self._all_connections = [(p, c) for p, c in self._all_connections if c != conn]

    def get_connection(self) -> sqlite3.Connection:
        if not hasattr(self.thread_local, 'conn') or getattr(self.thread_local, 'db_path', None) != self.current_db_path:
            if hasattr(self.thread_local, 'conn') and self.thread_local.conn:
                try:
                    old_conn = self.thread_local.conn
                    old_conn.close()
                    self._unregister_connection(old_conn)
                except:
                    pass
                    
            if not self.current_db_path:
                raise RuntimeError("No active database connection path set.")
                
            path = self.current_db_path
            logger.info(f"Connecting to database (thread {threading.get_ident()}): {path}")
            conn = sqlite3.connect(str(path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            
            # Read-heavy optimizations
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode = WAL;")
            cursor.execute("PRAGMA temp_store = MEMORY;")
            cursor.execute("PRAGMA mmap_size = 3000000000;")
            cursor.execute("PRAGMA cache_size = -64000;")
            cursor.execute("PRAGMA synchronous = NORMAL;")
            
            self.thread_local.conn = conn
            self.thread_local.db_path = path
            
            # Register connection globally
            with self._all_connections_lock:
                self._all_connections.append((path, conn))
            
        return self.thread_local.conn

    def close(self):
        if hasattr(self.thread_local, 'conn') and self.thread_local.conn is not None:
            try:
                self.thread_local.conn.close()
            except:
                pass
            self.thread_local.conn = None
            self.thread_local.db_path = None
            logger.info(f"Database connection closed for thread {threading.get_ident()}")



    def init_db(self, db_path: str, schema_path: str = "schema.sql"):
        """Creates a new database file and applies the schema."""
        if Path(db_path).exists():
            # Sprawdź czy baza jest faktycznie zainicjalizowana
            try:
                temp_conn = sqlite3.connect(db_path)
                cursor = temp_conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Config'")
                initialized = cursor.fetchone() is not None
                temp_conn.close()
                if initialized:
                    logger.info(f"Database already exists and is initialized at {db_path}. Using existing.")
                    return
                else:
                    logger.warning(f"Database file exists at {db_path} but is not initialized. Re-applying schema.")
            except Exception:
                logger.warning(f"Database file at {db_path} is corrupted or inaccessible. Attempting to overwrite with schema.")

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
        if getattr(self.thread_local, 'conn', None) is None and not self.current_db_path:
             # If no connection and no db path is set, do nothing.
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



    def group_zois_accounts(self):
        """Tworzy strukturę analityczną 3-znakową dla kont ZOiS, agregując salda."""
        with self.transaction() as conn:
            cursor = conn.cursor()
            
            # A) Utworzenie brakujących kont syntetycznych
            cursor.execute("""
                INSERT INTO ZOiS (S_1, S_2, S_3, IsAnalytical, TypKonta)
                SELECT DISTINCT 
                    substr(S_1, 1, 3), 
                    substr(S_1, 1, 3), 
                    substr(S_1, 1, 3), 
                    0,                 
                    'GrupaKont'        
                FROM ZOiS
                WHERE substr(S_1, 1, 3) NOT IN (SELECT S_1 FROM ZOiS)
            """)
            
            # B) Naprawa hierarchii
            cursor.execute("""
                UPDATE ZOiS
                SET S_3 = substr(S_1, 1, 3)
                WHERE length(S_1) > 3
            """)
            
            # C) Agregacja wartości finansowych - idempotentna modyfikacja ograniczająca do kont posiadających dzieci
            cursor.execute("""
                UPDATE ZOiS AS parent
                SET 
                    S_4 = (SELECT SUM(COALESCE(child.S_4, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3),
                    S_5 = (SELECT SUM(COALESCE(child.S_5, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3),
                    S_6 = (SELECT SUM(COALESCE(child.S_6, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3),
                    S_7 = (SELECT SUM(COALESCE(child.S_7, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3),
                    S_8 = (SELECT SUM(COALESCE(child.S_8, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3),
                    S_9 = (SELECT SUM(COALESCE(child.S_9, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3),
                    S_10 = (SELECT SUM(COALESCE(child.S_10, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3),
                    S_11 = (SELECT SUM(COALESCE(child.S_11, 0)) FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3)
                WHERE length(S_1) = 3
                  AND EXISTS (SELECT 1 FROM ZOiS child WHERE child.S_3 = parent.S_1 AND length(child.S_1) > 3)
            """)

    def get_unnamed_groups(self) -> list:
        conn = self.get_connection()
        groups = conn.execute("SELECT S_1, S_10, S_11 FROM ZOiS WHERE S_1 = S_2 AND length(S_1) = 3").fetchall()
        
        result = []
        for g in groups:
            s1 = g['S_1']
            wn = float(g['S_10'] or 0)
            ma = float(g['S_11'] or 0)
            saldo = "both"
            if wn > 0 and ma == 0: saldo = "debit"
            elif ma > 0 and wn == 0: saldo = "credit"
            elif wn == 0 and ma == 0: saldo = "none"
            
            children = conn.execute("SELECT S_2 FROM ZOiS WHERE S_3 = ? AND length(S_1) > 3", (s1,)).fetchall()
            children_names = [c['S_2'] for c in children]
            
            result.append({
                "numer_grupy": s1,
                "saldo": saldo,
                "analityka": children_names
            })
        return result

    def update_group_names(self, names_data: Any):
        if not names_data: return
        
        # Przekształć listę słowników w jeden słownik, jeśli przyszła lista
        final_dict = {}
        if isinstance(names_data, list):
            for item in names_data:
                if isinstance(item, dict):
                    # Obsługa formatu {"100": "Nazwa"}
                    if len(item) == 1 and "numer_grupy" not in item:
                        final_dict.update(item)
                    # Obsługa formatu {"numer_grupy": "100", "proponowana_nazwa": "Nazwa"} lub podobnych
                    elif "numer_grupy" in item:
                        k = item.get("numer_grupy")
                        v = item.get("proponowana_nazwa") or item.get("nazwa") or item.get("value")
                        if k and v:
                            final_dict[k] = v
        elif isinstance(names_data, dict):
            final_dict = names_data

        if not final_dict:
            return

        with self.transaction() as conn:
            cursor = conn.cursor()
            for s1, new_name in final_dict.items():
                cursor.execute("UPDATE ZOiS SET S_2 = ? WHERE S_1 = ? AND length(S_1) = 3", (new_name, s1))



    def find_account_mapping(self, bz_target: float, bo_target: Optional[float], side: str, prefixes: str, timeout: int = 30) -> Optional[List[str]]:
        import time
        start_time = time.perf_counter()
        
        prefix_clauses = []
        params = {}
        if prefixes:
            prefix_list = parse_smart_query(prefixes)
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

    def _ensure_performance_schema(self, conn: sqlite3.Connection):
        """Sprawdza i dodaje brakujące kolumny wydajnościowe oraz inicjalizuje obszary."""
        try:
            cursor = conn.cursor()
            
            # 1. Sprawdź kolumny w ZOiS
            cursor.execute("PRAGMA table_info(ZOiS)")
            columns = [row['name'] for row in cursor.fetchall()]
            
            needed_columns = [
                ("Obszar_Id", "INTEGER"),
                ("Strona_Salda", "TEXT"),
                ("Is_Direct_Mapping", "INTEGER DEFAULT 0"),
                ("Source_Mapping_Konto", "TEXT")
            ]
            
            added = False
            for col_name, col_type in needed_columns:
                if col_name not in columns:
                    logger.info(f"Dodawanie brakującej kolumny {col_name} do tabeli ZOiS...")
                    cursor.execute(f"ALTER TABLE ZOiS ADD COLUMN {col_name} {col_type}")
                    added = True
            
            # 2. Sprawdź czy Obszary są puste
            cursor.execute("SELECT COUNT(*) FROM Obszary")
            if cursor.fetchone()[0] == 0:
                logger.info("Inicjalizacja domyślnych obszarów badania...")
                from app.core.path_utils import resource_path
                obszary_sql = resource_path("insert_obszary.sql")
                if obszary_sql.exists():
                    self.execute_script_file(str(obszary_sql))
                    added = True
            
            if added:
                conn.commit()
                self.update_zois_mapping_cache()
                
        except Exception as e:
            logger.error(f"Błąd podczas sprawdzania schematu wydajnościowego: {e}")

    def update_zois_mapping_cache(self):
        """
        Denormalizuje mapowanie obszarów bezpośrednio do tabeli ZOiS.
        Kluczowe dla wydajności przy dużych zbiorach danych (>100k rekordów).
        """
        conn = self.get_connection()
        try:
            logger.info("Aktualizacja cache'u mapowania obszarów w tabeli ZOiS...")
            # 1. Czyścimy stare dane
            conn.execute("UPDATE ZOiS SET Obszar_Id = NULL, Strona_Salda = NULL, Is_Direct_Mapping = 0, Source_Mapping_Konto = NULL")
            
            # 2. Pobieramy mapowania z widoku i aktualizujemy ZOiS
            sql = """
                UPDATE ZOiS
                SET Obszar_Id = rm.Obszar_Id,
                    Strona_Salda = rm.Strona_Salda,
                    Is_Direct_Mapping = rm.Is_Direct,
                    Source_Mapping_Konto = rm.Source_Konto
                FROM v_zois_resolved_mapping rm
                WHERE ZOiS.S_1 = rm.S_1
            """
            conn.execute(sql)
            conn.commit()
            logger.info("Zakończono aktualizację cache'u mapowania.")
        except Exception as e:
            logger.error(f"Błąd aktualizacji cache'u mapowania: {e}")
            conn.rollback()

db_service = DatabaseService()


