import sqlite3
import re
import logging
from typing import Optional, List, Any, Dict
from pathlib import Path

logger = logging.getLogger(__name__)

class ZapisyService:
    """
    Service responsible for handling accounting entries (Zapisy) logic,
    including filtering and retrieval from the analytical views.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    @staticmethod
    def parse_smart_query(q: str):
        if not q:
            return []
        # Split by common separators: LUB (case insensitive), comma, pipe
        parts = re.split(r'\s+LUB\s+|[,|]', q, flags=re.IGNORECASE)
        return [p.strip() for p in parts if p.strip()]

    def build_zapisy_where(self, q: str = "", type: str = "", zq: str = "", month: str = "", label_id: str = "", label_zapisy_id: str = "", konto: str = "", opis: str = "", min_kwota: str = "", dziennik_id: str = ""):
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
                        term_clauses.append(f"z.Dziennik_Id IN (SELECT DISTINCT sub_z.Dziennik_Id FROM Zapisy sub_z WHERE sub_z.Z_GrupaKont = :{key})")
                        params[key] = val
                    else:
                        term_clauses.append(f"(z.Z_3 LIKE :{key} OR z.Z_2 LIKE :{key})")
                        params[key] = f"%{term}%"
                where_clauses.append("(" + " OR ".join(term_clauses) + ")")
        if month and month.isdigit():
            where_clauses.append("z.Z_DataMiesiac = :month")
            params["month"] = int(month)
            
        if konto:
            terms = self.parse_smart_query(konto)
            if terms:
                term_clauses = []
                for i, term in enumerate(terms):
                    key = f"advz3_{i}"
                    term_clauses.append(f"z.Z_3 LIKE :{key}")
                    params[key] = f"%{term}%"
                where_clauses.append("(" + " OR ".join(term_clauses) + ")")
                
        if opis:
            terms = self.parse_smart_query(opis)
            if terms:
                term_clauses = []
                for i, term in enumerate(terms):
                    key = f"advz2_{i}"
                    term_clauses.append(f"z.Z_2 LIKE :{key}")
                    params[key] = f"%{term}%"
                where_clauses.append("(" + " OR ".join(term_clauses) + ")")
                
        if dziennik_id:
            terms = self.parse_smart_query(dziennik_id)
            if terms:
                term_clauses = []
                for i, term in enumerate(terms):
                    key = f"advd1_{i}"
                    term_clauses.append(f"d.D_1 LIKE :{key}")
                    params[key] = f"%{term}%"
                where_clauses.append("(" + " OR ".join(term_clauses) + ")")
                
        if min_kwota:
            try:
                min_val = float(min_kwota)
                where_clauses.append("(ABS(COALESCE(z.Z_4, 0)) >= :min_val OR ABS(COALESCE(z.Z_7, 0)) >= :min_val)")
                params["min_val"] = min_val
            except ValueError:
                pass
                
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        return where_sql, params

    def get_zapisy_pelne(self, q: str = "", type: str = "", zq: str = "", month: str = "", label_id: str = "", label_zapisy_id: str = "", konto: str = "", opis: str = "", min_kwota: str = "", dziennik_id: str = "", limit: int = 100):
        """
        Retrieves accounting entries from the v_zapisy_pelne view applying all filters.
        """
        where_sql, params = self.build_zapisy_where(q, type, zq, month, label_id, label_zapisy_id, konto, opis, min_kwota, dziennik_id)
        sql = f"SELECT * FROM v_zapisy_pelne z LEFT JOIN ZOiS s ON z.Numer_Konta = s.S_1 {where_sql} LIMIT :limit"
        params["limit"] = limit

        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
            conn.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Error getting records from v_zapisy_pelne: {e}")
            return []
