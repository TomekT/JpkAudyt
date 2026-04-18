from typing import List, Dict, Any
from app.services.database import db_service

class ChatService:
    """
    Serwis dostarczający ustrukturyzowane dane z bazy SQLite dla Agenta AI.
    Wykorzystuje semantyczne aliasy SQL, aby wyniki były czytelne dla modelu LLM.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = db_service
        # Upewniamy się, że połączenie jest nawiązane, jeśli ścieżka została podana
        if self.db_path and (not self.db.current_db_path or str(self.db.current_db_path) != self.db_path):
            try:
                self.db.connect(self.db_path)
            except:
                pass

    def get_account_balance(self, account_code: str) -> List[Dict[str, Any]]:
        """
        Pobiera dane finansowe (bilans otwarcia, obroty, salda) dla konkretnego konta 
        z tabeli ZOiS.
        """
        print(f"[AI-TOOL] Wywołano funkcję get_account_balance z parametrem {account_code}")
        
        query = """
            SELECT 
                S_1 AS numer_konta, 
                S_2 AS nazwa_konta, 
                S_4 AS bo_wn, 
                S_5 AS bo_ma, 
                S_8 AS obroty_wn, 
                S_9 AS obroty_ma, 
                S_10 AS saldo_koncowe_wn, 
                S_11 AS saldo_koncowe_ma
            FROM ZOiS
            WHERE S_1 = ?
        """

        try:
            results = self.db.execute_query(query, (account_code,))
            if not results:
                return [{"info": f"Nie znaleziono danych dla konta: {account_code}"}]
            return results
        except Exception as e:
            return [{"error": f"Błąd serwisu: {str(e)}"}]

    def _build_where_clause(self, 
                           numer_konta: str = None, 
                           numer_dziennika: str = None, 
                           kwota_min: float = None, 
                           dokument: str = None,  
                           opis: str = None) -> tuple:
        """
        Prywatna metoda pomocnicza budująca klauzulę WHERE na podstawie filtrów.
        Zwraca krotkę (query_part, params).
        """
        where_clauses = []
        params = []

        if numer_konta:
            where_clauses.append("Numer_Konta LIKE ?")
            params.append(f"{numer_konta}%")
        
        if numer_dziennika:
            where_clauses.append("Numer_Dziennika = ?")
            params.append(numer_dziennika)
            
        if kwota_min is not None:
            where_clauses.append("Kwota >= ?")
            params.append(float(kwota_min))

        if dokument:
            where_clauses.append("Rodzaj_Dowodu LIKE ?")
            params.append(f"%{dokument}%")
            
        if opis:
            where_clauses.append("Opis_Zapisu LIKE ?")
            params.append(f"%{opis}%")

        query_part = ""
        if where_clauses:
            query_part = " WHERE " + " AND ".join(where_clauses)
            
        return query_part, params

    def get_accounting_summary(self, 
                               numer_konta: str = None, 
                               numer_dziennika: str = None, 
                               kwota_min: float = None, 
                               dokument: str = None,  
                               opis: str = None) -> Dict[str, Any]:
        """
        Pobiera sumaryczne dane finansowe dla zapisów spełniających kryteria.
        Zwraca liczbę zapisów oraz sumy stron Wn i Ma.
        """
        print(f"[AI-TOOL] Wywołano get_accounting_summary: konto={numer_konta}, dziennik={numer_dziennika}, kwota_min={kwota_min}, dokument={dokument}, opis={opis}")
        
        where_clause, params = self._build_where_clause(numer_konta, numer_dziennika, kwota_min, dokument, opis)
        
        query = f"""
            SELECT 
                COUNT(*) as liczba_zapisow, 
                SUM(Kwota_Wn) as suma_wn, 
                SUM(Kwota_Ma) as suma_ma 
            FROM v_zapisy_pelne 
            {where_clause}
        """

        try:
            results = self.db.execute_query(query, tuple(params))
            if not results:
                return {"liczba_zapisow": 0, "suma_wn": 0.0, "suma_ma": 0.0}
            
            row = results[0]
            return {
                "liczba_zapisow": row.get("liczba_zapisow", 0),
                "suma_wn": float(row.get("suma_wn") or 0.0),
                "suma_ma": float(row.get("suma_ma") or 0.0)
            }
        except Exception as e:
            return {"error": f"Błąd podczas pobierania podsumowania: {str(e)}"}

    def search_accounting_entries(self, 
                                  numer_konta: str = None, 
                                  numer_dziennika: str = None, 
                                  kwota_min: float = None, 
                                  dokument: str = None,  
                                  opis: str = None, 
                                  limit: int = 100) -> List[Dict[str, Any]]:
        """
        Wyszukuje najważniejsze zapisy księgowe, domyślnie sortując je od najwyższej kwoty (DESC). 
        Używaj tego narzędzia, gdy użytkownik pyta o:
        - Konkretne dokumenty lub detale zapisów.
        - 'Najważniejsze', 'największe' lub 'kluczowe' pozycje (wykorzystaj parametr limit).
        - Przykłady operacji dla danego konta/opisu.
        Pomija operacje Bilansu Otwarcia (BO).

        Args:
            numer_konta: Numer konta (dopasowanie od początku, np. '101%').
            numer_dziennika: Numer dziennika/dowodu (dopasowanie dokładne).
            dokument: Numer dowodu księgowego (dopasowanie po frazie).
            opis: Fragment opisu zapisu (szukanie po frazie).
            limit: Określa ile największych kwotowo pozycji ma zostać zwróconych (np. limit=5 zwróci 5 największych zapisów).

        Returns:
            Lista słowników z danymi o zapisach płatności i księgowaniach.
        """
        print(f"[AI-TOOL] Wywołano search_accounting_entries: konto={numer_konta}, dziennik={numer_dziennika}, kwota_min={kwota_min}, dokument={dokument}, opis={opis}")
        
        where_clause, params = self._build_where_clause(numer_konta, numer_dziennika, kwota_min, dokument, opis)
        
        query = f"SELECT * FROM v_zapisy_pelne {where_clause} ORDER BY Kwota DESC LIMIT ?"
        params.append(limit)

        try:
            results = self.db.execute_query(query, tuple(params))
            if not results:
                return [{"info": "Nie znaleziono zapisów spełniających podane kryteria."}]
            return results
        except Exception as e:
            return [{"error": f"Błąd podczas wyszukiwania zapisów: {str(e)}"}]