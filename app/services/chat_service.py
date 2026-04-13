import sqlite3
from typing import List, Dict, Any

class ChatService:
    """
    Serwis dostarczający ustrukturyzowane dane z bazy SQLite dla Agenta AI.
    Wykorzystuje semantyczne aliasy SQL, aby wyniki były czytelne dla modelu LLM.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path

    def get_account_balance(self, account_code: str) -> List[Dict[str, Any]]:
        """
        Pobiera dane finansowe (bilans otwarcia, obroty, salda) dla konkretnego konta 
        z tabeli ZOiS.
        
        Logika działania:
        1. Szuka dokładnego dopasowania numeru konta.

        Args:
            account_code: Numer konta (np. '101', '201-1'). 

        Returns:
            Lista słowników z ustrukturyzowanymi danymi finansowymi:
            - numer_konta: Kolumna S_1 (Konto)
            - nazwa_konta: Kolumna S_2 (Nazwa)
            - bo_wn: Bilans Otwarcia Winien (S_4)
            - bo_ma: Bilans Otwarcia Ma (S_5)
            - obroty_wn: Obroty Winien (S_8)
            - obroty_ma: Obroty Ma (S_9)
            - saldo_koncowe_wn: Saldo Końcowe Winien (S_10)
            - saldo_koncowe_ma: Saldo Końcowe Ma (S_11)
        """
        print(f"[AI-TOOL] Wywołano funkcję get_account_balance z parametrem {account_code}")
        
        if not self.db_path:
            return [{"error": "Baza danych nie jest podłączona."}]

        # Bazowe zapytanie z aliasami semantycznymi
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
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, (account_code,))
            rows = cursor.fetchall()
            results = [dict(row) for row in rows]
            conn.close()

            if not results:
                return [{"info": f"Nie znaleziono danych dla konta: {account_code}"}]

            return results

        except sqlite3.Error as e:
            return [{"error": f"Błąd bazy danych (SQLite): {str(e)}"}]
        except Exception as e:
            return [{"error": f"Błąd krytyczny serwisu: {str(e)}"}]