import sqlite3
import re
from typing import Generator, Union

def generate_tsv(conn: sqlite3.Connection, query: str, params: Union[tuple, dict, list]) -> Generator[str, None, None]:
    """
    Generyczny generator eksportujący wyniki zapytania SQL do formatu TSV.
    Pobiera dane paczkami, aby zoptymalizować zużycie pamięci (fetchmany).
    """
    cursor = conn.cursor()
    cursor.execute(query, params)
    
    # Dynamiczne nagłówki z opisu kursora
    col_names = [desc[0] for desc in cursor.description]
    yield "\t".join(col_names) + "\n"
    
    # Kolumny, które Excel agresywnie zamienia na daty (np. 401-1-1)
    TEXT_COLUMNS = {"Konto", "Konta Przec."}
    
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
            
        for row in rows:
            formatted_row = []
            for i, item in enumerate(row):
                col_name = col_names[i]
                
                if item is None:
                    val = ""
                elif isinstance(item, (int, float)):
                    # Formatowanie numeryczne: zmiana kropki dziesiętnej na przecinek
                    val = str(item).replace(".", ",")
                elif isinstance(item, str):
                    # Sanityzacja tekstów: usunięcie znaków nowej linii oraz tabulacji
                    val = re.sub(r'[\r\n\t]+', ' ', item)
                else:
                    val = str(item)
                
                # Wymuszanie formatu tekstowego dla wybranych kolumn (numery kont)
                if col_name in TEXT_COLUMNS and val and not val.startswith('="'):
                    val = f'="{val}"'
                    
                formatted_row.append(val)
                    
            yield "\t".join(formatted_row) + "\n"
