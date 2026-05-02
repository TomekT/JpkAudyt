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
    
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
            
        for row in rows:
            formatted_row = []
            for item in row:
                if item is None:
                    formatted_row.append("")
                elif isinstance(item, bool):
                    # Zabezpieczenie przed bool, choć sqlite nie ma typu bool jako takiego
                    formatted_row.append(str(item))
                elif isinstance(item, (int, float)):
                    # Formatowanie numeryczne: zmiana kropki dziesiętnej na przecinek, brak separatora tysięcy
                    # sqlite zwraca float lub int
                    formatted_row.append(str(item).replace(".", ","))
                elif isinstance(item, str):
                    # Sanityzacja tekstów: usunięcie znaków nowej linii oraz tabulacji (zamiana na spację)
                    sanitized = re.sub(r'[\r\n\t]+', ' ', item)
                    formatted_row.append(sanitized)
                else:
                    formatted_row.append(str(item))
                    
            yield "\t".join(formatted_row) + "\n"
