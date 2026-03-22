from pathlib import Path
from lxml import etree
from datetime import datetime
import logging
import os
from app.services.database import db_service
from app.core.path_utils import resource_path

logger = logging.getLogger(__name__)

class ETLService:
    BATCH_SIZE = 5000

    def __init__(self):
        self.db = db_service

    def import_jpk(self, xml_path: str) -> str:
        """Imports a JPK XML file using optimized streaming and batching."""
        xml_file = Path(xml_path)
        if not xml_file.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        logger.info(f"Starting optimized import of {xml_path}")
        metadata = self._extract_metadata(xml_path)
        db_path = self._init_database(xml_file, metadata)
        
        # Sprawdzenie czy baza już istnieje i zawiera dane przed procesowaniem
        if os.path.exists(db_path) and self.db.is_database_populated(str(db_path)):
            raise ValueError("Baza danych dla tego pliku JPK już istnieje i zawiera dane. Import został przerwany, aby uniknąć duplikatów.")

        # Connect is handled by db_service globally but we ensure it's connected to new DB
        self.db.connect(str(db_path))
        
        self._process_xml(xml_path)
        
        # Post-processing outside the main streaming loop
        self._update_zois_analytical_status()
        self._insert_header_data(metadata)
        self._update_zois_typ_konta()
        
        logger.info("Import completed successfully.")
        return str(db_path)

    def _extract_metadata(self, xml_path: str) -> dict:
        metadata = {}
        target_tags = {'NIP', 'PelnaNazwa', 'REGON', 'KodKraju', 'Wojewodztwo', 'Powiat', 'Gmina', 
                       'Ulica', 'NrDomu', 'NrLokalu', 'Miejscowosc', 'KodPocztowy',
                       'DataOd', 'DataDo', 'DataWytworzeniaJPK', 'KodFormularza'}
        try:
            for event, elem in etree.iterparse(xml_path, events=('end',)):
                tag = etree.QName(elem).localname
                if tag in target_tags:
                    if tag == 'KodFormularza' and elem.text and 'JPK_KR' not in elem.text:
                        raise ValueError(f"Nieprawidłowy typ pliku JPK: {elem.text}")
                    if tag not in metadata:
                        metadata[tag] = elem.text
                if tag in ['ZOiS', 'Dziennik', 'Kontrahenci']:
                    break
        except etree.XMLSyntaxError:
            raise ValueError("Błąd składni XML")
        return metadata

    def _init_database(self, xml_file: Path, metadata: dict) -> Path:
        nip = metadata.get('NIP', 'UNKNOWN')
        year = metadata.get('DataDo', 'YEAR')[:4]
        date_created = (metadata.get('DataWytworzeniaJPK') or "DATE")[:10].replace('-', '')
        
        db_filename = f"{nip}-{year}-{date_created}.db"
        db_path = xml_file.parent / db_filename
        
        # Create DB
        schema_path = resource_path("schema.sql")
        self.db.init_db(str(db_path), str(schema_path))
        self.db.connect(str(db_path))
        
        # Load Slownik - Assuming "insert_slownik.sql" is in CWD
        slownik_path = resource_path("insert_slownik.sql")
        if slownik_path.exists():
            self.db.execute_script_file(str(slownik_path))
            
        return db_path

    def _process_xml(self, xml_path: str):
        """Streaming parser with heavy batching and business key linking."""
        buffers = {
            'ZOiS': [],
            'Kontrahenci': [],
            'Dziennik': [],
            'Zapisy': [],
            'RPD': [],
            'Ctrl': []
        }

        with self.db.transaction() as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA temp_store = MEMORY")

            def flush_buffers(target_table=None, force=False):
                for table, data in buffers.items():
                    if not data or (target_table and table != target_table):
                        continue
                    
                    if not force and len(data) < self.BATCH_SIZE:
                        continue
                    
                    if table == 'ZOiS':
                        sql = """INSERT OR REPLACE INTO ZOiS (S_1, S_2, S_3, S_4, S_5, S_6, S_7, S_8, S_9, S_10, S_11, S_12_1, S_12_2, S_12_3) 
                                 VALUES (:S_1, :S_2, :S_3, :S_4, :S_5, :S_6, :S_7, :S_8, :S_9, :S_10, :S_11, :S_12_1, :S_12_2, :S_12_3)"""
                    elif table == 'Kontrahenci':
                        sql = "INSERT OR REPLACE INTO Kontrahenci (T_1, T_2, T_3) VALUES (:T_1, :T_2, :T_3)"
                    elif table == 'Dziennik':
                        sql = """INSERT INTO Dziennik (D_1, D_2, D_3, D_4, D_5, D_6, D_7, D_8, D_9, D_10, D_11)
                                 VALUES (:D_1, :D_2, :D_3, :D_4, :D_5, :D_6, :D_7, :D_8, :D_9, :D_10, :D_11)"""
                    elif table == 'Zapisy':
                        sql = """INSERT INTO Zapisy (Z_1, Z_2, Z_3, Z_4, Z_5, Z_6, Z_7, Z_8, Z_9, Z_Data, Z_DataMiesiac, Z_NrZapisu)
                                 VALUES (:Z_1, :Z_2, :Z_3, :Z_4, :Z_5, :Z_6, :Z_7, :Z_8, :Z_9, :Z_Data, :Z_DataMiesiac, :Z_NrZapisu)"""
                    elif table == 'RPD':
                        sql = "INSERT INTO RPD (K_1, K_2, K_3, K_4, K_5, K_6, K_7, K_8) VALUES (:K_1, :K_2, :K_3, :K_4, :K_5, :K_6, :K_7, :K_8)"
                    elif table == 'Ctrl':
                        sql = "INSERT INTO Ctrl (C_1, C_2, C_3, C_4, C_5) VALUES (:C_1, :C_2, :C_3, :C_4, :C_5)"
                    else: continue

                    try:
                        cursor.executemany(sql, data)
                        data.clear()
                    except Exception as e:
                        logger.error(f"Batch insert error {table}: {e}")
                        raise e

            def get_data(elem):
                return {etree.QName(c).localname: (c.text.strip() if c.text else None) for c in elem}

            # Streaming iteration
            context = etree.iterparse(xml_path, events=('end',))
            for event, elem in context:
                tag = etree.QName(elem).localname
                is_target_record = False
                
                if tag.startswith('ZOiS') and tag != 'ZOiS':
                    d = get_data(elem)
                    buffers['ZOiS'].append({k: d.get(k) for k in ['S_1', 'S_2', 'S_3', 'S_4', 'S_5', 'S_6', 'S_7', 'S_8', 'S_9', 'S_10', 'S_11', 'S_12_1', 'S_12_2', 'S_12_3']})
                    is_target_record = True
                
                elif tag == 'Kontrahent':
                    d = get_data(elem)
                    buffers['Kontrahenci'].append({'T_1': d.get('T_1'), 'T_2': d.get('T_2'), 'T_3': d.get('T_3')})
                    is_target_record = True
                
                elif tag == 'Dziennik':
                    d_data = {}
                    d_1 = None
                    zapisy_in_block = []
                    
                    for child in elem:
                        ctag = etree.QName(child).localname
                        if ctag == 'KontoZapis':
                            zapisy_in_block.append(child)
                        else:
                            d_data[ctag] = child.text.strip() if child.text else None
                            if ctag == 'D_1': d_1 = d_data[ctag]

                    # Add Dziennik row
                    buffers['Dziennik'].append({k: d_data.get(k) for k in ['D_1', 'D_2', 'D_3', 'D_4', 'D_5', 'D_6', 'D_7', 'D_8', 'D_9', 'D_10', 'D_11']})
                    
                    # Add Zapisy rows linked by Business Key D_1
                    d8 = d_data.get('D_8')
                    month = None
                    if d8 and len(d8) >= 7:
                        try:
                            month = int(d8[5:7])
                        except (ValueError, TypeError):
                            month = None

                    for kz in zapisy_in_block:
                        z = get_data(kz)
                        buffers['Zapisy'].append({
                            'Z_1': z.get('Z_1'), 'Z_2': z.get('Z_2'), 'Z_3': z.get('Z_3'),
                            'Z_4': z.get('Z_4'), 'Z_5': z.get('Z_5'), 'Z_6': z.get('Z_6'),
                            'Z_7': z.get('Z_7'), 'Z_8': z.get('Z_8'), 'Z_9': z.get('Z_9'),
                            'Z_Data': d8, 'Z_DataMiesiac': month, 'Z_NrZapisu': d_1
                        })
                    is_target_record = True

                elif tag == 'RPD':
                    d = get_data(elem)
                    buffers['RPD'].append({f'K_{i}': d.get(f'K_{i}') for i in range(1, 9)})
                    is_target_record = True
                
                elif tag == 'Ctrl':
                    d = get_data(elem)
                    buffers['Ctrl'].append({f'C_{i}': d.get(f'C_{i}') for i in range(1, 6)})
                    is_target_record = True

                if is_target_record:
                    # Check total buffer size
                    if sum(len(v) for v in buffers.values()) >= self.BATCH_SIZE:
                        flush_buffers()
                    
                    # Cleanup lxml memory ONLY for the target record (clearing children prematurely causes NULLs)
                    elem.clear()
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]

            # Final flush
            flush_buffers(force=True)

            # Link Zapisy to Dziennik using business key D_1 = Z_NrZapisu
            logger.info("Linking Zapisy to Dziennik via business key...")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_dziennik_d1 ON Dziennik(D_1)")
            cursor.execute("""
                UPDATE Zapisy 
                SET Dziennik_Id = (SELECT Id FROM Dziennik WHERE D_1 = Zapisy.Z_NrZapisu) 
                WHERE Dziennik_Id IS NULL
            """)
            cursor.execute("DROP INDEX idx_dziennik_d1")

    def _insert_header_data(self, metadata):
        conn = self.db.get_connection()
        try:
            conn.execute(
                "INSERT INTO Naglowek (KodFormularza, DataWytworzeniaJPK, DataOd, DataDo) VALUES (?, ?, ?, ?)",
                ('JPK_KR_PD', metadata.get('DataWytworzeniaJPK'), metadata.get('DataOd'), metadata.get('DataDo'))
            )
            if 'NIP' in metadata:
                conn.execute(
                    """INSERT OR REPLACE INTO Podmiot (
                        NIP, PelnaNazwa, REGON, KodKraju, Wojewodztwo, Powiat, Gmina, 
                        Ulica, NrDomu, NrLokalu, Miejscowosc, KodPocztowy
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (metadata.get('NIP'), metadata.get('PelnaNazwa'), metadata.get('REGON'), metadata.get('KodKraju'),
                     metadata.get('Wojewodztwo'), metadata.get('Powiat'), metadata.get('Gmina'), metadata.get('Ulica'),
                     metadata.get('NrDomu'), metadata.get('NrLokalu'), metadata.get('Miejscowosc'), metadata.get('KodPocztowy'))
                )
            conn.commit()
        except Exception as e:
            logger.error(f"Error inserting header: {e}")

    def _update_zois_typ_konta(self):
        conn = self.db.get_connection()
        try:
            sql = """
            UPDATE ZOiS SET TypKonta = (
                SELECT CASE 
                    WHEN sk.Poziom3 IS NOT NULL AND sk.Poziom3 != '' THEN sk.Poziom1 || ' / ' || sk.Poziom2 || ' / ' || sk.Poziom3
                    WHEN sk.Poziom2 IS NOT NULL AND sk.Poziom2 != '' THEN sk.Poziom1 || ' / ' || sk.Poziom2
                    ELSE sk.Poziom1
                END
                FROM Slownik_Kategorii sk WHERE sk.Kod = ZOiS.S_12_1
            ) WHERE S_12_1 IS NOT NULL;
            """
            conn.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating ZOiS.TypKonta: {e}")

    def _update_zois_analytical_status(self):
        conn = self.db.get_connection()
        try:
            sql = """
            UPDATE ZOiS SET IsAnalytical = 1 
            WHERE S_1 NOT IN (SELECT DISTINCT S_3 FROM ZOiS WHERE S_3 IS NOT NULL AND S_3 != '');
            """
            conn.execute(sql)
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating ZOiS IsAnalytical: {e}")

etl_service = ETLService()

