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

    def import_jpk(self, xml_path: str) -> str:
        """
        Imports a JPK XML file.
        Returns the path to the created database.
        """
        xml_file = Path(xml_path)
        if not xml_file.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        logger.info(f"Starting import of {xml_path}")

        # 1. Validate & Extract Metadata
        metadata = self._extract_metadata(xml_path)
        
        # 2. Init Database
        db_path = self._init_database(xml_file, metadata)
        
        # 3. Connect 
        db_service.connect(str(db_path))
        
        # 4. Process XML (Batch Insert)
        self._process_xml(xml_path)
        
        self._update_zois_analytical_status()
        
        # 5. Finalize (Insert Meta Info, indices are already created in schema)
        self._insert_header_data(metadata)
        
        # 6. Post-process: Update TypKonta based on dictionary mapping
        self._update_zois_typ_konta()
        
        logger.info("Import completed successfully.")
        return str(db_path)

    def _extract_metadata(self, xml_path: str) -> dict:
        metadata = {}
        target_tags = {
            'NIP', 'PelnaNazwa', 'REGON', 
            'KodKraju', 'Wojewodztwo', 'Powiat', 'Gmina', 
            'Ulica', 'NrDomu', 'NrLokalu', 'Miejscowosc', 'KodPocztowy',
            'DataOd', 'DataDo', 'DataWytworzeniaJPK', 'KodFormularza'
        }
        
        try:
            # We iterate through everything until we hit data sections.
            # Namespaces are ignored by using .localname via etree.QName
            for event, elem in etree.iterparse(xml_path, events=('end',)):
                tag = etree.QName(elem).localname
                
                if tag in target_tags:
                    if tag == 'KodFormularza':
                        if elem.text and 'JPK_KR' not in elem.text:
                             raise ValueError(f"Nieprawidłowy typ pliku JPK: {elem.text}")
                    else:
                        # Capture the text if not already set (keep first occurrence in header)
                        if tag not in metadata:
                            metadata[tag] = elem.text
                
                if tag in ['ZOiS', 'Dziennik', 'Kontrahenci', 'Naglowek']:
                    # We continue until we hit a clear data section marker.
                    # Note: Naglowek is a container, we don't stop there, we stop at DATA sections.
                    pass
                
                if tag in ['ZOiS', 'Dziennik', 'Kontrahenci', 'Podmiot1']:
                    # Stop if we hit repeating data sections to save time
                    if tag != 'Podmiot1' or ('NIP' in metadata and 'PelnaNazwa' in metadata):
                        # If it's Podmiot1, only stop if we already have the core info
                        # because NIP/Name are usually INSIDE Podmiot1.
                        pass

                # If we encounter the first major data section, we stop the metadata scan.
                if tag in ['ZOiS', 'Dziennik', 'Kontrahenci']:
                    break

        except etree.XMLSyntaxError:
             raise ValueError("Błąd składni XML")
             
        return metadata

    def _init_database(self, xml_file: Path, metadata: dict) -> Path:
        nip = metadata.get('NIP', 'UNKNOWN')
        
        # Extract Year from DataDo (YYYY-MM-DD)
        year = "YEAR"
        if 'DataDo' in metadata:
            try:
                year = metadata['DataDo'][:4]
            except: pass
            
        # Format DataWytworzeniaJPK (2026-01-26T14:41:08 -> 20260126)
        date_created = "DATE"
        if 'DataWytworzeniaJPK' in metadata:
            try:
                # Assuming ISO format
                dt_str = metadata['DataWytworzeniaJPK']
                # Take first 10 chars YYYY-MM-DD then remove dashes
                date_created = dt_str[:10].replace('-', '')
            except: pass
            
        db_filename = f"{nip}-{year}-{date_created}.db"
        db_path = xml_file.parent / db_filename
        
        # Create DB
        schema_path = resource_path("schema.sql")
        db_service.init_db(str(db_path), str(schema_path))
        
        # Connect to allow executing scripts
        db_service.connect(str(db_path))
        
        # Load Slownik - Assuming "insert_slownik.sql" is in CWD
        slownik_path = resource_path("insert_slownik.sql")
        if slownik_path.exists():
            db_service.execute_script_file(str(slownik_path))
            
        return db_path

    def _insert_header_data(self, metadata):
        conn = db_service.get_connection()
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
                    (
                        metadata.get('NIP'),
                        metadata.get('PelnaNazwa'),
                        metadata.get('REGON'),
                        metadata.get('KodKraju'),
                        metadata.get('Wojewodztwo'),
                        metadata.get('Powiat'),
                        metadata.get('Gmina'),
                        metadata.get('Ulica'),
                        metadata.get('NrDomu'),
                        metadata.get('NrLokalu'),
                        metadata.get('Miejscowosc'),
                        metadata.get('KodPocztowy')
                    )
                )
            conn.commit()
        except Exception as e:
            logger.error(f"Error inserting header: {e}")

    def _process_xml(self, xml_path: str):
        conn = db_service.get_connection()
        cursor = conn.cursor()
        
        # Batch buffers
        buffers = {
            'ZOiS': [],
            'Kontrahenci': [],
            'Dziennik': [],
            'Zapisy': [],
            'RPD': [],
            'Ctrl': []
        }
        
        # Helper to flush buffer
        def flush_buffer(table, data_list):
            if not data_list: return
            
            # Construct insert statement dynamically based on first item keys
            # or hardcode for performance/safety. Prefer hardcode mapping.
            # But let's build map:
            if table == 'ZOiS':
                sql = """INSERT INTO ZOiS (S_1, S_2, S_3, S_4, S_5, S_6, S_7, S_8, S_9, S_10, S_11, S_12_1, S_12_2, S_12_3) 
                         VALUES (:S_1, :S_2, :S_3, :S_4, :S_5, :S_6, :S_7, :S_8, :S_9, :S_10, :S_11, :S_12_1, :S_12_2, :S_12_3)"""
                # For safety use INSERT OR IGNORE or REPLACE? PRIMARY KEY is S_1.
                sql = sql.replace("INSERT INTO", "INSERT OR REPLACE INTO")
            
            elif table == 'Kontrahenci':
                sql = "INSERT OR REPLACE INTO Kontrahenci (T_1, T_2, T_3) VALUES (:T_1, :T_2, :T_3)"
            
            elif table == 'Dziennik':
                sql = """INSERT INTO Dziennik (D_1, D_2, D_3, D_4, D_5, D_6, D_7, D_8, D_9, D_10, D_11)
                         VALUES (:D_1, :D_2, :D_3, :D_4, :D_5, :D_6, :D_7, :D_8, :D_9, :D_10, :D_11)"""
                # We need to return ID? No, we don't need Dziennik ID here if we link Zapisy via explicit relation or order?
                # Wait, Schema for Zapisy has 'Dziennik_Id'.
                # If we batch insert Dziennik, we don't easily get the IDs back for Zapisy unless we assume order.
                # SQLite rowid? 
                # Actually, in JPK_KR, Zapisy are nested in Dziennik.
                # We can insert Dziennik one by one and get ID, OR we can use something else.
                # Batching Dziennik with Zapisy is tricky for FK.
                # Solution: Insert Dziennik, get ID (cursor.lastrowid), then buffer Zapisy with that ID.
                # So Dziennik cannot be heavily batched if we need the ID for Zapisy IMMEDIATELY.
                # BUT, we can wrap the Transaction.
                pass 
            
            elif table == 'Zapisy':
                sql = """INSERT INTO Zapisy (Dziennik_Id, Z_1, Z_2, Z_3, Z_4, Z_5, Z_6, Z_7, Z_8, Z_9, Z_Data, Z_DataMiesiac)
                         VALUES (:Dziennik_Id, :Z_1, :Z_2, :Z_3, :Z_4, :Z_5, :Z_6, :Z_7, :Z_8, :Z_9, :Z_Data, :Z_DataMiesiac)"""

            elif table == 'RPD':
                sql = "INSERT INTO RPD (K_1, K_2, K_3, K_4, K_5, K_6, K_7, K_8) VALUES (:K_1, :K_2, :K_3, :K_4, :K_5, :K_6, :K_7, :K_8)"
                
            elif table == 'Ctrl':
                sql = "INSERT INTO Ctrl (C_1, C_2, C_3, C_4, C_5) VALUES (:C_1, :C_2, :C_3, :C_4, :C_5)"

            else:
                 return

            try:
                cursor.executemany(sql, data_list)
                data_list.clear()
            except Exception as e:
                logger.error(f"Batch insert error {table}: {e}")
                raise e

        # Iterparse
        context = etree.iterparse(xml_path, events=('end',), tag=['*'])
        
        # We need to handle Dziennik specially because of Zapisy.
        # We will iterate, if we see Dziennik, we process it fully.
        
        # Helper to extract child text by localname
        def get_child_data(element):
            data = {}
            for child in element:
                tag = etree.QName(child).localname
                val = child.text
                if val:
                    data[tag] = val.strip()
                else:
                    data[tag] = val
            return data

        for event, elem in context:
            tag = etree.QName(elem).localname
            
            if tag.startswith('ZOiS') and tag != 'ZOiS': # e.g. ZOiS7
                data = get_child_data(elem)
                row = {
                    'S_1': data.get('S_1'),
                    'S_2': data.get('S_2'),
                    'S_3': data.get('S_3'),
                    'S_4': data.get('S_4'),
                    'S_5': data.get('S_5'),
                    'S_6': data.get('S_6'),
                    'S_7': data.get('S_7'),
                    'S_8': data.get('S_8'),
                    'S_9': data.get('S_9'),
                    'S_10': data.get('S_10'),
                    'S_11': data.get('S_11'),
                    'S_12_1': data.get('S_12_1'),
                    'S_12_2': data.get('S_12_2'),
                    'S_12_3': data.get('S_12_3'),
                }
                buffers['ZOiS'].append(row)
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                
            elif tag == 'Kontrahent':
                data = get_child_data(elem)
                row = {
                    'T_1': data.get('T_1'),
                    'T_2': data.get('T_2'),
                    'T_3': data.get('T_3')
                }
                buffers['Kontrahenci'].append(row)
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            elif tag == 'Dziennik':
                # Process Dziennik
                # First get Dziennik data
                d_data = {}
                # separating children handling: direct values vs KontoZapis list
                zapisy_nodes = []
                
                for child in elem:
                    ctag = etree.QName(child).localname
                    if ctag == 'KontoZapis':
                        zapisy_nodes.append(child)
                    else:
                        d_data[ctag] = child.text

                d_row = {
                    'D_1': d_data.get('D_1'),
                    'D_2': d_data.get('D_2'),
                    'D_3': d_data.get('D_3'),
                    'D_4': d_data.get('D_4'),
                    'D_5': d_data.get('D_5'),
                    'D_6': d_data.get('D_6'),
                    'D_7': d_data.get('D_7'),
                    'D_8': d_data.get('D_8'),
                    'D_9': d_data.get('D_9'),
                    'D_10': d_data.get('D_10'),
                    'D_11': d_data.get('D_11'),
                }
                
                # Posting date (D_8) and Month are needed for related Zapisy
                d8_val = d_row.get('D_8')
                month_val = None
                try:
                    if d8_val:
                        month_val = int(d8_val[5:7])
                except:
                    pass

                cursor.execute("""INSERT INTO Dziennik (D_1, D_2, D_3, D_4, D_5, D_6, D_7, D_8, D_9, D_10, D_11)
                         VALUES (:D_1, :D_2, :D_3, :D_4, :D_5, :D_6, :D_7, :D_8, :D_9, :D_10, :D_11)""", d_row)
                
                dziennik_id = cursor.lastrowid
                
                # Now process saved KontoZapis nodes
                for child in zapisy_nodes:
                    z_data = get_child_data(child)
                    z_row = {
                        'Dziennik_Id': dziennik_id,
                        'Z_1': z_data.get('Z_1'),
                        'Z_2': z_data.get('Z_2'),
                        'Z_3': z_data.get('Z_3'),
                        'Z_4': z_data.get('Z_4'),
                        'Z_5': z_data.get('Z_5'),
                        'Z_6': z_data.get('Z_6'),
                        'Z_7': z_data.get('Z_7'),
                        'Z_8': z_data.get('Z_8'),
                        'Z_9': z_data.get('Z_9'),
                        'Z_Data': d8_val,
                        'Z_DataMiesiac': month_val
                    }
                    buffers['Zapisy'].append(z_row)
                
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
            
            elif tag == 'RPD':
                data = get_child_data(elem)
                row = {
                    'K_1': data.get('K_1'),
                    'K_2': data.get('K_2'),
                    'K_3': data.get('K_3'),
                    'K_4': data.get('K_4'),
                    'K_5': data.get('K_5'),
                    'K_6': data.get('K_6'),
                    'K_7': data.get('K_7'),
                    'K_8': data.get('K_8'),
                }
                buffers['RPD'].append(row)
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            elif tag == 'Ctrl':
                data = get_child_data(elem)
                row = {
                    'C_1': data.get('C_1'),
                    'C_2': data.get('C_2'),
                    'C_3': data.get('C_3'),
                    'C_4': data.get('C_4'),
                    'C_5': data.get('C_5'),
                }
                buffers['Ctrl'].append(row)
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            # Flush Check
            for name, buf in buffers.items():
                if len(buf) >= self.BATCH_SIZE:
                    flush_buffer(name, buf)
                
        # Flush remaining
        for name, buf in buffers.items():
            flush_buffer(name, buf)

        conn.commit()

    def _update_zois_typ_konta(self):
        """
        Populates TypKonta field based on Poziom1 - Poziom2 from Slownik_Kategorii
        using S_12_1 as the join key.
        """
        conn = db_service.get_connection()
        try:
            sql = """
            UPDATE ZOiS 
            SET TypKonta = (
                SELECT 
                    CASE 
                        WHEN sk.Poziom3 IS NOT NULL AND sk.Poziom3 != '' THEN sk.Poziom1 || ' / ' || sk.Poziom2 || ' / ' || sk.Poziom3
                        WHEN sk.Poziom2 IS NOT NULL AND sk.Poziom2 != '' THEN sk.Poziom1 || ' / ' || sk.Poziom2
                        ELSE sk.Poziom1
                    END
                FROM Slownik_Kategorii sk 
                WHERE sk.Kod = ZOiS.S_12_1
            )
            WHERE S_12_1 IS NOT NULL;
            """
            conn.execute(sql)
            conn.commit()
            logger.info("ZOiS.TypKonta updated successfully.")
        except Exception as e:
            logger.error(f"Error updating ZOiS.TypKonta: {e}")
            # Non-critical, let it pass

    def _update_zois_analytical_status(self):
        conn = db_service.get_connection()
        try:
            sql = """
            UPDATE ZOiS 
            SET IsAnalytical = 1 
            WHERE S_1 NOT IN (
                SELECT DISTINCT S_3 
                FROM ZOiS 
                WHERE S_3 IS NOT NULL AND S_3 != ''
            );
            """
            conn.execute(sql)
            conn.commit()
            logger.info("ZOiS IsAnalytical status updated successfully.")
        except Exception as e:
            logger.error(f"Error updating ZOiS IsAnalytical status: {e}")
    
etl_service = ETLService()
