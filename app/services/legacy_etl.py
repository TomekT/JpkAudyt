import logging
from pathlib import Path
from lxml import etree
import os
import time

from app.services.database import db_service
from app.core.path_utils import resource_path

logger = logging.getLogger(__name__)

class LegacyETLService:
    BATCH_SIZE = 5000
    NAMESPACE = "http://jpk.mf.gov.pl/wzor/2016/03/09/03091/"

    def import_jpk(self, xml_path: str, progress_callback=None) -> str:
        """
        Imports an old format JPK_KR XML file.
        Returns the path to the created database.
        """
        xml_file = Path(xml_path)
        if not xml_file.exists():
            raise FileNotFoundError(f"XML file not found: {xml_path}")

        logger.info(f"Starting import of old format JPK_KR: {xml_path}")
        if progress_callback: progress_callback("Pobieranie metadanych (Legacy)...", 20)
        metadata = self._extract_metadata(xml_path)
        
        if progress_callback: progress_callback("Inicjalizacja struktury bazy danych...", 25)
        db_path = self._init_database(xml_file, metadata)
        
        # Sprawdzenie czy baza już istnieje i zawiera dane przed procesowaniem
        if os.path.exists(db_path) and db_service.is_database_populated(str(db_path)):
            raise ValueError("Baza danych dla tego pliku JPK już istnieje i zawiera dane. Import został przerwany.")

        # Connect to newly created local SQLite file for this JPK
        db_service.connect(str(db_path))
        
        if progress_callback: progress_callback("Przetwarzanie strumieniowe XML (Legacy)...", 30)
        self._process_xml(xml_path, progress_callback)
        
        # Post-process (analytic status, type of account, and headers)
        if progress_callback: progress_callback("Optymalizacja statusów analitycznych...", 85)
        self._update_zois_analytical_status()
        
        if progress_callback: progress_callback("Zapisywanie metadanych podmiotu...", 90)
        self._insert_header_data(metadata)
        
        if progress_callback: progress_callback("Mapowanie typów kont ze słownika...", 95)
        self._update_zois_typ_konta()
        
        db_service.update_zois_mapping_cache()

        if progress_callback: progress_callback("Import zakończony!", 100)
        logger.info("Legacy JPK_KR import completed successfully.")
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
            for event, elem in etree.iterparse(xml_path, events=('end',)):
                tag = etree.QName(elem).localname
                
                if tag in target_tags:
                    if tag not in metadata:
                        metadata[tag] = elem.text
                
                if tag in ['ZOiS', 'Dziennik', 'KontoZapis', 'Kontrahent']:
                    break
        except etree.XMLSyntaxError:
             raise ValueError("Błąd składni XML")
             
        return metadata

    def _init_database(self, xml_file: Path, metadata: dict) -> Path:
        nip = metadata.get('NIP', 'UNKNOWN')
        year = "YEAR"
        if 'DataDo' in metadata and metadata['DataDo']:
            year = metadata['DataDo'][:4]
        
        date_created = "DATE"
        if 'DataWytworzeniaJPK' in metadata and metadata['DataWytworzeniaJPK']:
            date_created = metadata['DataWytworzeniaJPK'][:10].replace('-', '')
            
        db_filename = f"{nip}-{year}-{date_created}_legacy.db"
        db_path = xml_file.parent / db_filename
        
        schema_path = resource_path("schema.sql")
        db_service.init_db(str(db_path), str(schema_path))
        
        db_service.connect(str(db_path))
        
        slownik_path = resource_path("insert_slownik.sql")
        if slownik_path.exists():
            db_service.execute_script_file(str(slownik_path))
            
        obszary_sql_path = resource_path("insert_obszary.sql")
        if obszary_sql_path.exists():
            db_service.execute_script_file(str(obszary_sql_path))
            
        return db_path

    def _insert_header_data(self, metadata):
        conn = db_service.get_connection()
        try:
            conn.execute(
                "INSERT INTO Naglowek (KodFormularza, DataWytworzeniaJPK, DataOd, DataDo) VALUES (?, ?, ?, ?)",
                (metadata.get('KodFormularza', 'JPK_KR'), metadata.get('DataWytworzeniaJPK'), metadata.get('DataOd'), metadata.get('DataDo'))
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

    def _process_xml(self, xml_path: str, progress_callback=None):
        logger.info(f"Rozpoczynanie parsowania strumieniowego XML: {xml_path}")
        conn = db_service.get_connection()
        cursor = conn.cursor()
        
        ns_prefix = f"{{{self.NAMESPACE}}}"
        dziennik_tag = f"{ns_prefix}Dziennik"
        konto_zapis_tag = f"{ns_prefix}KontoZapis"
        zois_tag = f"{ns_prefix}ZOiS"
        
        # Mapping control tags
        ctrl_dziennik_tag = f"{ns_prefix}DziennikCtrl"
        ctrl_zapisy_tag = f"{ns_prefix}KontoZapisCtrl"

        dziennik_buffer = []
        zapisy_buffer = []
        zois_buffer = []
        ctrl_data = {"C_1": 0, "C_2": 0, "C_3": 0, "C_4": 0, "C_5": 0}

        insert_dziennik_sql = """
            INSERT INTO Dziennik (D_1, D_2, D_4, D_5, D_6, D_7, D_8, D_9, D_10, D_11)
            VALUES (:D_1, :D_2, :D_4, :D_5, :D_6, :D_7, :D_8, :D_9, :D_10, :D_11)
        """

        insert_zapisy_sql = """
            INSERT INTO Zapisy (Z_NrZapisu, Z_1, Z_2, Z_3, Z_4, Z_5, Z_6, Z_7, Z_8, Z_9, Z_Data, Z_GrupaKont)
            VALUES (:Z_NrZapisu, :Z_1, :Z_2, :Z_3, :Z_4, :Z_5, :Z_6, :Z_7, :Z_8, :Z_9, :Z_Data, :Z_GrupaKont)
        """

        insert_zois_sql = """
             INSERT OR REPLACE INTO ZOiS (S_1, S_2, S_3, S_4, S_5, S_6, S_7, S_8, S_9, S_10, S_11, TypKonta)
             VALUES (:S_1, :S_2, :S_3, :S_4, :S_5, :S_6, :S_7, :S_8, :S_9, :S_10, :S_11, :TypKonta)
        """

        context = etree.iterparse(
            xml_path, 
            events=('end',), 
            tag=(dziennik_tag, konto_zapis_tag, zois_tag, ctrl_dziennik_tag, ctrl_zapisy_tag)
        )

        # Track counts for progress
        total_records = 0
        last_report_time = time.time()

        for event, elem in context:
            local_tag = etree.QName(elem).localname
            
            total_records += 1
            if progress_callback and total_records % 1000 == 0:
                now = time.time()
                if now - last_report_time > 1.0:
                    pct = min(80, 30 + (total_records // 3000))
                    progress_callback(f"Wczytywanie rekordów Legacy: {total_records}...", pct)
                    last_report_time = now
            
            if local_tag == 'Dziennik':
                def get_stripped(tag_name):
                    val = elem.findtext(f"{ns_prefix}{tag_name}", default=None)
                    return val.strip() if val else None

                dziennik_buffer.append({
                    'D_1': get_stripped("NrZapisuDziennika"),
                    'D_2': elem.findtext(f"{ns_prefix}OpisDziennika", default=None),
                    'D_4': elem.findtext(f"{ns_prefix}NrDowoduKsiegowego", default=None),
                    'D_5': elem.findtext(f"{ns_prefix}RodzajDowodu", default=None),
                    'D_6': elem.findtext(f"{ns_prefix}DataOperacji", default=None),
                    'D_7': elem.findtext(f"{ns_prefix}DataDowodu", default=None),
                    'D_8': elem.findtext(f"{ns_prefix}DataKsiegowania", default=None),
                    'D_9': elem.findtext(f"{ns_prefix}KodOperatora", default=None),
                    'D_10': elem.findtext(f"{ns_prefix}OpisOperacji", default=None),
                    'D_11': elem.findtext(f"{ns_prefix}DziennikKwotaOperacji", default=None)
                })

                if len(dziennik_buffer) >= self.BATCH_SIZE:
                    cursor.executemany(insert_dziennik_sql, dziennik_buffer)
                    dziennik_buffer.clear()

            elif local_tag == 'KontoZapis':
                # Legacy JPK_KR <KontoZapis> maps to Zapisy table
                # Z_1: LpZapisu, Z_2: Opis, Z_3: KodKonta
                lp_zapisu = elem.findtext(f"{ns_prefix}LpZapisu", default="")
                if lp_zapisu: lp_zapisu = lp_zapisu.strip()
                
                raw_nr = elem.findtext(f"{ns_prefix}NrZapisu", default="")
                nr_zapisu = raw_nr.strip() if raw_nr else "" # Reference to Dziennik (D_1)
                
                # WINIEN side (Z_4)
                kwota_wn_str = elem.findtext(f"{ns_prefix}KwotaWinien", default="0")
                kod_konta_wn = elem.findtext(f"{ns_prefix}KodKontaWinien", default="")
                if kod_konta_wn: kod_konta_wn = kod_konta_wn.strip()
                
                kwota_wn = float(kwota_wn_str) if kwota_wn_str else 0.0
                
                if kod_konta_wn and kod_konta_wn != "-" and kwota_wn != 0:
                    zapisy_buffer.append({
                        'Z_NrZapisu': nr_zapisu,
                        'Z_1': lp_zapisu,
                        'Z_2': (elem.findtext(f"{ns_prefix}OpisZapisuWinien", default="") or "").strip(),
                        'Z_3': kod_konta_wn,
                        'Z_4': kwota_wn,
                        'Z_5': elem.findtext(f"{ns_prefix}KwotaWinienWaluta", default=None),
                        'Z_6': elem.findtext(f"{ns_prefix}KodWalutyWinien", default=None),
                        'Z_7': 0.0,
                        'Z_8': None,
                        'Z_9': None,
                        'Z_Data': None,
                        'Z_GrupaKont': kod_konta_wn[:3] if kod_konta_wn else None
                    })

                # MA side (Z_7)
                kwota_ma_str = elem.findtext(f"{ns_prefix}KwotaMa", default="0")
                kod_konta_ma = elem.findtext(f"{ns_prefix}KodKontaMa", default="")
                if kod_konta_ma: kod_konta_ma = kod_konta_ma.strip()
                
                kwota_ma = float(kwota_ma_str) if kwota_ma_str else 0.0
                
                if kod_konta_ma and kod_konta_ma != "-" and kwota_ma != 0:
                    zapisy_buffer.append({
                        'Z_NrZapisu': nr_zapisu,
                        'Z_1': lp_zapisu,
                        'Z_2': (elem.findtext(f"{ns_prefix}OpisZapisuMa", default="") or "").strip(),
                        'Z_3': kod_konta_ma,
                        'Z_4': 0.0,
                        'Z_5': None,
                        'Z_6': None,
                        'Z_7': kwota_ma,
                        'Z_8': elem.findtext(f"{ns_prefix}KwotaMaWaluta", default=None),
                        'Z_9': elem.findtext(f"{ns_prefix}KodWalutyMa", default=None),
                        'Z_Data': None,
                        'Z_GrupaKont': kod_konta_ma[:3] if kod_konta_ma else None
                    })

                if len(zapisy_buffer) >= self.BATCH_SIZE:
                    cursor.executemany(insert_zapisy_sql, zapisy_buffer)
                    zapisy_buffer.clear()
                    
            elif local_tag == 'ZOiS':
                 kod_konta = elem.findtext(f"{ns_prefix}KodKonta", default="")
                 zois_buffer.append({
                     'S_1': kod_konta,
                     'S_2': elem.findtext(f"{ns_prefix}OpisKonta", default=None),
                     'S_3': kod_konta[:3] if kod_konta else None,
                     'S_4': elem.findtext(f"{ns_prefix}BilansOtwarciaWinien", default=None),
                     'S_5': elem.findtext(f"{ns_prefix}BilansOtwarciaMa", default=None),
                     'S_6': elem.findtext(f"{ns_prefix}ObrotyWinien", default=None),
                     'S_7': elem.findtext(f"{ns_prefix}ObrotyMa", default=None),
                     'S_8': elem.findtext(f"{ns_prefix}ObrotyWinienNarast", default=None),
                     'S_9': elem.findtext(f"{ns_prefix}ObrotyMaNarast", default=None),
                     'S_10': elem.findtext(f"{ns_prefix}SaldoWinien", default=None),
                     'S_11': elem.findtext(f"{ns_prefix}SaldoMa", default=None),
                     'TypKonta': elem.findtext(f"{ns_prefix}TypKonta", default=None)
                 })
                 
                 if len(zois_buffer) >= self.BATCH_SIZE:
                     cursor.executemany(insert_zois_sql, zois_buffer)
                     zois_buffer.clear()

            elif local_tag == 'DziennikCtrl':
                ctrl_data["C_1"] = elem.findtext(f"{ns_prefix}LiczbaWierszyDziennika", default="0")
                ctrl_data["C_2"] = elem.findtext(f"{ns_prefix}SumaKwotOperacji", default="0")

            elif local_tag == 'KontoZapisCtrl':
                ctrl_data["C_3"] = elem.findtext(f"{ns_prefix}LiczbaWierszyKontoZapis", default="0")
                ctrl_data["C_4"] = elem.findtext(f"{ns_prefix}SumaWinien", default="0")
                ctrl_data["C_5"] = elem.findtext(f"{ns_prefix}SumaMa", default="0")

            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        # Flush remaining buffers
        if dziennik_buffer:
            cursor.executemany(insert_dziennik_sql, dziennik_buffer)
        if zapisy_buffer:
            cursor.executemany(insert_zapisy_sql, zapisy_buffer)
        if zois_buffer:
            cursor.executemany(insert_zois_sql, zois_buffer)

        # Insert Control data
        cursor.execute(
            "INSERT INTO Ctrl (C_1, C_2, C_3, C_4, C_5) VALUES (:C_1, :C_2, :C_3, :C_4, :C_5)",
            ctrl_data
        )

        # Create temporary indices for speed
        cursor.execute("CREATE INDEX IF NOT EXISTS temp_idx_dziennik_d1 ON Dziennik(D_1)")
        cursor.execute("CREATE INDEX IF NOT EXISTS temp_idx_zapisy_nr ON Zapisy(Z_NrZapisu)")

        # Update Z_Data and Z_DataMiesiac based on related Dziennik link (D_1 <-> Z_NrZapisu)
        logger.info("Łączenie Zapisów z Dziennikiem (klucze biznesowe) i aktualizacja dat...")
        cursor.execute("""
            UPDATE Zapisy
            SET Dziennik_Id = d.Id,
                Z_Data = d.D_8
            FROM Dziennik d
            WHERE d.D_1 = Zapisy.Z_NrZapisu
        """)
        
        cursor.execute("""
            UPDATE Zapisy
            SET Z_DataMiesiac = CAST(SUBSTR(Z_Data, 6, 2) AS INTEGER)
            WHERE Z_Data IS NOT NULL
        """)

        logger.info("Zakończono łączenie i aktualizację dat.")

        # Drop temporary indices
        cursor.execute("DROP INDEX IF EXISTS temp_idx_dziennik_d1")
        cursor.execute("DROP INDEX IF EXISTS temp_idx_zapisy_nr")

        conn.commit()

    def _update_zois_typ_konta(self):
        # We already populated TypKonta directly from XML during processing.
        pass

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
            logger.info("Legacy ZOiS IsAnalytical status updated successfully.")
        except Exception as e:
            logger.error(f"Error updating Legacy ZOiS IsAnalytical status: {e}")

legacy_etl_service = LegacyETLService()
