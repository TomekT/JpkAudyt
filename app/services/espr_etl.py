import logging
from lxml import etree

logger = logging.getLogger(__name__)

class EsprEtlService:
    def __init__(self):
        pass

    def _get_amount(self, tree, element_name: str, amount_type: str) -> float:
        """
        Pomocnicza funkcja do wyciągania kwot z uwzględnieniem ignorowania przestrzeni nazw.
        Szuka elementu o nazwie `element_name`, a następnie wewnątrz niego tagu `amount_type` (np. 'KwotaA' lub 'KwotaB').
        Jeśli elementu nie ma lub nie ma w nim kwoty, zwraca 0.00.
        """
        xpath_query = f"//*[local-name()='{element_name}']/*[local-name()='{amount_type}']"
        elements = tree.xpath(xpath_query)
        if elements and elements[0].text:
            try:
                return float(elements[0].text.strip())
            except ValueError:
                return 0.00
        return 0.00

    def import_jednostka_inna(self, xml_path: str, db_conn):
        logger.info(f"Rozpoczynanie importu e-Sprawozdania dla Jednostki Innej z pliku: {xml_path}")
        try:
            tree = etree.parse(xml_path)
        except Exception as e:
            logger.error(f"Błąd podczas parsowania pliku XML: {e}")
            raise

        # 4. Jawna inicjalizacja zmiennych
        logger.info("Pobieranie wartości z pliku XML (tagi KwotaA / KwotaB)...")
        
        # Aktywa_A_II
        aktywa_A_II_rb = self._get_amount(tree, 'Aktywa_A_II', 'KwotaA')
        aktywa_A_II_rp = self._get_amount(tree, 'Aktywa_A_II', 'KwotaB')
        aktywa_A_II_2_rb = self._get_amount(tree, 'Aktywa_A_II_2', 'KwotaA')
        aktywa_A_II_2_rp = self._get_amount(tree, 'Aktywa_A_II_2', 'KwotaB')

        # Aktywa_A_IV
        aktywa_A_IV_rb = self._get_amount(tree, 'Aktywa_A_IV', 'KwotaA')
        aktywa_A_IV_rp = self._get_amount(tree, 'Aktywa_A_IV', 'KwotaB')
        aktywa_A_IV_1_rb = self._get_amount(tree, 'Aktywa_A_IV_1', 'KwotaA')
        aktywa_A_IV_1_rp = self._get_amount(tree, 'Aktywa_A_IV_1', 'KwotaB')
        aktywa_A_IV_2_rb = self._get_amount(tree, 'Aktywa_A_IV_2', 'KwotaA')
        aktywa_A_IV_2_rp = self._get_amount(tree, 'Aktywa_A_IV_2', 'KwotaB')

        # Aktywa_A_V
        aktywa_A_V_rb = self._get_amount(tree, 'Aktywa_A_V', 'KwotaA')
        aktywa_A_V_rp = self._get_amount(tree, 'Aktywa_A_V', 'KwotaB')
        aktywa_A_V_1_rb = self._get_amount(tree, 'Aktywa_A_V_1', 'KwotaA')
        aktywa_A_V_1_rp = self._get_amount(tree, 'Aktywa_A_V_1', 'KwotaB')

        # Aktywa_B_II
        aktywa_B_II_rb = self._get_amount(tree, 'Aktywa_B_II', 'KwotaA')
        aktywa_B_II_rp = self._get_amount(tree, 'Aktywa_B_II', 'KwotaB')
        aktywa_B_II_1_A_rb = self._get_amount(tree, 'Aktywa_B_II_1_A', 'KwotaA')
        aktywa_B_II_1_A_rp = self._get_amount(tree, 'Aktywa_B_II_1_A', 'KwotaB')
        aktywa_B_II_2_A_rb = self._get_amount(tree, 'Aktywa_B_II_2_A', 'KwotaA')
        aktywa_B_II_2_A_rp = self._get_amount(tree, 'Aktywa_B_II_2_A', 'KwotaB')
        aktywa_B_II_3_A_rb = self._get_amount(tree, 'Aktywa_B_II_3_A', 'KwotaA')
        aktywa_B_II_3_A_rp = self._get_amount(tree, 'Aktywa_B_II_3_A', 'KwotaB')
        aktywa_B_II_3_B_rb = self._get_amount(tree, 'Aktywa_B_II_3_B', 'KwotaA')
        aktywa_B_II_3_B_rp = self._get_amount(tree, 'Aktywa_B_II_3_B', 'KwotaB')

        # Aktywa_B_III
        aktywa_B_III_rb = self._get_amount(tree, 'Aktywa_B_III', 'KwotaA')
        aktywa_B_III_rp = self._get_amount(tree, 'Aktywa_B_III', 'KwotaB')
        aktywa_B_III_1_C_rb = self._get_amount(tree, 'Aktywa_B_III_1_C', 'KwotaA')
        aktywa_B_III_1_C_rp = self._get_amount(tree, 'Aktywa_B_III_1_C', 'KwotaB')

        # Pasywa_B_I
        pasywa_B_I_rb = self._get_amount(tree, 'Pasywa_B_I', 'KwotaA')
        pasywa_B_I_rp = self._get_amount(tree, 'Pasywa_B_I', 'KwotaB')
        pasywa_B_I_1_rb = self._get_amount(tree, 'Pasywa_B_I_1', 'KwotaA')
        pasywa_B_I_1_rp = self._get_amount(tree, 'Pasywa_B_I_1', 'KwotaB')
        pasywa_B_I_2_rb = self._get_amount(tree, 'Pasywa_B_I_2', 'KwotaA')
        pasywa_B_I_2_rp = self._get_amount(tree, 'Pasywa_B_I_2', 'KwotaB')

        # Pasywa_B_II
        pasywa_B_II_rb = self._get_amount(tree, 'Pasywa_B_II', 'KwotaA')
        pasywa_B_II_rp = self._get_amount(tree, 'Pasywa_B_II', 'KwotaB')
        pasywa_B_II_3_A_rb = self._get_amount(tree, 'Pasywa_B_II_3_A', 'KwotaA')
        pasywa_B_II_3_A_rp = self._get_amount(tree, 'Pasywa_B_II_3_A', 'KwotaB')

        # Pasywa_B_III
        pasywa_B_III_rb = self._get_amount(tree, 'Pasywa_B_III', 'KwotaA')
        pasywa_B_III_rp = self._get_amount(tree, 'Pasywa_B_III', 'KwotaB')
        pasywa_B_III_1_A_rb = self._get_amount(tree, 'Pasywa_B_III_1_A', 'KwotaA')
        pasywa_B_III_1_A_rp = self._get_amount(tree, 'Pasywa_B_III_1_A', 'KwotaB')
        pasywa_B_III_2_A_rb = self._get_amount(tree, 'Pasywa_B_III_2_A', 'KwotaA')
        pasywa_B_III_2_A_rp = self._get_amount(tree, 'Pasywa_B_III_2_A', 'KwotaB')
        pasywa_B_III_3_D_rb = self._get_amount(tree, 'Pasywa_B_III_3_D', 'KwotaA')
        pasywa_B_III_3_D_rp = self._get_amount(tree, 'Pasywa_B_III_3_D', 'KwotaB')
        pasywa_B_III_3_G_rb = self._get_amount(tree, 'Pasywa_B_III_3_G', 'KwotaA')
        pasywa_B_III_3_G_rp = self._get_amount(tree, 'Pasywa_B_III_3_G', 'KwotaB')
        pasywa_B_III_3_A_rb = self._get_amount(tree, 'Pasywa_B_III_3_A', 'KwotaA')
        pasywa_B_III_3_A_rp = self._get_amount(tree, 'Pasywa_B_III_3_A', 'KwotaB')

        # 5. Jawny blok obliczeniowy dla pól rezydualnych (_Inne)
        logger.info("Obliczanie pól rezydualnych (_Inne)...")
        
        aktywa_A_II_Inne_rb = aktywa_A_II_rb - aktywa_A_II_2_rb
        aktywa_A_II_Inne_rp = aktywa_A_II_rp - aktywa_A_II_2_rp

        aktywa_A_IV_Inne_rb = aktywa_A_IV_rb - (aktywa_A_IV_1_rb + aktywa_A_IV_2_rb)
        aktywa_A_IV_Inne_rp = aktywa_A_IV_rp - (aktywa_A_IV_1_rp + aktywa_A_IV_2_rp)

        aktywa_A_V_Inne_rb = aktywa_A_V_rb - aktywa_A_V_1_rb
        aktywa_A_V_Inne_rp = aktywa_A_V_rp - aktywa_A_V_1_rp

        aktywa_B_II_Inne_rb = aktywa_B_II_rb - (aktywa_B_II_1_A_rb + aktywa_B_II_2_A_rb + aktywa_B_II_3_A_rb + aktywa_B_II_3_B_rb)
        aktywa_B_II_Inne_rp = aktywa_B_II_rp - (aktywa_B_II_1_A_rp + aktywa_B_II_2_A_rp + aktywa_B_II_3_A_rp + aktywa_B_II_3_B_rp)

        aktywa_B_III_Inne_rb = aktywa_B_III_rb - aktywa_B_III_1_C_rb
        aktywa_B_III_Inne_rp = aktywa_B_III_rp - aktywa_B_III_1_C_rp

        pasywa_B_I_Inne_rb = pasywa_B_I_rb - (pasywa_B_I_1_rb + pasywa_B_I_2_rb)
        pasywa_B_I_Inne_rp = pasywa_B_I_rp - (pasywa_B_I_1_rp + pasywa_B_I_2_rp)

        pasywa_B_II_Inne_rb = pasywa_B_II_rb - pasywa_B_II_3_A_rb
        pasywa_B_II_Inne_rp = pasywa_B_II_rp - pasywa_B_II_3_A_rp

        pasywa_B_III_Inne_rb = pasywa_B_III_rb - (pasywa_B_III_1_A_rb + pasywa_B_III_2_A_rb + pasywa_B_III_3_D_rb + pasywa_B_III_3_G_rb + pasywa_B_III_3_A_rb)
        pasywa_B_III_Inne_rp = pasywa_B_III_rp - (pasywa_B_III_1_A_rp + pasywa_B_III_2_A_rp + pasywa_B_III_3_D_rp + pasywa_B_III_3_G_rp + pasywa_B_III_3_A_rp)

        logger.info(f"Wyliczono Aktywa_A_II_Inne: RB={aktywa_A_II_Inne_rb}, RP={aktywa_A_II_Inne_rp}")
        logger.info(f"Wyliczono Aktywa_A_IV_Inne: RB={aktywa_A_IV_Inne_rb}, RP={aktywa_A_IV_Inne_rp}")
        logger.info(f"Wyliczono Aktywa_A_V_Inne: RB={aktywa_A_V_Inne_rb}, RP={aktywa_A_V_Inne_rp}")
        logger.info(f"Wyliczono Aktywa_B_II_Inne: RB={aktywa_B_II_Inne_rb}, RP={aktywa_B_II_Inne_rp}")
        logger.info(f"Wyliczono Aktywa_B_III_Inne: RB={aktywa_B_III_Inne_rb}, RP={aktywa_B_III_Inne_rp}")
        logger.info(f"Wyliczono Pasywa_B_I_Inne: RB={pasywa_B_I_Inne_rb}, RP={pasywa_B_I_Inne_rp}")
        logger.info(f"Wyliczono Pasywa_B_II_Inne: RB={pasywa_B_II_Inne_rb}, RP={pasywa_B_II_Inne_rp}")
        logger.info(f"Wyliczono Pasywa_B_III_Inne: RB={pasywa_B_III_Inne_rb}, RP={pasywa_B_III_Inne_rp}")

        # Słownik wszystkich wyciągniętych i wyliczonych wartości do zapisu
        dane_do_bazy = {
            'Aktywa_A_II': (aktywa_A_II_rb, aktywa_A_II_rp),
            'Aktywa_A_II_2': (aktywa_A_II_2_rb, aktywa_A_II_2_rp),
            'Aktywa_A_IV': (aktywa_A_IV_rb, aktywa_A_IV_rp),
            'Aktywa_A_IV_1': (aktywa_A_IV_1_rb, aktywa_A_IV_1_rp),
            'Aktywa_A_IV_2': (aktywa_A_IV_2_rb, aktywa_A_IV_2_rp),
            'Aktywa_A_V': (aktywa_A_V_rb, aktywa_A_V_rp),
            'Aktywa_A_V_1': (aktywa_A_V_1_rb, aktywa_A_V_1_rp),
            'Aktywa_B_II': (aktywa_B_II_rb, aktywa_B_II_rp),
            'Aktywa_B_II_1_A': (aktywa_B_II_1_A_rb, aktywa_B_II_1_A_rp),
            'Aktywa_B_II_2_A': (aktywa_B_II_2_A_rb, aktywa_B_II_2_A_rp),
            'Aktywa_B_II_3_A': (aktywa_B_II_3_A_rb, aktywa_B_II_3_A_rp),
            'Aktywa_B_II_3_B': (aktywa_B_II_3_B_rb, aktywa_B_II_3_B_rp),
            'Aktywa_B_III': (aktywa_B_III_rb, aktywa_B_III_rp),
            'Aktywa_B_III_1_C': (aktywa_B_III_1_C_rb, aktywa_B_III_1_C_rp),
            'Pasywa_B_I': (pasywa_B_I_rb, pasywa_B_I_rp),
            'Pasywa_B_I_1': (pasywa_B_I_1_rb, pasywa_B_I_1_rp),
            'Pasywa_B_I_2': (pasywa_B_I_2_rb, pasywa_B_I_2_rp),
            'Pasywa_B_II': (pasywa_B_II_rb, pasywa_B_II_rp),
            'Pasywa_B_II_3_A': (pasywa_B_II_3_A_rb, pasywa_B_II_3_A_rp),
            'Pasywa_B_III': (pasywa_B_III_rb, pasywa_B_III_rp),
            'Pasywa_B_III_1_A': (pasywa_B_III_1_A_rb, pasywa_B_III_1_A_rp),
            'Pasywa_B_III_2_A': (pasywa_B_III_2_A_rb, pasywa_B_III_2_A_rp),
            'Pasywa_B_III_3_D': (pasywa_B_III_3_D_rb, pasywa_B_III_3_D_rp),
            'Pasywa_B_III_3_G': (pasywa_B_III_3_G_rb, pasywa_B_III_3_G_rp),
            'Pasywa_B_III_3_A': (pasywa_B_III_3_A_rb, pasywa_B_III_3_A_rp),
            
            # Pola wyliczone (_Inne)
            'Aktywa_A_II_Inne': (aktywa_A_II_Inne_rb, aktywa_A_II_Inne_rp),
            'Aktywa_A_IV_Inne': (aktywa_A_IV_Inne_rb, aktywa_A_IV_Inne_rp),
            'Aktywa_A_V_Inne': (aktywa_A_V_Inne_rb, aktywa_A_V_Inne_rp),
            'Aktywa_B_II_Inne': (aktywa_B_II_Inne_rb, aktywa_B_II_Inne_rp),
            'Aktywa_B_III_Inne': (aktywa_B_III_Inne_rb, aktywa_B_III_Inne_rp),
            'Pasywa_B_I_Inne': (pasywa_B_I_Inne_rb, pasywa_B_I_Inne_rp),
            'Pasywa_B_II_Inne': (pasywa_B_II_Inne_rb, pasywa_B_II_Inne_rp),
            'Pasywa_B_III_Inne': (pasywa_B_III_Inne_rb, pasywa_B_III_Inne_rp)
        }

        # 6. Zapis do bazy danych
        logger.info("Rozpoczynanie zapisu do bazy danych...")
        cursor = db_conn.cursor()
        
        try:
            update_query = """
                UPDATE Sprawozdanie_Pozycje 
                SET Kwota_RB = ?, Kwota_RP = ? 
                WHERE XmlTag = ?
            """
            
            # Przygotowanie danych do executemany
            params = []
            for tag, (rb, rp) in dane_do_bazy.items():
                params.append((rb, rp, tag))
            
            logger.info(f"Wykonywanie zapytań UPDATE dla {len(params)} tagów...")
            cursor.executemany(update_query, params)
            
            db_conn.commit()
            logger.info("Pomyślnie zaktualizowano wartości w bazie danych (commit transakcji).")
            
        except Exception as e:
            db_conn.rollback()
            logger.error(f"Błąd podczas zapisu do bazy danych, wycofano transakcję (rollback): {e}")
            raise

    def import_jednostka_mala(self, xml_path: str, db_conn):
        logger.info(f"Rozpoczynanie importu e-Sprawozdania dla Jednostki Małej z pliku: {xml_path}")
        try:
            tree = etree.parse(xml_path)
        except Exception as e:
            logger.error(f"Błąd podczas parsowania pliku XML: {e}")
            raise

        logger.info("Pobieranie wartości z pliku XML (tagi KwotaA / KwotaB)...")
        
        # --- BILANS ---
        aktywa_B_II_A_rb = self._get_amount(tree, 'Aktywa_B_II_A', 'KwotaA')
        aktywa_B_II_A_rp = self._get_amount(tree, 'Aktywa_B_II_A', 'KwotaB')

        aktywa_B_III_A_1_rb = self._get_amount(tree, 'Aktywa_B_III_A_1', 'KwotaA')
        aktywa_B_III_A_1_rp = self._get_amount(tree, 'Aktywa_B_III_A_1', 'KwotaB')

        pasywa_B_II_1_rb = self._get_amount(tree, 'Pasywa_B_II_1', 'KwotaA')
        pasywa_B_II_1_rp = self._get_amount(tree, 'Pasywa_B_II_1', 'KwotaB')

        pasywa_B_III_A_rb = self._get_amount(tree, 'Pasywa_B_III_A', 'KwotaA')
        pasywa_B_III_A_rp = self._get_amount(tree, 'Pasywa_B_III_A', 'KwotaB')

        pasywa_B_III_B_rb = self._get_amount(tree, 'Pasywa_B_III_B', 'KwotaA')
        pasywa_B_III_B_rp = self._get_amount(tree, 'Pasywa_B_III_B', 'KwotaB')

        # --- RZiS Porównawczy ---
        # Obsługa kluczowego wyłomu semantycznego - wyciągamy tagi wg wzorca dla małej jednostki
        rzispor_b_iv_rb = self._get_amount(tree, 'B_IV', 'KwotaA')
        rzispor_b_iv_rp = self._get_amount(tree, 'B_IV', 'KwotaB')

        rzispor_b_v_rb = self._get_amount(tree, 'B_V', 'KwotaA')
        rzispor_b_v_rp = self._get_amount(tree, 'B_V', 'KwotaB')

        rzispor_b_vi_rb = self._get_amount(tree, 'B_VI', 'KwotaA')
        rzispor_b_vi_rp = self._get_amount(tree, 'B_VI', 'KwotaB')

        rzispor_f_rb = self._get_amount(tree, 'F', 'KwotaA')
        rzispor_f_rp = self._get_amount(tree, 'F', 'KwotaB')

        rzispor_g_rb = self._get_amount(tree, 'G', 'KwotaA')
        rzispor_g_rp = self._get_amount(tree, 'G', 'KwotaB')

        rzispor_i_rb = self._get_amount(tree, 'I', 'KwotaA')
        rzispor_i_rp = self._get_amount(tree, 'I', 'KwotaB')

        # --- RZiS Kalkulacyjny ---
        rziskalk_c_rb = self._get_amount(tree, 'C', 'KwotaA')
        rziskalk_c_rp = self._get_amount(tree, 'C', 'KwotaB')

        rziskalk_d_rb = self._get_amount(tree, 'D', 'KwotaA')
        rziskalk_d_rp = self._get_amount(tree, 'D', 'KwotaB')

        rziskalk_f_rb = self._get_amount(tree, 'F', 'KwotaA')
        rziskalk_f_rp = self._get_amount(tree, 'F', 'KwotaB')

        rziskalk_g_rb = self._get_amount(tree, 'G', 'KwotaA')
        rziskalk_g_rp = self._get_amount(tree, 'G', 'KwotaB')

        rziskalk_h_rb = self._get_amount(tree, 'H', 'KwotaA')
        rziskalk_h_rp = self._get_amount(tree, 'H', 'KwotaB')

        rziskalk_i_rb = self._get_amount(tree, 'I', 'KwotaA')
        rziskalk_i_rp = self._get_amount(tree, 'I', 'KwotaB')

        rziskalk_k_rb = self._get_amount(tree, 'K', 'KwotaA')
        rziskalk_k_rp = self._get_amount(tree, 'K', 'KwotaB')

        # Słownik do bazy danych ze specyficznymi aliasami .Mala
        dane_do_bazy = {
            # Bilans
            'Aktywa_B_II_A': (aktywa_B_II_A_rb, aktywa_B_II_A_rp),
            'Aktywa_B_III_A_1': (aktywa_B_III_A_1_rb, aktywa_B_III_A_1_rp),
            'Pasywa_B_II_1': (pasywa_B_II_1_rb, pasywa_B_II_1_rp),
            'Pasywa_B_III_A': (pasywa_B_III_A_rb, pasywa_B_III_A_rp),
            'Pasywa_B_III_B': (pasywa_B_III_B_rb, pasywa_B_III_B_rp),

            # RZiS Porównawczy
            'RZiSPor.B_IV.Mala': (rzispor_b_iv_rb, rzispor_b_iv_rp),
            'RZiSPor.B_V.Mala': (rzispor_b_v_rb, rzispor_b_v_rp),
            'RZiSPor.B_VI.Mala': (rzispor_b_vi_rb, rzispor_b_vi_rp),
            'RZiSPor.F.Mala': (rzispor_f_rb, rzispor_f_rp),
            'RZiSPor.G.Mala': (rzispor_g_rb, rzispor_g_rp),
            'RZiSPor.I.Mala': (rzispor_i_rb, rzispor_i_rp),

            # RZiS Kalkulacyjny
            'RZiSKalk.C.Mala': (rziskalk_c_rb, rziskalk_c_rp),
            'RZiSKalk.D.Mala': (rziskalk_d_rb, rziskalk_d_rp),
            'RZiSKalk.F.Mala': (rziskalk_f_rb, rziskalk_f_rp),
            'RZiSKalk.G.Mala': (rziskalk_g_rb, rziskalk_g_rp),
            'RZiSKalk.H.Mala': (rziskalk_h_rb, rziskalk_h_rp),
            'RZiSKalk.I.Mala': (rziskalk_i_rb, rziskalk_i_rp),
            'RZiSKalk.K.Mala': (rziskalk_k_rb, rziskalk_k_rp)
        }

        # 6. Zapis do bazy danych
        logger.info("Rozpoczynanie zapisu do bazy danych dla Jednostki Małej...")
        cursor = db_conn.cursor()
        
        try:
            update_query = """
                UPDATE Sprawozdanie_Pozycje 
                SET Kwota_RB = ?, Kwota_RP = ? 
                WHERE XmlTag = ?
            """
            
            # Przygotowanie danych do executemany
            params = []
            for tag, (rb, rp) in dane_do_bazy.items():
                params.append((rb, rp, tag))
            
            logger.info(f"Wykonywanie zapytań UPDATE dla {len(params)} tagów jednostki małej...")
            cursor.executemany(update_query, params)
            
            db_conn.commit()
            logger.info("Pomyślnie zaktualizowano wartości w bazie danych dla Jednostki Małej (commit transakcji).")
            
        except Exception as e:
            db_conn.rollback()
            logger.error(f"Błąd podczas zapisu do bazy danych dla Jednostki Małej, wycofano transakcję (rollback): {e}")
            raise
