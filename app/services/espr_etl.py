import logging
from lxml import etree

logger = logging.getLogger(__name__)

class EsprEtlService:
    def __init__(self):
        pass

    def _get_amount(self, context_node, element_name: str, amount_type: str, parent_name: str = None) -> float:
        """
        Pobiera kwotę z XML z uwzględnieniem kontekstu rodzica (parent_name).
        Zapobiega konfliktom nazw tagów (np. <A>, <B>, <D>) występujących w różnych sekcjach RZiS.
        Jeśli parent_name jest podany, wyszukiwanie jest ograniczone do tej gałęzi.
        """
        if parent_name:
            # Wyszukiwanie z jawnym rodzicem w celu uniknięcia kolizji nazw
            xpath_query = f"//*[local-name()='{parent_name}']//*[local-name()='{element_name}']/*[local-name()='{amount_type}']"
        else:
            # Wyszukiwanie relatywne względem context_node
            xpath_query = f".//*[local-name()='{element_name}']/*[local-name()='{amount_type}']"
            
        elements = context_node.xpath(xpath_query)
        if elements and elements[0].text:
            try:
                return float(elements[0].text.strip())
            except (ValueError, TypeError):
                return 0.00
        return 0.00

    def import_jednostka_inna(self, xml_path: str, db_conn):
        logger.info(f"Rozpoczynanie importu e-Sprawozdania dla Jednostki Innej z pliku: {xml_path}")
        try:
            tree = etree.parse(xml_path)
        except Exception as e:
            logger.error(f"Błąd podczas parsowania pliku XML: {e}")
            raise

        logger.info("Pobieranie wartości z pliku XML...")
        
        # --- BILANS AKTYWA (Tagi unikalne) ---
        a_A_I_rb, a_A_I_rp = self._get_amount(tree, 'Aktywa_A_I', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_I', 'KwotaB')
        a_A_II_rb, a_A_II_rp = self._get_amount(tree, 'Aktywa_A_II', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_II', 'KwotaB')
        a_A_II_2_rb, a_A_II_2_rp = self._get_amount(tree, 'Aktywa_A_II_2', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_II_2', 'KwotaB')
        a_A_III_rb, a_A_III_rp = self._get_amount(tree, 'Aktywa_A_III', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_III', 'KwotaB')
        a_A_IV_rb, a_A_IV_rp = self._get_amount(tree, 'Aktywa_A_IV', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_IV', 'KwotaB')
        a_A_IV_1_rb, a_A_IV_1_rp = self._get_amount(tree, 'Aktywa_A_IV_1', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_IV_1', 'KwotaB')
        a_A_IV_2_rb, a_A_IV_2_rp = self._get_amount(tree, 'Aktywa_A_IV_2', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_IV_2', 'KwotaB')
        a_A_V_rb, a_A_V_rp = self._get_amount(tree, 'Aktywa_A_V', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_V', 'KwotaB')
        a_A_V_1_rb, a_A_V_1_rp = self._get_amount(tree, 'Aktywa_A_V_1', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_V_1', 'KwotaB')
        a_B_I_rb, a_B_I_rp = self._get_amount(tree, 'Aktywa_B_I', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_I', 'KwotaB')
        a_B_II_rb, a_B_II_rp = self._get_amount(tree, 'Aktywa_B_II', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II', 'KwotaB')
        a_B_II_1_A_rb, a_B_II_1_A_rp = self._get_amount(tree, 'Aktywa_B_II_1_A', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II_1_A', 'KwotaB')
        a_B_II_2_A_rb, a_B_II_2_A_rp = self._get_amount(tree, 'Aktywa_B_II_2_A', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II_2_A', 'KwotaB')
        a_B_II_3_A_rb, a_B_II_3_A_rp = self._get_amount(tree, 'Aktywa_B_II_3_A', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II_3_A', 'KwotaB')
        a_B_II_A_rb, a_B_II_A_rp = self._get_amount(tree, 'Aktywa_B_II_A', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II_A', 'KwotaB')
        a_B_II_3_B_rb, a_B_II_3_B_rp = self._get_amount(tree, 'Aktywa_B_II_3_B', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II_3_B', 'KwotaB')
        a_B_III_rb, a_B_III_rp = self._get_amount(tree, 'Aktywa_B_III', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_III', 'KwotaB')
        a_B_III_1_C_rb, a_B_III_1_C_rp = self._get_amount(tree, 'Aktywa_B_III_1_C', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_III_1_C', 'KwotaB')
        a_B_III_A_1_rb, a_B_III_A_1_rp = self._get_amount(tree, 'Aktywa_B_III_A_1', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_III_A_1', 'KwotaB')
        a_B_IV_rb, a_B_IV_rp = self._get_amount(tree, 'Aktywa_B_IV', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_IV', 'KwotaB')
        a_C_rb, a_C_rp = self._get_amount(tree, 'Aktywa_C', 'KwotaA'), self._get_amount(tree, 'Aktywa_C', 'KwotaB')
        a_D_rb, a_D_rp = self._get_amount(tree, 'Aktywa_D', 'KwotaA'), self._get_amount(tree, 'Aktywa_D', 'KwotaB')

        # --- BILANS PASYWA (Tagi unikalne) ---
        p_B_I_rb, p_B_I_rp = self._get_amount(tree, 'Pasywa_B_I', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_I', 'KwotaB')
        p_B_II_rb, p_B_II_rp = self._get_amount(tree, 'Pasywa_B_II', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_II', 'KwotaB')
        p_B_III_rb, p_B_III_rp = self._get_amount(tree, 'Pasywa_B_III', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III', 'KwotaB')
        p_A_I_rb, p_A_I_rp = self._get_amount(tree, 'Pasywa_A_I', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_I', 'KwotaB')
        p_A_II_rb, p_A_II_rp = self._get_amount(tree, 'Pasywa_A_II', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_II', 'KwotaB')
        p_A_III_rb, p_A_III_rp = self._get_amount(tree, 'Pasywa_A_III', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_III', 'KwotaB')
        p_A_IV_rb, p_A_IV_rp = self._get_amount(tree, 'Pasywa_A_IV', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_IV', 'KwotaB')
        p_A_V_rb, p_A_V_rp = self._get_amount(tree, 'Pasywa_A_V', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_V', 'KwotaB')
        p_A_VI_rb, p_A_VI_rp = self._get_amount(tree, 'Pasywa_A_VI', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_VI', 'KwotaB')
        p_A_VII_rb, p_A_VII_rp = self._get_amount(tree, 'Pasywa_A_VII', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_VII', 'KwotaB')
        p_B_I_1_rb, p_B_I_1_rp = self._get_amount(tree, 'Pasywa_B_I_1', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_I_1', 'KwotaB')
        p_B_I_2_rb, p_B_I_2_rp = self._get_amount(tree, 'Pasywa_B_I_2', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_I_2', 'KwotaB')
        p_B_II_1_rb, p_B_II_1_rp = self._get_amount(tree, 'Pasywa_B_II_1', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_II_1', 'KwotaB')
        p_B_II_3_A_rb, p_B_II_3_A_rp = self._get_amount(tree, 'Pasywa_B_II_3_A', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_II_3_A', 'KwotaB')
        p_B_III_1_A_rb, p_B_III_1_A_rp = self._get_amount(tree, 'Pasywa_B_III_1_A', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_1_A', 'KwotaB')
        p_B_III_2_A_rb, p_B_III_2_A_rp = self._get_amount(tree, 'Pasywa_B_III_2_A', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_2_A', 'KwotaB')
        p_B_III_3_D_rb, p_B_III_3_D_rp = self._get_amount(tree, 'Pasywa_B_III_3_D', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_3_D', 'KwotaB')
        p_B_III_B_rb, p_B_III_B_rp = self._get_amount(tree, 'Pasywa_B_III_B', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_B', 'KwotaB')
        p_B_III_3_G_rb, p_B_III_3_G_rp = self._get_amount(tree, 'Pasywa_B_III_3_G', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_3_G', 'KwotaB')
        p_B_III_3_A_rb, p_B_III_3_A_rp = self._get_amount(tree, 'Pasywa_B_III_3_A', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_3_A', 'KwotaB')
        p_B_III_A_rb, p_B_III_A_rp = self._get_amount(tree, 'Pasywa_B_III_A', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_A', 'KwotaB')
        p_B_IV_rb, p_B_IV_rp = self._get_amount(tree, 'Pasywa_B_IV', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_IV', 'KwotaB')

        # --- DETEKCJA WARIANTU RZiS ---
        # Sprawdzamy obecność sekcji RZiSPor lub RZiSKalk
        has_rzpor = len(tree.xpath("//*[local-name()='RZiSPor']")) > 0
        has_rzkal = len(tree.xpath("//*[local-name()='RZiSKalk']")) > 0

        # --- RZiS PORÓWNAWCZY (Pobieranie z kontekstem RZiSPor) ---
        if has_rzpor:
            logger.info("Wykryto wariant Porównawczy RZiS.")
            rzpor_A_rb, rzpor_A_rp = self._get_amount(tree, 'A', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'A', 'KwotaB', 'RZiSPor')
            rzpor_B_I_rb, rzpor_B_I_rp = self._get_amount(tree, 'B_I', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_I', 'KwotaB', 'RZiSPor')
            rzpor_B_II_rb, rzpor_B_II_rp = self._get_amount(tree, 'B_II', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_II', 'KwotaB', 'RZiSPor')
            rzpor_B_III_rb, rzpor_B_III_rp = self._get_amount(tree, 'B_III', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_III', 'KwotaB', 'RZiSPor')
            rzpor_B_IV_rb, rzpor_B_IV_rp = self._get_amount(tree, 'B_IV', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_IV', 'KwotaB', 'RZiSPor')
            rzpor_B_V_rb, rzpor_B_V_rp = self._get_amount(tree, 'B_V', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_V', 'KwotaB', 'RZiSPor')
            rzpor_B_VI_rb, rzpor_B_VI_rp = self._get_amount(tree, 'B_VI', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_VI', 'KwotaB', 'RZiSPor')
            rzpor_B_VII_rb, rzpor_B_VII_rp = self._get_amount(tree, 'B_VII', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_VII', 'KwotaB', 'RZiSPor')
            rzpor_B_VIII_rb, rzpor_B_VIII_rp = self._get_amount(tree, 'B_VIII', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_VIII', 'KwotaB', 'RZiSPor')
            rzpor_D_rb, rzpor_D_rp = self._get_amount(tree, 'D', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'D', 'KwotaB', 'RZiSPor')
            rzpor_E_rb, rzpor_E_rp = self._get_amount(tree, 'E', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'E', 'KwotaB', 'RZiSPor')
            rzpor_G_rb, rzpor_G_rp = self._get_amount(tree, 'G', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'G', 'KwotaB', 'RZiSPor')
            rzpor_H_rb, rzpor_H_rp = self._get_amount(tree, 'H', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'H', 'KwotaB', 'RZiSPor')
            rzpor_J_rb, rzpor_J_rp = self._get_amount(tree, 'J', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'J', 'KwotaB', 'RZiSPor')
        else:
            rzpor_A_rb = rzpor_A_rp = rzpor_B_I_rb = rzpor_B_I_rp = rzpor_B_II_rb = rzpor_B_II_rp = 0.0
            rzpor_B_III_rb = rzpor_B_III_rp = rzpor_B_IV_rb = rzpor_B_IV_rp = rzpor_B_V_rb = rzpor_B_V_rp = 0.0
            rzpor_B_VI_rb = rzpor_B_VI_rp = rzpor_B_VII_rb = rzpor_B_VII_rp = rzpor_B_VIII_rb = rzpor_B_VIII_rp = 0.0
            rzpor_D_rb = rzpor_D_rp = rzpor_E_rb = rzpor_E_rp = rzpor_G_rb = rzpor_G_rp = rzpor_H_rb = rzpor_H_rp = rzpor_J_rb = rzpor_J_rp = 0.0

        # --- RZiS KALKULACYJNY (Pobieranie z kontekstem RZiSKalk) ---
        if has_rzkal:
            logger.info("Wykryto wariant Kalkulacyjny RZiS.")
            rzkal_A_rb, rzkal_A_rp = self._get_amount(tree, 'A', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'A', 'KwotaB', 'RZiSKalk')
            rzkal_B_rb, rzkal_B_rp = self._get_amount(tree, 'B', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'B', 'KwotaB', 'RZiSKalk')
            rzkal_D_rb, rzkal_D_rp = self._get_amount(tree, 'D', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'D', 'KwotaB', 'RZiSKalk')
            rzkal_E_rb, rzkal_E_rp = self._get_amount(tree, 'E', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'E', 'KwotaB', 'RZiSKalk')
            rzkal_G_rb, rzkal_G_rp = self._get_amount(tree, 'G', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'G', 'KwotaB', 'RZiSKalk')
            rzkal_H_rb, rzkal_H_rp = self._get_amount(tree, 'H', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'H', 'KwotaB', 'RZiSKalk')
            rzkal_J_rb, rzkal_J_rp = self._get_amount(tree, 'J', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'J', 'KwotaB', 'RZiSKalk')
            rzkal_K_rb, rzkal_K_rp = self._get_amount(tree, 'K', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'K', 'KwotaB', 'RZiSKalk')
            rzkal_M_rb, rzkal_M_rp = self._get_amount(tree, 'M', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'M', 'KwotaB', 'RZiSKalk')
        else:
            rzkal_A_rb = rzkal_A_rp = rzkal_B_rb = rzkal_B_rp = rzkal_D_rb = rzkal_D_rp = 0.0
            rzkal_E_rb = rzkal_E_rp = rzkal_G_rb = rzkal_G_rp = rzkal_H_rb = rzkal_H_rp = 0.0
            rzkal_J_rb = rzkal_J_rp = rzkal_K_rb = rzkal_K_rp = rzkal_M_rb = rzkal_M_rp = 0.0

        # --- OBLICZENIA POL REZYDUALNYCH ---
        a_A_II_Inne_rb, a_A_II_Inne_rp = a_A_II_rb - a_A_II_2_rb, a_A_II_rp - a_A_II_2_rp
        a_A_IV_Inne_rb, a_A_IV_Inne_rp = a_A_IV_rb - (a_A_IV_1_rb + a_A_IV_2_rb), a_A_IV_rp - (a_A_IV_1_rp + a_A_IV_2_rp)
        a_A_V_Inne_rb, a_A_V_Inne_rp = a_A_V_rb - a_A_V_1_rb, a_A_V_rp - a_A_V_1_rp
        a_B_II_Inne_rb = a_B_II_rb - (a_B_II_1_A_rb + a_B_II_2_A_rb + a_B_II_3_A_rb + a_B_II_3_B_rb + a_B_II_A_rb)
        a_B_II_Inne_rp = a_B_II_rp - (a_B_II_1_A_rp + a_B_II_2_A_rp + a_B_II_3_A_rp + a_B_II_3_B_rp + a_B_II_A_rp)
        a_B_III_Inne_rb, a_B_III_Inne_rp = a_B_III_rb - (a_B_III_1_C_rb + a_B_III_A_1_rb), a_B_III_rp - (a_B_III_1_C_rp + a_B_III_A_1_rp)
        p_B_I_Inne_rb, p_B_I_Inne_rp = p_B_I_rb - (p_B_I_1_rb + p_B_I_2_rb), p_B_I_rp - (p_B_I_1_rp + p_B_I_2_rp)
        p_B_II_Inne_rb, p_B_II_Inne_rp = p_B_II_rb - (p_B_II_1_rb + p_B_II_3_A_rb), p_B_II_rp - (p_B_II_1_rp + p_B_II_3_A_rp)
        p_B_III_Inne_rb = p_B_III_rb - (p_B_III_1_A_rb + p_B_III_2_A_rb + p_B_III_3_D_rb + p_B_III_B_rb + p_B_III_3_G_rb + p_B_III_3_A_rb + p_B_III_A_rb)
        p_B_III_Inne_rp = p_B_III_rp - (p_B_III_1_A_rp + p_B_III_2_A_rp + p_B_III_3_D_rp + p_B_III_B_rp + p_B_III_3_G_rp + p_B_III_3_A_rp + p_B_III_A_rp)

        # Słownik 80 tagów SQL (zsynchronizowany z insert_obszary.sql)
        dane_do_bazy = {
            'Aktywa_A_I': (a_A_I_rb, a_A_I_rp), 'Aktywa_A_II_2': (a_A_II_2_rb, a_A_II_2_rp), 'Aktywa_A_II_Inne': (a_A_II_Inne_rb, a_A_II_Inne_rp),
            'Aktywa_A_III': (a_A_III_rb, a_A_III_rp), 'Aktywa_A_IV_1': (a_A_IV_1_rb, a_A_IV_1_rp), 'Aktywa_A_IV_2': (a_A_IV_2_rb, a_A_IV_2_rp), 'Aktywa_A_IV_Inne': (a_A_IV_Inne_rb, a_A_IV_Inne_rp),
            'Aktywa_A_V_1': (a_A_V_1_rb, a_A_V_1_rp), 'Aktywa_A_V_Inne': (a_A_V_Inne_rb, a_A_V_Inne_rp), 'Aktywa_B_I': (a_B_I_rb, a_B_I_rp),
            'Aktywa_B_II_1_A': (a_B_II_1_A_rb, a_B_II_1_A_rp), 'Aktywa_B_II_2_A': (a_B_II_2_A_rb, a_B_II_2_A_rp), 'Aktywa_B_II_3_A': (a_B_II_3_A_rb, a_B_II_3_A_rp),
            'Aktywa_B_II_A': (0.0, 0.0), 'Aktywa_B_II_3_B': (a_B_II_3_B_rb, a_B_II_3_B_rp), 'Aktywa_B_II_Inne': (a_B_II_Inne_rb, a_B_II_Inne_rp),
            'Aktywa_B_III_1_C': (a_B_III_1_C_rb, a_B_III_1_C_rp), 'Aktywa_B_III_A_1': (0.0, 0.0), 'Aktywa_B_III_Inne': (a_B_III_Inne_rb, a_B_III_Inne_rp),
            'Aktywa_B_IV': (a_B_IV_rb, a_B_IV_rp), 'Aktywa_C': (a_C_rb, a_C_rp), 'Aktywa_D': (a_D_rb, a_D_rp),
            'Pasywa_A_I': (p_A_I_rb, p_A_I_rp), 'Pasywa_A_II': (p_A_II_rb, p_A_II_rp), 'Pasywa_A_III': (p_A_III_rb, p_A_III_rp), 'Pasywa_A_IV': (p_A_IV_rb, p_A_IV_rp), 'Pasywa_A_V': (p_A_V_rb, p_A_V_rp), 'Pasywa_A_VI': (p_A_VI_rb, p_A_VI_rp), 'Pasywa_A_VII': (p_A_VII_rb, p_A_VII_rp),
            'Pasywa_B_I_1': (p_B_I_1_rb, p_B_I_1_rp), 'Pasywa_B_I_2': (p_B_I_2_rb, p_B_I_2_rp), 'Pasywa_B_I_Inne': (p_B_I_Inne_rb, p_B_I_Inne_rp),
            'Pasywa_B_II_1': (0.0, 0.0), 'Pasywa_B_II_3_A': (p_B_II_3_A_rb, p_B_II_3_A_rp), 'Pasywa_B_II_Inne': (p_B_II_Inne_rb, p_B_II_Inne_rp),
            'Pasywa_B_III_1_A': (p_B_III_1_A_rb, p_B_III_1_A_rp), 'Pasywa_B_III_2_A': (p_B_III_2_A_rb, p_B_III_2_A_rp), 'Pasywa_B_III_3_D': (p_B_III_3_D_rb, p_B_III_3_D_rp), 'Pasywa_B_III_B': (0.0, 0.0), 'Pasywa_B_III_3_G': (p_B_III_3_G_rb, p_B_III_3_G_rp), 'Pasywa_B_III_3_A': (p_B_III_3_A_rb, p_B_III_3_A_rp), 'Pasywa_B_III_A': (0.0, 0.0), 'Pasywa_B_III_Inne': (p_B_III_Inne_rb, p_B_III_Inne_rp), 'Pasywa_B_IV': (p_B_IV_rb, p_B_IV_rp),
            'RZiSKalk.A': (rzkal_A_rb, rzkal_A_rp), 'RZiSKalk.B': (rzkal_B_rb, rzkal_B_rp), 'RZiSKalk.D': (rzkal_D_rb, rzkal_D_rp), 'RZiSKalk.C.Mala': (0.0, 0.0), 'RZiSKalk.E': (rzkal_E_rb, rzkal_E_rp), 'RZiSKalk.D.Mala': (0.0, 0.0), 'RZiSKalk.G': (rzkal_G_rb, rzkal_G_rp), 'RZiSKalk.F.Mala': (0.0, 0.0), 'RZiSKalk.H': (rzkal_H_rb, rzkal_H_rp), 'RZiSKalk.G.Mala': (0.0, 0.0), 'RZiSKalk.J': (rzkal_J_rb, rzkal_J_rp), 'RZiSKalk.H.Mala': (0.0, 0.0), 'RZiSKalk.K': (rzkal_K_rb, rzkal_K_rp), 'RZiSKalk.I.Mala': (0.0, 0.0), 'RZiSKalk.M': (rzkal_M_rb, rzkal_M_rp), 'RZiSKalk.K.Mala': (0.0, 0.0),
            'RZiSPor.A': (rzpor_A_rb, rzpor_A_rp), 'RZiSPor.B_I': (rzpor_B_I_rb, rzpor_B_I_rp), 'RZiSPor.B_II': (rzpor_B_II_rb, rzpor_B_II_rp), 'RZiSPor.B_III': (rzpor_B_III_rb, rzpor_B_III_rp), 'RZiSPor.B_IV': (rzpor_B_IV_rb, rzpor_B_IV_rp), 'RZiSPor.B_V': (rzpor_B_V_rb, rzpor_B_V_rp), 'RZiSPor.B_IV.Mala': (0.0, 0.0), 'RZiSPor.B_VI': (rzpor_B_VI_rb, rzpor_B_VI_rp), 'RZiSPor.B_V.Mala': (0.0, 0.0), 'RZiSPor.B_VII': (rzpor_B_VII_rb, rzpor_B_VII_rp), 'RZiSPor.B_VI.Mala': (0.0, 0.0), 'RZiSPor.B_VIII': (rzpor_B_VIII_rb, rzpor_B_VIII_rp), 'RZiSPor.D': (rzpor_D_rb, rzpor_D_rp), 'RZiSPor.E': (rzpor_E_rb, rzpor_E_rp), 'RZiSPor.G': (rzpor_G_rb, rzpor_G_rp), 'RZiSPor.F.Mala': (0.0, 0.0), 'RZiSPor.H': (rzpor_H_rb, rzpor_H_rp), 'RZiSPor.G.Mala': (0.0, 0.0), 'RZiSPor.J': (rzpor_J_rb, rzpor_J_rp), 'RZiSPor.I.Mala': (0.0, 0.0)
        }

        # Zapis do bazy danych
        cursor = db_conn.cursor()
        try:
            logger.info("Resetowanie dotychczasowych kwot sprawozdania...")
            cursor.execute("UPDATE Sprawozdanie_Pozycje SET Kwota_RB = 0.0, Kwota_RP = 0.0")
            update_query = "UPDATE Sprawozdanie_Pozycje SET Kwota_RB = ?, Kwota_RP = ? WHERE XmlTag = ?"
            params = [(rb, rp, tag) for tag, (rb, rp) in dane_do_bazy.items()]
            cursor.executemany(update_query, params)
            db_conn.commit()
            logger.info("Pomyślnie zaktualizowano 80 tagów dla Jednostki Innej.")
        except Exception as e:
            db_conn.rollback()
            logger.error(f"Błąd zapisu: {e}")
            raise

    def import_jednostka_mala(self, xml_path: str, db_conn):
        logger.info(f"Rozpoczynanie importu e-Sprawozdania dla Jednostki Małej z pliku: {xml_path}")
        try:
            tree = etree.parse(xml_path)
        except Exception as e:
            logger.error(f"Błąd podczas parsowania pliku XML: {e}")
            raise

        logger.info("Pobieranie wartości z pliku XML...")
        
        # --- BILANS (Tagi unikalne lub sekcje o stałej strukturze) ---
        a_A_II_rb, a_A_II_rp = self._get_amount(tree, 'Aktywa_A_II', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_II', 'KwotaB')
        a_A_IV_rb, a_A_IV_rp = self._get_amount(tree, 'Aktywa_A_IV', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_IV', 'KwotaB')
        a_A_V_rb, a_A_V_rp = self._get_amount(tree, 'Aktywa_A_V', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_V', 'KwotaB')
        a_B_II_rb, a_B_II_rp = self._get_amount(tree, 'Aktywa_B_II', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II', 'KwotaB')
        a_B_III_rb, a_B_III_rp = self._get_amount(tree, 'Aktywa_B_III', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_III', 'KwotaB')
        p_B_I_rb, p_B_I_rp = self._get_amount(tree, 'Pasywa_B_I', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_I', 'KwotaB')
        p_B_II_rb, p_B_II_rp = self._get_amount(tree, 'Pasywa_B_II', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_II', 'KwotaB')
        p_B_III_rb, p_B_III_rp = self._get_amount(tree, 'Pasywa_B_III', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III', 'KwotaB')

        a_A_I_rb, a_A_I_rp = self._get_amount(tree, 'Aktywa_A_I', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_I', 'KwotaB')
        a_A_II_2_rb, a_A_II_2_rp = self._get_amount(tree, 'Aktywa_A_II_2', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_II_2', 'KwotaB')
        a_A_III_rb, a_A_III_rp = self._get_amount(tree, 'Aktywa_A_III', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_III', 'KwotaB')
        a_A_IV_1_rb, a_A_IV_1_rp = self._get_amount(tree, 'Aktywa_A_IV_1', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_IV_1', 'KwotaB')
        a_A_IV_2_rb, a_A_IV_2_rp = self._get_amount(tree, 'Aktywa_A_IV_2', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_IV_2', 'KwotaB')
        a_A_V_1_rb, a_A_V_1_rp = self._get_amount(tree, 'Aktywa_A_V_1', 'KwotaA'), self._get_amount(tree, 'Aktywa_A_V_1', 'KwotaB')
        a_B_I_rb, a_B_I_rp = self._get_amount(tree, 'Aktywa_B_I', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_I', 'KwotaB')
        a_B_II_A_rb, a_B_II_A_rp = self._get_amount(tree, 'Aktywa_B_II_A', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_II_A', 'KwotaB')
        a_B_III_1_C_rb, a_B_III_1_C_rp = self._get_amount(tree, 'Aktywa_B_III_1_C', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_III_1_C', 'KwotaB')
        a_B_III_A_1_rb, a_B_III_A_1_rp = self._get_amount(tree, 'Aktywa_B_III_A_1', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_III_A_1', 'KwotaB')
        a_B_IV_rb, a_B_IV_rp = self._get_amount(tree, 'Aktywa_B_IV', 'KwotaA'), self._get_amount(tree, 'Aktywa_B_IV', 'KwotaB')
        a_C_rb, a_C_rp = self._get_amount(tree, 'Aktywa_C', 'KwotaA'), self._get_amount(tree, 'Aktywa_C', 'KwotaB')
        a_D_rb, a_D_rp = self._get_amount(tree, 'Aktywa_D', 'KwotaA'), self._get_amount(tree, 'Aktywa_D', 'KwotaB')

        p_A_I_rb, p_A_I_rp = self._get_amount(tree, 'Pasywa_A_I', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_I', 'KwotaB')
        p_A_II_rb, p_A_II_rp = self._get_amount(tree, 'Pasywa_A_II', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_II', 'KwotaB')
        p_A_III_rb, p_A_III_rp = self._get_amount(tree, 'Pasywa_A_III', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_III', 'KwotaB')
        p_A_IV_rb, p_A_IV_rp = self._get_amount(tree, 'Pasywa_A_IV', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_IV', 'KwotaB')
        p_A_V_rb, p_A_V_rp = self._get_amount(tree, 'Pasywa_A_V', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_V', 'KwotaB')
        p_A_VI_rb, p_A_VI_rp = self._get_amount(tree, 'Pasywa_A_VI', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_VI', 'KwotaB')
        p_A_VII_rb, p_A_VII_rp = self._get_amount(tree, 'Pasywa_A_VII', 'KwotaA'), self._get_amount(tree, 'Pasywa_A_VII', 'KwotaB')
        p_B_I_1_rb, p_B_I_1_rp = self._get_amount(tree, 'Pasywa_B_I_1', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_I_1', 'KwotaB')
        p_B_I_2_rb, p_B_I_2_rp = self._get_amount(tree, 'Pasywa_B_I_2', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_I_2', 'KwotaB')
        p_B_II_1_rb, p_B_II_1_rp = self._get_amount(tree, 'Pasywa_B_II_1', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_II_1', 'KwotaB')
        p_B_II_3_A_rb, p_B_II_3_A_rp = self._get_amount(tree, 'Pasywa_B_II_3_A', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_II_3_A', 'KwotaB')
        p_B_III_B_rb, p_B_III_B_rp = self._get_amount(tree, 'Pasywa_B_III_B', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_B', 'KwotaB')
        p_B_III_A_rb, p_B_III_A_rp = self._get_amount(tree, 'Pasywa_B_III_A', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_III_A', 'KwotaB')
        p_B_IV_rb, p_B_IV_rp = self._get_amount(tree, 'Pasywa_B_IV', 'KwotaA'), self._get_amount(tree, 'Pasywa_B_IV', 'KwotaB')

        # --- DETEKCJA WARIANTU RZiS ---
        has_rzpor = len(tree.xpath("//*[local-name()='RZiSPor']")) > 0
        has_rzkal = len(tree.xpath("//*[local-name()='RZiSKalk']")) > 0

        # --- RZiS Porównawczy (Mała) ---
        if has_rzpor:
            logger.info("Wykryto wariant Porównawczy RZiS (Mała).")
            rzmala_A_rb, rzmala_A_rp = self._get_amount(tree, 'A', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'A', 'KwotaB', 'RZiSPor')
            rzmala_B_I_rb, rzmala_B_I_rp = self._get_amount(tree, 'B_I', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_I', 'KwotaB', 'RZiSPor')
            rzmala_B_II_rb, rzmala_B_II_rp = self._get_amount(tree, 'B_II', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_II', 'KwotaB', 'RZiSPor')
            rzmala_B_III_rb, rzmala_B_III_rp = self._get_amount(tree, 'B_III', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_III', 'KwotaB', 'RZiSPor')
            rzmala_B_IV_rb, rzmala_B_IV_rp = self._get_amount(tree, 'B_IV', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_IV', 'KwotaB', 'RZiSPor')
            rzmala_B_V_rb, rzmala_B_V_rp = self._get_amount(tree, 'B_V', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_V', 'KwotaB', 'RZiSPor')
            rzmala_B_VI_rb, rzmala_B_VI_rp = self._get_amount(tree, 'B_VI', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'B_VI', 'KwotaB', 'RZiSPor')
            rzmala_D_rb, rzmala_D_rp = self._get_amount(tree, 'D', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'D', 'KwotaB', 'RZiSPor')
            rzmala_E_rb, rzmala_E_rp = self._get_amount(tree, 'E', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'E', 'KwotaB', 'RZiSPor')
            rzmala_F_rb, rzmala_F_rp = self._get_amount(tree, 'F', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'F', 'KwotaB', 'RZiSPor')
            rzmala_G_rb, rzmala_G_rp = self._get_amount(tree, 'G', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'G', 'KwotaB', 'RZiSPor')
            rzmala_H_rb, rzmala_H_rp = self._get_amount(tree, 'H', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'H', 'KwotaB', 'RZiSPor')
            rzmala_I_rb, rzmala_I_rp = self._get_amount(tree, 'I', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'I', 'KwotaB', 'RZiSPor')
            rzmala_J_rb, rzmala_J_rp = self._get_amount(tree, 'J', 'KwotaA', 'RZiSPor'), self._get_amount(tree, 'J', 'KwotaB', 'RZiSPor')
        else:
            rzmala_A_rb = rzmala_A_rp = rzmala_B_I_rb = rzmala_B_I_rp = rzmala_B_II_rb = rzmala_B_II_rp = 0.0
            rzmala_B_III_rb = rzmala_B_III_rp = rzmala_B_IV_rb = rzmala_B_IV_rp = rzmala_B_V_rb = rzmala_B_V_rp = 0.0
            rzmala_B_VI_rb = rzmala_B_VI_rp = rzmala_D_rb = rzmala_D_rp = rzmala_E_rb = rzmala_E_rp = 0.0
            rzmala_F_rb = rzmala_F_rp = rzmala_G_rb = rzmala_G_rp = rzmala_H_rb = rzmala_H_rp = rzmala_I_rb = rzmala_I_rp = rzmala_J_rb = rzmala_J_rp = 0.0

        # --- RZiS Kalkulacyjny (Mała) ---
        if has_rzkal:
            logger.info("Wykryto wariant Kalkulacyjny RZiS (Mała).")
            rzkal_A_rb, rzkal_A_rp = self._get_amount(tree, 'A', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'A', 'KwotaB', 'RZiSKalk')
            rzkal_B_rb, rzkal_B_rp = self._get_amount(tree, 'B', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'B', 'KwotaB', 'RZiSKalk')
            rzkal_C_m_rb, rzkal_C_m_rp = self._get_amount(tree, 'C', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'C', 'KwotaB', 'RZiSKalk')
            rzkal_D_m_rb, rzkal_D_m_rp = self._get_amount(tree, 'D', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'D', 'KwotaB', 'RZiSKalk')
            rzkal_F_m_rb, rzkal_F_m_rp = self._get_amount(tree, 'F', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'F', 'KwotaB', 'RZiSKalk')
            rzkal_G_m_rb, rzkal_G_m_rp = self._get_amount(tree, 'G', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'G', 'KwotaB', 'RZiSKalk')
            rzkal_H_m_rb, rzkal_H_m_rp = self._get_amount(tree, 'H', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'H', 'KwotaB', 'RZiSKalk')
            rzkal_I_m_rb, rzkal_I_m_rp = self._get_amount(tree, 'I', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'I', 'KwotaB', 'RZiSKalk')
            rzkal_K_m_rb, rzkal_K_m_rp = self._get_amount(tree, 'K', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'K', 'KwotaB', 'RZiSKalk')
            rzkal_M_rb, rzkal_M_rp = self._get_amount(tree, 'M', 'KwotaA', 'RZiSKalk'), self._get_amount(tree, 'M', 'KwotaB', 'RZiSKalk')
        else:
            rzkal_A_rb = rzkal_A_rp = rzkal_B_rb = rzkal_B_rp = rzkal_C_m_rb = rzkal_C_m_rp = rzkal_D_m_rb = rzkal_D_m_rp = 0.0
            rzkal_F_m_rb = rzkal_F_m_rp = rzkal_G_m_rb = rzkal_G_m_rp = rzkal_H_m_rb = rzkal_H_m_rp = 0.0
            rzkal_I_m_rb = rzkal_I_m_rp = rzkal_K_m_rb = rzkal_K_m_rp = rzkal_M_rb = rzkal_M_rp = 0.0

        # --- OBLICZENIA POL REZYDUALNYCH ---
        a_A_II_Inne_rb, a_A_II_Inne_rp = a_A_II_rb - a_A_II_2_rb, a_A_II_rp - a_A_II_2_rp
        a_A_IV_Inne_rb, a_A_IV_Inne_rp = a_A_IV_rb - (a_A_IV_1_rb + a_A_IV_2_rb), a_A_IV_rp - (a_A_IV_1_rp + a_A_IV_2_rp)
        a_A_V_Inne_rb, a_A_V_Inne_rp = a_A_V_rb - a_A_V_1_rb, a_A_V_rp - a_A_V_1_rp
        a_B_II_Inne_rb = a_B_II_rb - (a_B_II_A_rb)
        a_B_II_Inne_rp = a_B_II_rp - (a_B_II_A_rp)
        a_B_III_Inne_rb, a_B_III_Inne_rp = a_B_III_rb - (a_B_III_1_C_rb + a_B_III_A_1_rb), a_B_III_rp - (a_B_III_1_C_rp + a_B_III_A_1_rp)
        p_B_I_Inne_rb, p_B_I_Inne_rp = p_B_I_rb - (p_B_I_1_rb + p_B_I_2_rb), p_B_I_rp - (p_B_I_1_rp + p_B_I_2_rp)
        p_B_II_Inne_rb, p_B_II_Inne_rp = p_B_II_rb - (p_B_II_1_rb + p_B_II_3_A_rb), p_B_II_rp - (p_B_II_1_rp + p_B_II_3_A_rp)
        p_B_III_Inne_rb = p_B_III_rb - (p_B_III_B_rb + p_B_III_A_rb)
        p_B_III_Inne_rp = p_B_III_rp - (p_B_III_B_rp + p_B_III_A_rp)

        # Słownik 80 tagów SQL (zsynchronizowany z insert_obszary.sql)
        dane_do_bazy = {
            'Aktywa_A_I': (a_A_I_rb, a_A_I_rp), 'Aktywa_A_II_2': (a_A_II_2_rb, a_A_II_2_rp), 'Aktywa_A_II_Inne': (a_A_II_Inne_rb, a_A_II_Inne_rp),
            'Aktywa_A_III': (a_A_III_rb, a_A_III_rp), 'Aktywa_A_IV_1': (a_A_IV_1_rb, a_A_IV_1_rp), 'Aktywa_A_IV_2': (a_A_IV_2_rb, a_A_IV_2_rp), 'Aktywa_A_IV_Inne': (a_A_IV_Inne_rb, a_A_IV_Inne_rp),
            'Aktywa_A_V_1': (a_A_V_1_rb, a_A_V_1_rp), 'Aktywa_A_V_Inne': (a_A_V_Inne_rb, a_A_V_Inne_rp), 'Aktywa_B_I': (a_B_I_rb, a_B_I_rp),
            'Aktywa_B_II_1_A': (0.0, 0.0), 'Aktywa_B_II_2_A': (0.0, 0.0), 'Aktywa_B_II_3_A': (0.0, 0.0),
            'Aktywa_B_II_A': (a_B_II_A_rb, a_B_II_A_rp), 'Aktywa_B_II_3_B': (0.0, 0.0), 'Aktywa_B_II_Inne': (a_B_II_Inne_rb, a_B_II_Inne_rp),
            'Aktywa_B_III_1_C': (a_B_III_1_C_rb, a_B_III_1_C_rp), 'Aktywa_B_III_A_1': (a_B_III_A_1_rb, a_B_III_A_1_rp), 'Aktywa_B_III_Inne': (a_B_III_Inne_rb, a_B_III_Inne_rp),
            'Aktywa_B_IV': (a_B_IV_rb, a_B_IV_rp), 'Aktywa_C': (a_C_rb, a_C_rp), 'Aktywa_D': (a_D_rb, a_D_rp),
            'Pasywa_A_I': (p_A_I_rb, p_A_I_rp), 'Pasywa_A_II': (p_A_II_rb, p_A_II_rp), 'Pasywa_A_III': (p_A_III_rb, p_A_III_rp), 'Pasywa_A_IV': (p_A_IV_rb, p_A_IV_rp), 'Pasywa_A_V': (p_A_V_rb, p_A_V_rp), 'Pasywa_A_VI': (p_A_VI_rb, p_A_VI_rp), 'Pasywa_A_VII': (p_A_VII_rb, p_A_VII_rp),
            'Pasywa_B_I_1': (p_B_I_1_rb, p_B_I_1_rp), 'Pasywa_B_I_2': (p_B_I_2_rb, p_B_I_2_rp), 'Pasywa_B_I_Inne': (p_B_I_Inne_rb, p_B_I_Inne_rp),
            'Pasywa_B_II_1': (p_B_II_1_rb, p_B_II_1_rp), 'Pasywa_B_II_3_A': (p_B_II_3_A_rb, p_B_II_3_A_rp), 'Pasywa_B_II_Inne': (p_B_II_Inne_rb, p_B_II_Inne_rp),
            'Pasywa_B_III_1_A': (0.0, 0.0), 'Pasywa_B_III_2_A': (0.0, 0.0), 'Pasywa_B_III_3_D': (0.0, 0.0), 'Pasywa_B_III_B': (p_B_III_B_rb, p_B_III_B_rp), 'Pasywa_B_III_3_G': (0.0, 0.0), 'Pasywa_B_III_3_A': (0.0, 0.0), 'Pasywa_B_III_A': (p_B_III_A_rb, p_B_III_A_rp), 'Pasywa_B_III_Inne': (p_B_III_Inne_rb, p_B_III_Inne_rp), 'Pasywa_B_IV': (p_B_IV_rb, p_B_IV_rp),
            'RZiSKalk.A': (rzkal_A_rb, rzkal_A_rp), 'RZiSKalk.B': (rzkal_B_rb, rzkal_B_rp), 'RZiSKalk.D': (0.0, 0.0), 'RZiSKalk.C.Mala': (rzkal_C_m_rb, rzkal_C_m_rp), 'RZiSKalk.E': (0.0, 0.0), 'RZiSKalk.D.Mala': (rzkal_D_m_rb, rzkal_D_m_rp), 'RZiSKalk.G': (0.0, 0.0), 'RZiSKalk.F.Mala': (rzkal_F_m_rb, rzkal_F_m_rp), 'RZiSKalk.H': (0.0, 0.0), 'RZiSKalk.G.Mala': (rzkal_G_m_rb, rzkal_G_m_rp), 'RZiSKalk.J': (0.0, 0.0), 'RZiSKalk.H.Mala': (0.0, 0.0), 'RZiSKalk.K': (0.0, 0.0), 'RZiSKalk.I.Mala': (rzkal_I_m_rb, rzkal_I_m_rp), 'RZiSKalk.M': (rzkal_M_rb, rzkal_M_rp), 'RZiSKalk.K.Mala': (rzkal_K_m_rb, rzkal_K_m_rp),
            'RZiSPor.A': (rzmala_A_rb, rzmala_A_rp), 'RZiSPor.B_I': (rzmala_B_I_rb, rzmala_B_I_rp), 'RZiSPor.B_II': (rzmala_B_II_rb, rzmala_B_II_rp), 'RZiSPor.B_III': (rzmala_B_III_rb, rzmala_B_III_rp), 'RZiSPor.B_IV': (0.0, 0.0), 'RZiSPor.B_V': (0.0, 0.0), 'RZiSPor.B_IV.Mala': (rzmala_B_IV_rb, rzmala_B_IV_rp), 'RZiSPor.B_VI': (0.0, 0.0), 'RZiSPor.B_V.Mala': (rzmala_B_V_rb, rzmala_B_V_rp), 'RZiSPor.B_VII': (0.0, 0.0), 'RZiSPor.B_VI.Mala': (rzmala_B_VI_rb, rzmala_B_VI_rp), 'RZiSPor.B_VIII': (0.0, 0.0), 'RZiSPor.D': (rzmala_D_rb, rzmala_D_rp), 'RZiSPor.E': (rzmala_E_rb, rzmala_E_rp), 'RZiSPor.G': (0.0, 0.0), 'RZiSPor.F.Mala': (rzmala_F_rb, rzmala_F_rp), 'RZiSPor.H': (0.0, 0.0), 'RZiSPor.G.Mala': (rzmala_G_rb, rzmala_G_rp), 'RZiSPor.J': (rzmala_J_rb, rzmala_J_rp), 'RZiSPor.I.Mala': (rzmala_I_rb, rzmala_I_rp)
        }

        # Zapis do bazy danych
        cursor = db_conn.cursor()
        try:
            logger.info("Resetowanie dotychczasowych kwot sprawozdania...")
            cursor.execute("UPDATE Sprawozdanie_Pozycje SET Kwota_RB = 0.0, Kwota_RP = 0.0")
            update_query = "UPDATE Sprawozdanie_Pozycje SET Kwota_RB = ?, Kwota_RP = ? WHERE XmlTag = ?"
            params = [(rb, rp, tag) for tag, (rb, rp) in dane_do_bazy.items()]
            cursor.executemany(update_query, params)
            db_conn.commit()
            logger.info("Pomyślnie zaktualizowano 80 tagów dla Jednostki Małej.")
        except Exception as e:
            db_conn.rollback()
            logger.error(f"Błąd zapisu: {e}")
            raise
