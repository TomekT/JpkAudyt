-- Plik: insert_obszary.sql
-- Inicjalizacja pozycji e-Sprawozdania Finansowego (Bilans)

INSERT OR IGNORE INTO Sprawozdanie_Pozycje (XmlTag, Nazwa) VALUES 
('Aktywa_A_I', 'Wartości niematerialne i prawne'),
('Aktywa_A_II', 'Rzeczowe aktywa trwałe'),
('Aktywa_A_III', 'Należności długoterminowe'),
('Aktywa_A_IV_1', 'Inwestycje długoterminowe - nieruchomości i prawo użytkowania'),
('Aktywa_A_IV_2', 'Inwestycje długoterminowe - Wartości niematerialne i prawne'),
('Aktywa_A_IV_3_A_1', 'Inwestycje długoterminowe - udziały i akcje - powiązane jednostki'),
('Aktywa_A_IV_3_A_2', 'Inwestycje długoterminowe - inne papiery wartościowe - powiązane jednostki'),
('Aktywa_A_IV_3_A_3', 'Inwestycje długoterminowe - udzielone pożyczki - powiązane jednostki'),
('Aktywa_A_IV_3_A_4', 'Inwestycje długoterminowe - inne aktywa finansowe - powiązane jednostki'),
('Aktywa_A_IV_3_B_1', 'Inwestycje długoterminowe - udziały i akcje - zaangażowane jednostki'),
('Aktywa_A_IV_3_B_2', 'Inwestycje długoterminowe - inne papiery wartościowe - zaangażowane jednostki'),
('Aktywa_A_IV_3_B_3', 'Inwestycje długoterminowe - udzielone pożyczki - zaangażowane jednostki'),
('Aktywa_A_IV_3_B_4', 'Inwestycje długoterminowe - inne aktywa finansowe - zaangażowane jednostki'),
('Aktywa_A_IV_3_C_1', 'Inwestycje długoterminowe - udziały i akcje - pozostałe jednostki'),
('Aktywa_A_IV_3_C_2', 'Inwestycje długoterminowe - inne papiery wartościowe - pozostałe jednostki'),
('Aktywa_A_IV_3_C_3', 'Inwestycje długoterminowe - udzielone pożyczki - pozostałe jednostki'),
('Aktywa_A_IV_3_C_4', 'Inwestycje długoterminowe - inne aktywa finansowe - pozostałe jednostki'),
('Aktywa_A_IV_4', 'Inwestycje długoterminowe - Inne'),
('Aktywa_A_V_1', 'Długoterminowe RMK - Aktywa z tytułu odroczonego podatku dochodowego'),
('Aktywa_A_V_2', 'Długoterminowe RMK - Inne'),
('Aktywa_B_I', 'Zapasy'),
('Aktywa_B_II_1_A', 'Należności z tyt dostaw - powiązane jednostki'),
('Aktywa_B_II_1_B', 'Należności inne - powiązane jednostki'),
('Aktywa_B_II_2_A', 'Należności z tyt dostaw - zaangażowane jednostki'),
('Aktywa_B_II_2_B', 'Należności inne - zaangażowane jednostki'),
('Aktywa_B_II_3_A', 'Należności z tyt dostaw - pozostałe jednostki'),
('Aktywa_B_II_3_B', 'Należności z tyt. podatków, ceł, ubezp. społ.'),
('Aktywa_B_II_3_C', 'Należności inne - pozostałe jednostki'),
('Aktywa_B_II_3_D', 'Należności dochodzone na drodze sądowej'),
('Aktywa_B_III_1_A_1', 'Krótkoterminowe udziały - powiązane jednostki'),
('Aktywa_B_III_1_A_2', 'Krótkoterminowe inne papiery wartościowe - powiązane jednostki'),
('Aktywa_B_III_1_A_3', 'Krótkoterminowe udzielone pożyczki - powiązane jednostki'),
('Aktywa_B_III_1_A_4', 'Krótkoterminowe inne aktywa finansowe - powiązane jednostki'),
('Aktywa_B_III_1_B_1', 'Krótkoterminowe udziały i akcje - pozostałe jednostki'),
('Aktywa_B_III_1_B_2', 'Krótkoterminowe inne papiery wartościowe - pozostałe jednostki'),
('Aktywa_B_III_1_B_3', 'Krótkoterminowe udzielone pożyczki - pozostałe jednostki'),
('Aktywa_B_III_1_B_4', 'Krótkoterminowe inne aktywa finansowe - pozostałe jednostki'),
('Aktywa_B_III_1_C', 'Środki pieniężne i inne aktywa pieniężne'),
('Aktywa_B_III_2', 'Inne inwestycje krótkoterminowe'),
('Aktywa_B_IV', 'Krótkoterminowe rozliczenia międzyokresowe'),
('Pasywa_A', 'Kapitał własny'),
('Pasywa_B_I_1', 'Rezerwy z tytułu odroczonego podatku dochodowego'),
('Pasywa_B_I_2', 'Rezerwa na świadczenia emerytalne i podobne'),
('Pasywa_B_I_3', 'Pozostałe rezerwy'),
('Pasywa_B_II', 'Zobowiązania długoterminowe'),
('Pasywa_B_III_1_A', 'Zobowiązania z tytułu dostaw i usług - powiązane jednostki'),
('Pasywa_B_III_1_B', 'Zobowiązania inne - powiązane jednostki'),
('Pasywa_B_III_2_A', 'Zobowiązania z tytułu dostaw i usług - zaangażowane jednostki'),
('Pasywa_B_III_2_B', 'Zobowiązania inne - zaangażowane jednostki'),
('Pasywa_B_III_3_A', 'Zobowiązania krótkoterminowe - kredyty i pożyczki'),
('Pasywa_B_III_3_B', 'Zobowiązania krótkoterminowe - z tytułu emisji dłużnych papierów wartościowych'),
('Pasywa_B_III_3_C', 'Zobowiązania krótkoterminowe - inne zobowiązania finansowe'),
('Pasywa_B_III_3_D', 'Zobowiązania krótkoterminowe - z tyt. dostaw i usług'),
('Pasywa_B_III_3_E', 'Zobowiązania krótkoterminowe - zaliczki otrzymane na dostawy i usługi'),
('Pasywa_B_III_3_F', 'Zobowiązania krótkoterminowe - zobowiązania wekslowe'),
('Pasywa_B_III_3_G', 'Zobowiązania krótkoterminowe - z tytułu podatków, ceł, ubezpieczeń społecznych'),
('Pasywa_B_III_3_H', 'Zobowiązania krótkoterminowe - z tytułu wynagrodzeń'),
('Pasywa_B_III_3_I', 'Zobowiązania krótkoterminowe - inne'),
('Pasywa_B_III_4', 'Fundusze specjalne'),
('Pasywa_B_IV', 'Rozliczenia międzyokresowe bierne');


-- Dodanie predefiniowanych obszarów badania z jawnymi ID
INSERT OR IGNORE INTO Obszary (Id, Nazwa, Typ, Czy_Systemowy) VALUES
-- AKTYWA
(1, 'WNiP', 'Aktywa', 1),
(2, 'Środki Trwałe', 'Aktywa', 1),
(3, 'Inwestycje - akcje, udziały', 'Aktywa', 1),
(4, 'Inwestycje - udzielone pożyczki', 'Aktywa', 1),
(5, 'Inwestycje - inne', 'Aktywa', 1),
(6, 'Zapasy', 'Aktywa', 1),
(7, 'Należności handlowe', 'Aktywa', 1),
(8, 'Inne należności', 'Aktywa', 1),
(9, 'Środki Pieniężne', 'Aktywa', 1),
(10, 'Czynne rozliczenia międzyokresowe', 'Aktywa', 1),

-- PASYWA
(11, 'Kapitał własny', 'Pasywa', 1),
(12, 'Rezerwy na zobowiązania', 'Pasywa', 1),
(13, 'Zobowiązania długoterminowe', 'Pasywa', 1),
(14, 'Zobowiązania krótkoterminowe', 'Pasywa', 1),
(15, 'Bierne rozliczenia międzyokresowe', 'Pasywa', 1),

-- RZiS
(16, 'Przychody ze sprzedaży', 'RZiS', 1),
(17, 'Koszty własne sprzedaży', 'RZiS', 1),
(18, 'Koszty ogólnego zarządu', 'RZiS', 1),
(19, 'Amortyzacja', 'RZiS', 1),
(20, 'Pozostałe przychody i koszty operacyjne', 'RZiS', 1),
(21, 'Przychody i koszty finansowe', 'RZiS', 1),
(22, 'Podatek dochodowy', 'RZiS', 1);

-- Mapowanie pozycji sprawozdania do systemowych obszarów badania po ID
INSERT OR IGNORE INTO Obszary_Sprawozdanie (Obszar_Id, XmlTag) VALUES
(1, 'Aktywa_A_I'),  -- WNiP
(2, 'Aktywa_A_II'), -- Środki Trwałe
(3, 'Aktywa_A_IV_3_A_1'), -- Inwestycje długoterminowe - udziały
(3, 'Aktywa_A_IV_3_B_1'), -- Inwestycje długoterminowe - udziały
(3, 'Aktywa_A_IV_3_C_1'), -- Inwestycje długoterminowe - udziały
(4, 'Aktywa_A_IV_3_A_3'), -- Inwestycje długoterminowe - pożyczki
(4, 'Aktywa_A_IV_3_B_3'), -- Inwestycje długoterminowe - pożyczki
(4, 'Aktywa_A_IV_3_C_3'), -- Inwestycje długoterminowe - pożyczki
(5, 'Aktywa_A_IV_1'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_2'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_3_A_2'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_3_B_2'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_3_C_2'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_3_A_4'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_3_B_4'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_3_C_4'), -- Inwestycje długoterminowe - inne
(5, 'Aktywa_A_IV_4'), -- Inwestycje długoterminowe - inne
(6, 'Aktywa_B_I'); -- Zapasy