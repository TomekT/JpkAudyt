-- Plik: insert_obszary.sql
-- Inicjalizacja pozycji e-Sprawozdania Finansowego (Bilans)

INSERT OR IGNORE INTO Sprawozdanie_Pozycje (XmlTag, Nazwa) VALUES 
('Aktywa_A_I', 'Wartości niematerialne i prawne'),
('Aktywa_A_II_2', 'Środki trwałe w budowie'),
('Aktywa_A_II_Inne', 'Inne środki trwałe'),  -- pozostałe A_II 
('Aktywa_A_III', 'Należności długoterminowe'),
-- Sekcja inwestycje długoterminowe
('Aktywa_A_IV_1', 'Nieruchomości inwestycyjne'),
('Aktywa_A_IV_2', 'Długoterminowe aktywa finansowe'),
('Aktywa_A_IV_Inne', 'Inne inwestycje długoterminowe'), -- pozostałe A_IV 
-- Sekcja długoterminowe rozliczenia międzyokresowe
('Aktywa_A_V_1', 'Aktywa z tytułu odroczonego podatku dochodowego'),
('Aktywa_A_V_Inne', 'Inne długoterminowe rozliczenia międzyokresowe'), -- pozostałe A_V 
-- Sekcja zapasy
('Aktywa_B_I', 'Zapasy'),
-- Sekcja nalezności krótkoterminowe
('Aktywa_B_II_1_A', 'Należności z tyt dostaw - powiązane jednostki'),
('Aktywa_B_II_2_A', 'Należności z tyt dostaw - zaangażowane jednostki'),
('Aktywa_B_II_3_A', 'Należności z tyt dostaw - pozostałe jednostki'),
('Aktywa_B_II_A', 'Należności z tyt dostaw - jednostka mała'), -- espr dla małej
('Aktywa_B_II_3_B', 'Należności z tyt. podatków, ceł, ubezp. społ.'),
('Aktywa_B_II_Inne', 'Inne należności krótkoterminowe'), -- pozostałe B_II 
-- Sekcja inwestycje krótkoterminowe
('Aktywa_B_III_1_C', 'środki pieniężne i inne aktywa pieniężne'),  -- środki pieniężne
('Aktywa_B_III_A_1', 'środki pieniężne w kasie i na rachunkach'), -- espr dla małej
('Aktywa_B_III_Inne', 'Inne inwestycje krótkoterminowe'),
-- Sekcja krótkoterminowe rozliczenia międzyokresowe
('Aktywa_B_IV', 'Krótkoterminowe rozliczenia międzyokresowe'),
-- Sekcja należne wpłaty
('Aktywa_C', 'Należne wpłaty na kapitał (fundusz) podstawowy'),
('Aktywa_D', 'Udziały (akcje) własne'),
-- Sekcja kapitały
('Pasywa_A_I', 'Kapitał (fundusz) podstawowy'),
('Pasywa_A_II', 'Kapitał zapasowy'),
('Pasywa_A_III', 'Kapitał z aktualizacji wyceny'),
('Pasywa_A_IV', 'Pozostałe kapitały (fundusze) rezerwowe'),
('Pasywa_A_V', 'Zysk (strata) z lat ubiegłych'),
('Pasywa_A_VI', 'Zysk (strata) netto'),
('Pasywa_A_VII', 'Odpisy z zysku netto w ciągu roku obrotowego (wielkość ujemna)'),
-- Sekcja rezerwy
('Pasywa_B_I_1', 'Rezerwy z tytułu odroczonego podatku dochodowego'),
('Pasywa_B_I_2', 'Rezerwa na świadczenia emerytalne i podobne'),
('Pasywa_B_I_Inne', 'Pozostałe rezerwy'), -- pozostałe B_I 
-- Sekcja zobowiązania długoterminowe
('Pasywa_B_II_1', 'Długoterminowe kredyty i pożyczki'), --jednostka mała, dla dużej będzie problem kolizji nazw
('Pasywa_B_II_3_A', 'Zobowiązania długoterminowe - kredyty i pożyczki'),
('Pasywa_B_II_Inne', 'Inne zobowiązania długoterminowe'),  -- pozostałe B_II 
-- Sekcja zobowiązania krótkoterminowe
('Pasywa_B_III_1_A', 'Zobow. krótk. z tyt dostaw - powiązane jednostki'),
('Pasywa_B_III_2_A', 'Zobow. krótk. z tyt dostaw - zaangażowane jednostki'),
('Pasywa_B_III_3_D', 'Zobow. krótk. - z tyt. dostaw i usług'),
('Pasywa_B_III_B', 'Zobow. krótk. z tyt dostaw - jednostka mała'), -- espr dla małej
('Pasywa_B_III_3_G', 'Zobow. krótk. z tyt. podatków'),
('Pasywa_B_III_3_A', 'Zobow. krótk. - kredyty i pożyczki'),
('Pasywa_B_III_A', 'Zobow, krótk. - kredyty i pożyczki - jednostka mała'), -- espr dla małej
('Pasywa_B_III_Inne', 'Inne zobow. krótkoterminowe'), -- pozostałe B_III 
-- Sekcja krótkoterminowe rozliczenia międzyokresowe
('Pasywa_B_IV', 'Rozliczenia międzyokresowe bierne'),
-- Sekcja RZiS kalkulacyjny
('RZiSKalk.A', 'Przychody netto ze sprzedaży produktów i towarów'),
('RZiSKalk.B', 'Koszt sprzedanych produktów i towarów'),
('RZiSKalk.D', 'Koszty sprzedaży'),
('RZiSKalk.C.Mala', 'Koszty sprzedaży - jednostka mała'), -- koszty sprzedaży dla jednostki małej
('RZiSKalk.E', 'Koszty ogólnego zarządu'),
('RZiSKalk.D.Mala', 'Koszty ogólnego zarządu - jednostka mała'), -- koszty ogólnego zarządu dla jednostki małej
('RZiSKalk.G', 'Pozostałe przychody operacyjne'),
('RZiSKalk.F.Mala', 'Poz. przych. operac. - jednostka mała'), -- pozostałe przychody operacyjne dla jednostki małej
('RZiSKalk.H', 'Pozostałe koszty operacyjne'),
('RZiSKalk.G.Mala', 'Poz. koszty operac. - jednostka mała'), -- pozostałe koszty operacyjne dla jednostki małej
('RZiSKalk.J', 'Przychody finansowe'),
('RZiSKalk.H.Mala', 'Przychody finansowe. - jednostka mała'),
('RZiSKalk.K', 'Koszty finansowe'),
('RZiSKalk.I.Mala', 'Koszty finansowe. - jednostka mała'),
('RZiSKalk.M', 'Podatek dochodowy'),
('RZiSKalk.K.Mala', 'Podatek dochodowy. - jednostka mała'),
-- Sekcja RZiS porównawczy
('RZiSPor.A', 'Przychody netto ze sprzedaży'),
-- Koszty operacyjne
('RZiSPor.B_I', 'Amortyzacja'),
('RZiSPor.B_II', 'Zużycie materiałów i energii'),
('RZiSPor.B_III', 'Usługi obce'),
('RZiSPor.B_IV', 'Podatki i opłaty'),
('RZiSPor.B_V', 'Wynagrodzenia'),
('RZiSPor.B_IV.Mala', 'Wynagrodzenia - jednostka mała'),
('RZiSPor.B_VI', 'Ubezp. społeczne i inne świadczenia'),
('RZiSPor.B_V.Mala', 'Ubezp. społeczne i inne świadczenia - jednostka mała'),
('RZiSPor.B_VII', 'Pozostałe koszty rodzajowe'),
('RZiSPor.B_VI.Mala', 'Pozostałe koszty rodzajowe - jednostka mała'),
('RZiSPor.B_VIII', 'Wartość sprzedanych towarów'),
-- Pozstałe przychody i koszty
('RZiSPor.D', 'Pozostałe przychody operacyjne'),
('RZiSPor.E', 'Pozostałe koszty operacyjne'),
('RZiSPor.G', 'Przychody finansowe'),
('RZiSPor.F.Mala', 'Przychody finansowe. - jednostka mała'),
('RZiSPor.H', 'Koszty finansowe'),
('RZiSPor.G.Mala', 'Koszty finansowe. - jednostka mała'),
('RZiSPor.J', 'Podatek dochodowy'),
('RZiSPor.I.Mala', 'Podatek dochodowy. - jednostka mała');

-- Dodanie predefiniowanych obszarów badania z jawnymi ID
INSERT OR IGNORE INTO Obszary (Id, Nazwa, Typ, Czy_Systemowy) VALUES
-- AKTYWA
(0, 'Podsumowanie bilansu i RZiS', 'Podsumowanie', 1),
(1, 'WNiP', 'Aktywa', 1),
(2, 'Środki trwałe', 'Aktywa', 1),
(3, 'Inwestycje', 'Aktywa', 1),
(4, 'Zapasy', 'Aktywa', 1),
(5, 'Należności z tyt dostaw', 'Aktywa', 1),
(6, 'Należności inne', 'Aktywa', 1),
(7, 'Środki pieniężne', 'Aktywa', 1),
(8, 'RMK czynne', 'Aktywa', 1),
(9, 'Akcje własne', 'Aktywa', 1),
-- PASYWA
(10, 'Kapitały', 'Pasywa', 1),
(11, 'Rezerwy', 'Pasywa', 1),
(12, 'Kredyty i pożyczki', 'Pasywa', 1),
(13, 'Zobowiązania z tyt dostaw', 'Pasywa', 1),
(14, 'Zobowiązania inne', 'Pasywa', 1),
(15, 'Bierne rozliczenia międzyokresowe', 'Pasywa', 1),
-- RZiS
(16, 'Przychody - układ kalk.', 'RZiS', 1),
(17, 'Koszty - układ kalk.', 'RZiS', 1),
(18, 'Przychody - układ por.', 'RZiS', 1),
(19, 'Koszt amortyzacji', 'RZiS', 1),
(20, 'Zużycie materiałów i usługi obce', 'RZiS', 1),
(21, 'Wynagrodzenia i ubezp. społeczne', 'RZiS', 1),
(22, 'Pozostałe koszty rodzajowe', 'RZiS', 1),
(23, 'Koszty sprzedaży i zarządu', 'RZiS', 1),
(24, 'Pozostałe przychody i koszty operacyjne', 'RZiS', 1),
(25, 'Przychody i koszty finansowe', 'RZiS', 1),
(26, 'Podatek dochodowy', 'RZiS', 1);

-- Mapowanie pozycji sprawozdania do systemowych obszarów badania po ID
INSERT OR IGNORE INTO Obszary_Sprawozdanie (Obszar_Id, XmlTag) VALUES
(1, 'Aktywa_A_I'),  -- WNiP
(2, 'Aktywa_A_II_2'),  -- środki trwałe w budowie
(2, 'Aktywa_A_II_Inne'),  -- inne środki trwałe
-- Inwestycje
(3, 'Aktywa_A_IV_1'), -- Nieruchomości inwestycyjne
(3, 'Aktywa_A_IV_2'), -- Długoterminowe aktywa finansowe
(3, 'Aktywa_A_IV_Inne'), -- Inwestycje dł - inne
(3, 'Aktywa_B_III_Inne'), -- Inwestycje krótkie - inne
(4, 'Aktywa_B_I'), -- Zapasy
-- Należności z tytułu dostaw i usług
(5, 'Aktywa_B_II_1_A'), -- Należności z tyt dostaw - powiązane jednostki
(5, 'Aktywa_B_II_2_A'), -- Należności z tyt dostaw - zaangażowane jednostki
(5, 'Aktywa_B_II_3_A'), -- Należności z tyt dostaw - pozostałe jednostki
(5, 'Aktywa_B_II_A'), -- Należności z tyt. dostaw - jednostka mała
-- Należności inne
(6, 'Aktywa_A_III'), -- Należności długoterminowe
(6, 'Aktywa_B_II_3_B'), -- Należności z tyt. podatków
(6, 'Aktywa_B_II_Inne'), -- Pozostałe należności inne
-- Środki pieniężne
(7, 'Aktywa_B_III_1_C'), -- Środki pieniężne duża
(7, 'Aktywa_B_III_A_1'), -- Środki pieniężne mała
-- RMK
(8, 'Aktywa_A_V_1'), -- Aktywa z tyt. pod. odroczonego
(8, 'Aktywa_A_V_Inne'), -- Inne RMK czynne długie
(8, 'Aktywa_B_IV'), -- Inne RMK czynne krótkie
-- Akcje własne
(9, 'Aktywa_C'), -- Należne wpłaty na kapitał podstawowy
(9, 'Aktywa_D'), -- Udziały (akcje) własne
-- Kapitał własny
(10, 'Pasywa_A_I'), -- Kapitał (fundusz) podstawowy
(10, 'Pasywa_A_II'), -- Kapitał zapasowy
(10, 'Pasywa_A_III'), -- Kapitał z aktualizacji wyceny
(10, 'Pasywa_A_IV'), -- Pozostałe kapitały (fundusze) rezerwowe
(10, 'Pasywa_A_V'), -- Zysk (strata) z lat ubiegłych
(10, 'Pasywa_A_VI'), -- Zysk (strata) netto
(10, 'Pasywa_A_VII'), -- Odpisy z zysku netto w ciągu roku obrotowego (wielkość ujemna)
-- Rezerwy
(11, 'Pasywa_B_I_1'), -- Rezerwy z tytułu odroczonego podatku dochodowego
(11, 'Pasywa_B_I_2'), -- Rezerwa na świadczenia emerytalne i podobne
(11, 'Pasywa_B_I_Inne'), -- Pozostałe rezerwy
-- Kredyty i pożyczki
(12, 'Pasywa_B_II_1'), -- Jednostka mała - Długoterminowe kredyty i pożyczki
(12, 'Pasywa_B_II_3_A'), -- Zobowiązania długoterminowe - kredyty i pożyczki
(12, 'Pasywa_B_III_3_A'), -- Zobow. krótk. - kredyty i pożyczki
(12, 'Pasywa_B_III_A'), -- Zobow, krótk. - kredyty i pożyczki - jednostka mała
-- Zobowiązania z tytułu dostaw
(13, 'Pasywa_B_III_1_A'), -- zobowiazania z tyt dostaw krótk terminowe
(13, 'Pasywa_B_III_2_A'), -- zobowiazania z tyt. zaangaz.
(13, 'Pasywa_B_III_3_D'), -- zobowiazania z tyt. dostaw i uslug
(13, 'Pasywa_B_III_B'), -- zobowiazania z tyt dostaw krótk terminowe jednostka mała
-- Zobowiązania inne
(14, 'Pasywa_B_II_Inne'), -- Inne zobowiązania długoterminowe
(14, 'Pasywa_B_III_3_G'), -- zobowiazania z tyt. podatków
(14, 'Pasywa_B_III_Inne'), -- Inne zobowiązania krótkoterminowe
-- RM bierne
(15, 'Pasywa_B_IV'), -- Rozliczenia międzyokresowe bierne
-- RZiS kalkulacyjny
(16, 'RZiSKalk.A'), -- Przychody netto ze sprzedaży produktów i towarów
(17, 'RZiSKalk.B'), -- Koszt sprzedanych produktów i towarów
(23, 'RZiSKalk.D'), -- Koszty sprzedaży
(23, 'RZiSKalk.C.Mala'), -- Koszty sprzedaży - mała
(23, 'RZiSKalk.E'), -- Koszty ogólnego zarządu
(23, 'RZiSKalk.D.Mala'), -- Koszty ogólnego zarządu - mała
(24, 'RZiSKalk.G'), -- Pozostałe przychody operacyjne
(24, 'RZiSKalk.F.Mala'), -- Pozostałe przychody operacyjne - mała
(24, 'RZiSKalk.H'), -- Pozostałe koszty operacyjne
(24, 'RZiSKalk.G.Mala'), -- Pozostałe koszty operacyjne - mała
(25, 'RZiSKalk.J'), -- Przychody finansowe
(25, 'RZiSKalk.H.Mala'), -- Przychody finansowe - mała
(25, 'RZiSKalk.K'), -- Koszty finansowe
(25, 'RZiSKalk.I.Mala'), -- Koszty finansowe - mała
(26, 'RZiSKalk.M'), -- Podatek dochodowy
(26, 'RZiSKalk.K.Mala'), -- Podatek dochodowy - mała
-- RZiS porównawczy
(18, 'RZiSPor.A'), -- Przychody netto ze sprzedaży produktów i towarów
(19, 'RZiSPor.B_I'), -- Amortyzacja
(20, 'RZiSPor.B_II'), -- Zużycie materiałów i energii
(20, 'RZiSPor.B_III'), -- Usługi obce
(21, 'RZiSPor.B_V'), -- Wynagrodzenia
(21, 'RZiSPor.B_VI'), -- Ubezp. społeczne i inne świadczenia
(21, 'RZiSPor.B_IV.Mala'), -- Wynagrodzenia - mała
(21, 'RZiSPor.B_V.Mala'), -- Ubezp. społeczne i inne świadczenia - mała
(22, 'RZiSPor.B_IV'), -- Podatki i opłaty
(22, 'RZiSPor.B_VII'), -- Pozostałe koszty
(22, 'RZiSPor.B_VIII'), -- Wartość sprzedanych towarów
(22, 'RZiSPor.B_VI.Mala'), -- Pozostałe koszty - mała
(24, 'RZiSPor.D'), -- Pozostałe przychody operacyjne
(24, 'RZiSPor.E'), -- Pozostałe koszty operacyjne
(25, 'RZiSPor.G'), -- Przychody finansowe
(25, 'RZiSPor.H'), -- Koszty finansowe
(25, 'RZiSPor.F.Mala'), -- Przychody finansowe - mała
(25, 'RZiSPor.G.Mala'), -- Koszty finansowe - mała
(26, 'RZiSPor.J'), -- Podatek dochodowy
(26, 'RZiSPor.I.Mala'); -- Podatek dochodowy - mała




