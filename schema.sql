-- 1. Tabele konfiguracyjne i słownikowe
-- ---------------------------------------------------------

CREATE TABLE Naglowek (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    KodFormularza TEXT,
    WariantFormularza INTEGER,
    CelZlozenia INTEGER,
    DataWytworzeniaJPK DATETIME,
    DataOd DATE,
    DataDo DATE
);

-- Tabele główne bazy danych
CREATE TABLE IF NOT EXISTS Config (
    Klucz TEXT PRIMARY KEY,
    Wartosc TEXT
);

INSERT INTO Config (Klucz, Wartosc) VALUES ('Program', 'JPK_KR_PD Audytor');
INSERT INTO Config (Klucz, Wartosc) VALUES ('Wersja', '1.0');

CREATE TABLE Podmiot (
    NIP TEXT PRIMARY KEY,
    PelnaNazwa TEXT,
    REGON TEXT,
    KodKraju TEXT,
    Wojewodztwo TEXT,
    Powiat TEXT,
    Gmina TEXT,
    Ulica TEXT,
    NrDomu TEXT,
    NrLokalu TEXT,
    Miejscowosc TEXT,
    KodPocztowy TEXT
);

-- Tabela Kontrahenci
-- Zgodna z elementem <Kontrahent> w XSD (T_1, T_2, T_3)
CREATE TABLE Kontrahenci (
    T_1 TEXT PRIMARY KEY,          -- Identyfikator kontrahenta (unikalny w pliku)
    T_2 TEXT,                      -- Kod Kraju (np. PL)
    T_3 TEXT                       -- Numer Identyfikacji Podatkowej (NIP)
);

-- Tabela Słownik Kategorii (Znaczniki S_12)
-- Rozbudowana do 5 poziomów zgodnie z życzeniem
CREATE TABLE Slownik_Kategorii (
    Kod TEXT PRIMARY KEY,          -- np. 'B3AA1_W'
    TypSlownika TEXT,              -- np. 'S_12_1'
    OpisPelny TEXT,                -- Cały string z XSD
    Poziom1 TEXT,                  -- np. 'AKTYWA'
    Poziom2 TEXT,                  -- np. 'Aktywa trwałe'
    Poziom3 TEXT,                  -- np. 'Wartości niematerialne'
    Poziom4 TEXT,                  -- np. 'Wartość firmy'
    Poziom5 TEXT                   -- Dodatkowy poziom szczegółowości
);

-- Indeks hierarchiczny do filtrowania
CREATE INDEX idx_slownik_hierarchia ON Slownik_Kategorii(Poziom1, Poziom2, Poziom3, Poziom4, Poziom5);


-- 2. Księga Główna (ZOiS)
-- ---------------------------------------------------------

CREATE TABLE ZOiS (
    S_1 TEXT PRIMARY KEY,          -- Numer Konta
    S_2 TEXT,                      -- Nazwa Konta
    S_3 TEXT,                      -- Konto Nadrzędne (tekst)
    
    -- Bilans Otwarcia
    S_4 DECIMAL(18, 2) DEFAULT 0,  
    S_5 DECIMAL(18, 2) DEFAULT 0,  
    
    -- Obroty
    S_6 DECIMAL(18, 2) DEFAULT 0,  -- Obroty Wn
    S_7 DECIMAL(18, 2) DEFAULT 0,  -- Obroty Ma
    S_8 DECIMAL(18, 2) DEFAULT 0,  -- Obroty Narastające Wn
    S_9 DECIMAL(18, 2) DEFAULT 0,  -- Obroty Narastające Ma

    --Salda  
    S_10 DECIMAL(18, 2) DEFAULT 0, -- Saldo Wn
    S_11 DECIMAL(18, 2) DEFAULT 0, -- Saldo Ma
    
    -- Znaczniki Podatkowe
    S_12_1 TEXT,                   
    S_12_2 TEXT,                   
    S_12_3 TEXT,                   
    
    TypKonta TEXT,                 
    IsAnalytical INTEGER DEFAULT 0,  -- 1 = Analityczne, 0 = Syntetyczne
    GrupaKont TEXT GENERATED ALWAYS AS (SUBSTR(S_1, 1, 3)) STORED,  -- Grupa kont syntetycznych
    
    FOREIGN KEY (S_12_1) REFERENCES Slownik_Kategorii(Kod),
    FOREIGN KEY (S_12_2) REFERENCES Slownik_Kategorii(Kod),
    FOREIGN KEY (S_12_3) REFERENCES Slownik_Kategorii(Kod)
);

CREATE INDEX idx_zois_znacznik_glowny ON ZOiS(S_12_1);
CREATE INDEX IF NOT EXISTS idx_zois_grupakont ON ZOiS(GrupaKont);


-- 3. Dziennik i Zapisy
-- ---------------------------------------------------------

CREATE TABLE Dziennik (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    D_1 TEXT,                      -- Numer zapisu
    D_2 TEXT,                      -- Opis dziennika
    
    -- D_3 to Identyfikator kontrahenta (Relacja do tabeli Kontrahenci)
    D_3 TEXT,                      
    
    D_4 TEXT,                      -- Dowód
    D_5 TEXT,                      -- Dowód źródłowy
    D_6 DATE,                      -- Data operacji
    D_7 DATE,                      -- Data dowodu
    D_8 DATE,                      -- Data księgowania
    D_9 TEXT,                      -- Operator
    D_10 TEXT,                     -- Opis operacji
    D_11 DECIMAL(18, 2),           -- Kwota
    D_12 TEXT,                     -- KSeF
    
    FOREIGN KEY (D_3) REFERENCES Kontrahenci(T_1)
);

CREATE TABLE Zapisy (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Dziennik_Id INTEGER,           
    
    Z_1 TEXT,                      -- Lp zapisu
    Z_2 TEXT,                      -- Opis
    Z_3 TEXT,                      -- Identyfikator konta (FK do ZOiS)
    
    Z_4 DECIMAL(18, 2) DEFAULT 0,  -- Kwota Wn
    Z_5 TEXT,                      
    Z_6 DECIMAL(18, 2) DEFAULT 0,  
    
    Z_7 DECIMAL(18, 2) DEFAULT 0,  -- Kwota Ma
    Z_8 TEXT,                      
    Z_9 DECIMAL(18, 2) DEFAULT 0,  
    
    Z_Data DATE,                   -- Data księgowania (zdenormalizowana z Dziennik.D_8)
    Z_DataMiesiac INTEGER,         -- Miesiąc księgowania
    Z_NrZapisu TEXT,               -- Numer zapisu dla powiązania (Legacy JPK)
    Z_GrupaKont TEXT,              -- Pierwsze 3 znaki konta (grupa kont - dawniej syntetyka)
    
    FOREIGN KEY (Dziennik_Id) REFERENCES Dziennik(Id) ON DELETE CASCADE,
    FOREIGN KEY (Z_3) REFERENCES ZOiS(S_1)
);

-- Wydajny indeks na grupę kont
CREATE INDEX idx_zapisy_grupakont ON Zapisy(Z_GrupaKont);

-- Wydajny indeks złożony dla filtrowania po koncie i sortowania po dacie
CREATE INDEX idx_zapisy_analiza ON Zapisy(Z_3, Z_Data);

-- Indeks dla szybkości generowania wykresów
CREATE INDEX idx_zapisy_wykres ON Zapisy(Z_DataMiesiac);


-- 4. Sekcje Sumaryczne i Kontrolne
-- ---------------------------------------------------------

CREATE TABLE RPD (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    K_1 DECIMAL(18, 2), -- Przychody razem
    K_2 DECIMAL(18, 2),
    K_3 DECIMAL(18, 2),
    K_4 DECIMAL(18, 2),
    K_5 DECIMAL(18, 2),
    K_6 DECIMAL(18, 2), -- Koszty razem
    K_7 DECIMAL(18, 2),
    K_8 DECIMAL(18, 2)
);

-- Tabela Ctrl (Sumy Kontrolne Pliku)
-- Dokładnie odwzorowuje element <Ctrl> z XSD
CREATE TABLE Ctrl (
    C_1 INTEGER,        -- Liczba dzienników (lub inna suma ilościowa)
    C_2 DECIMAL(18, 2), -- Suma kwot Dziennika
    C_3 INTEGER,        -- Liczba zapisów ZOiS (lub inna suma ilościowa)
    C_4 DECIMAL(18, 2), -- Suma ZOiS Wn
    C_5 DECIMAL(18, 2)  -- Suma ZOiS Ma
);

-- 5. Parametry Badania i Konfiguracja Audytu
-- ---------------------------------------------------------

CREATE TABLE ParametryBadania (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Klucz TEXT UNIQUE NOT NULL,    -- Np. 'data_badania', 'osoba_badajaca', 'istotnosc_kwota'
    Opis TEXT,                     -- Czytelna etykieta np. 'Data zakończenia badania'
    
    -- Kolumny wartościowe
    Tekst TEXT,
    Liczba INTEGER,
    Kwota DECIMAL(18, 2),
    Procent DECIMAL(10, 4),      -- Tutaj zapiszesz 0.9500
    WartoscData DATE
);

-- 6. Indeksy wydajnościowe (Optymalizacja odczytu)
-- ---------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_dziennik_d1 ON Dziennik(D_1);
CREATE INDEX IF NOT EXISTS idx_zapisy_nrzapisu ON Zapisy(Z_NrZapisu);
CREATE INDEX IF NOT EXISTS idx_zapisy_data_val ON Zapisy(Z_Data);
CREATE INDEX IF NOT EXISTS idx_zois_s1 ON ZOiS(S_1);
CREATE INDEX IF NOT EXISTS idx_zois_s12_1 ON ZOiS(S_12_1);
CREATE INDEX IF NOT EXISTS idx_zois_typkonta ON ZOiS(TypKonta);
CREATE INDEX IF NOT EXISTS idx_zapisy_dziennikid ON Zapisy(Dziennik_Id);


-- Reset i aktualizacja widoku (usuwamy stary, aby wymusić nową strukturę)
DROP VIEW IF EXISTS v_zapisy_pelne;
CREATE VIEW v_zapisy_pelne AS
SELECT 
    z.Id AS id_zapisu,            -- Identyfikator zapisu w systemie
    z.Id,                         -- Alias dla kompatybilności
    z.Dziennik_Id,                -- Identyfikator dziennika (FK)
    z.Z_3,                        -- Kolumna techniczna konta
    z.Z_3 AS Numer_Konta,         -- Pełny numer konta księgowego
    z.Z_2,                        -- Alias dla kompatybilnosci
    z.Z_2 AS Opis_Zapisu,         -- Opis szczegółowy zapisu
    z.Z_Data,                     -- Data zapisu
    z.Z_DataMiesiac,              -- Miesiąc zapisu
    z.Z_GrupaKont,                -- Grupa kont (3 znaki)
    z.Z_4,                        -- Alias dla kompatybilności
    z.Z_4 AS Kwota_Wn,            -- Kwota po stronie Winien
    z.Z_5,                        -- Alias dla kompatybilności
    z.Z_5 AS Kwota_Waluta_Wn,     -- Kwota waluta Wn
    z.Z_6,                        -- Alias dla kompatybilności
    z.Z_6 AS Kod_Waluty_Wn,       -- Kod waluty Wn
    z.Z_7,                        -- Alias dla kompatybilności
    z.Z_7 AS Kwota_Ma,            -- Kwota po stronie Ma
    z.Z_8,                        -- Alias dla kompatybilności
    z.Z_8 AS Kwota_Waluta_Ma,     -- Kwota waluta Ma
    z.Z_9,                        -- Alias dla kompatybilności
    z.Z_9 AS Kod_Waluty_Ma,       -- Kod waluty Ma
    (z.Z_4 + z.Z_7) AS Kwota,     -- wartość operacji
    d.D_1 AS Numer_Dziennika,     -- Numer dziennika
    d.D_1,                        -- Alias dla kompatybilności
    d.D_8 AS Data_ksiegowania,    -- Data księgowania
    d.D_4 AS Dowod,               -- Numer dowodu
    d.D_10 AS Operacja,           -- Rodzaj operacji
    d.D_3 AS Kontrahent,          -- Nazwa kontrahenta
    d.D_12 AS KSeF                -- Numer KSeF
FROM Zapisy z
LEFT JOIN Dziennik d ON z.Dziennik_Id = d.Id
WHERE d.D_1 NOT LIKE 'BO%';

-- =============================================================================
-- SEKCJA: OBSZARY BADANIA (ARCHITEKTURA MAPOWANIA DZIEDZICZONEGO)
-- =============================================================================

CREATE TABLE IF NOT EXISTS Obszary (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    Nazwa TEXT UNIQUE NOT NULL,
    Typ TEXT,
    Czy_Systemowy BOOLEAN DEFAULT 0
);

CREATE TABLE IF NOT EXISTS Sprawozdanie_Pozycje (
    XmlTag TEXT PRIMARY KEY,
    Nazwa TEXT NOT NULL,
    Kwota_RB DECIMAL(18,2) DEFAULT 0,
    Kwota_RP DECIMAL(18,2) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS Obszary_Sprawozdanie (
    Obszar_Id INTEGER,
    XmlTag TEXT,
    PRIMARY KEY (Obszar_Id, XmlTag),
    FOREIGN KEY (Obszar_Id) REFERENCES Obszary(Id) ON DELETE CASCADE,
    FOREIGN KEY (XmlTag) REFERENCES Sprawozdanie_Pozycje(XmlTag) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS ZOiS_Mapowanie_Obszar (
    Obszar_Id INTEGER,
    ZOiS_S1 TEXT,
    Strona_Salda TEXT CHECK(Strona_Salda IN ('TYLKO_WN', 'TYLKO_MA', 'PERSALDO_WN_MA', 'PERSALDO_MA_WN')),
    UNIQUE(ZOiS_S1, Strona_Salda),
    FOREIGN KEY (Obszar_Id) REFERENCES Obszary(Id) ON DELETE CASCADE
);

DROP VIEW IF EXISTS v_zois_resolved_mapping;
CREATE VIEW v_zois_resolved_mapping AS
WITH AllMatches AS (
    SELECT 
        z.S_1, 
        z.S_2, 
        z.S_3,
        z.S_10, 
        z.S_11,
        m.Obszar_Id,
        m.Strona_Salda,
        m.ZOiS_S1 AS Source_Konto,
        CASE WHEN z.S_1 = m.ZOiS_S1 THEN 1 ELSE 0 END AS Is_Direct,
        ROW_NUMBER() OVER (PARTITION BY z.S_1 ORDER BY LENGTH(m.ZOiS_S1) DESC) as rnk
    FROM ZOiS z
    JOIN ZOiS_Mapowanie_Obszar m ON (z.S_1 = m.ZOiS_S1 OR z.S_1 LIKE m.ZOiS_S1 || '-%')
)
SELECT S_1, S_2, S_3, S_10, S_11, Obszar_Id, Strona_Salda, Is_Direct, Source_Konto
FROM AllMatches
WHERE rnk = 1;

DROP VIEW IF EXISTS v_obszary_rekoncyliacja;
CREATE VIEW v_obszary_rekoncyliacja AS
SELECT 
    o.Id,
    o.Nazwa,
    o.Typ,
    (SELECT SUM(sp.Kwota_RB) 
     FROM Obszary_Sprawozdanie os
     JOIN Sprawozdanie_Pozycje sp ON os.XmlTag = sp.XmlTag
     WHERE os.Obszar_Id = o.Id) AS Suma_Sprawozdanie,
    (SELECT SUM(
        CASE 
            WHEN rm.Strona_Salda = 'TYLKO_WN' THEN rm.S_10
            WHEN rm.Strona_Salda = 'TYLKO_MA' THEN rm.S_11
            WHEN rm.Strona_Salda = 'PERSALDO_WN_MA' THEN (rm.S_10 - rm.S_11)
            WHEN rm.Strona_Salda = 'PERSALDO_MA_WN' THEN (rm.S_11 - rm.S_10)
            ELSE 0 
        END)
     FROM v_zois_resolved_mapping rm
     WHERE rm.Obszar_Id = o.Id
     AND rm.S_1 NOT IN (SELECT S_3 FROM ZOiS WHERE S_3 IS NOT NULL AND S_3 != '')
    ) AS Suma_ZOiS,
    (
        COALESCE((SELECT SUM(
            CASE 
                WHEN rm.Strona_Salda = 'TYLKO_WN' THEN rm.S_10
                WHEN rm.Strona_Salda = 'TYLKO_MA' THEN rm.S_11
                WHEN rm.Strona_Salda = 'PERSALDO_WN_MA' THEN (rm.S_10 - rm.S_11)
                WHEN rm.Strona_Salda = 'PERSALDO_MA_WN' THEN (rm.S_11 - rm.S_10)
                ELSE 0 END) FROM v_zois_resolved_mapping rm WHERE rm.Obszar_Id = o.Id AND rm.S_1 NOT IN (SELECT S_3 FROM ZOiS WHERE S_3 IS NOT NULL AND S_3 != '')), 0)
        - 
        COALESCE((SELECT SUM(sp.Kwota_RB) FROM Obszary_Sprawozdanie os JOIN Sprawozdanie_Pozycje sp ON os.XmlTag = sp.XmlTag WHERE os.Obszar_Id = o.Id), 0)
    ) AS Odchylenie
FROM Obszary o;

-- Dodanie predefiniowanych obszarów badania
INSERT INTO Obszary (Nazwa, Typ, Czy_Systemowy) VALUES
-- AKTYWA
('WNiP', 'Aktywa', 1),
('Środki Trwałe', 'Aktywa', 1),
('Inwestycje długoterminowe', 'Aktywa', 1),
('Zapasy', 'Aktywa', 1),
('Należności handlowe', 'Aktywa', 1),
('Inne należności', 'Aktywa', 1),
('Środki Pieniężne', 'Aktywa', 1),
('Krótkoterminowe rozliczenia międzyokresowe', 'Aktywa', 1),

-- PASYWA
('Kapitał własny', 'Pasywa', 1),
('Rezerwy na zobowiązania', 'Pasywa', 1),
('Zobowiązania długoterminowe', 'Pasywa', 1),
('Zobowiązania krótkoterminowe', 'Pasywa', 1),
('Rozliczenia międzyokresowe', 'Pasywa', 1),

-- RZiS
('Przychody ze sprzedaży', 'RZiS', 1),
('Koszty własne sprzedaży', 'RZiS', 1),
('Koszty ogólnego zarządu', 'RZiS', 1),
('Amortyzacja', 'RZiS', 1),
('Pozostałe przychody i koszty operacyjne', 'RZiS', 1),
('Przychody i koszty finansowe', 'RZiS', 1),
('Podatek dochodowy', 'RZiS', 1);