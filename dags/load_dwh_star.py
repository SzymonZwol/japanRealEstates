from airflow.decorators import dag, task
from pendulum import datetime
import pyodbc
from airflow.models import Variable

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,   # manual
    catchup=False,
    tags=["etl", "sqlserver", "japan", "dwh"],
)
def load_dwh_star():

    def get_conn():
        SERVER = Variable.get("DB_SERVER")
        DB     = Variable.get("DB_NAME")
        USER   = Variable.get("DB_USER")
        PWD    = Variable.get("DB_PASSWORD")

        conn_str = (
            "Driver={ODBC Driver 18 for SQL Server};"
            f"Server=tcp:{SERVER},1433;"
            f"Database={DB};"
            f"Uid={USER};"
            f"Pwd={PWD};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        return pyodbc.connect(conn_str)

    @task
    def drop_fact_indexes():
        sql = """
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FACT_CZAS' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            DROP INDEX IX_FACT_CZAS ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FACT_LOKALIZACJA' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            DROP INDEX IX_FACT_LOKALIZACJA ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FACT_TYP_NIERUCHOMOSCI' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            DROP INDEX IX_FACT_TYP_NIERUCHOMOSCI ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FACT_BUDYNEK' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            DROP INDEX IX_FACT_BUDYNEK ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FACT_PRZEZNACZENIE' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            DROP INDEX IX_FACT_PRZEZNACZENIE ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FACT_DOSTEPNOSC' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            DROP INDEX IX_FACT_DOSTEPNOSC ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;
        IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_FACT_PLANOWANIE' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            DROP INDEX IX_FACT_PLANOWANIE ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;
        """
        cn = get_conn()
        cur = cn.cursor()
        cur.execute(sql)
        cn.commit()
        cur.close()
        cn.close()

    @task
    def truncate_dwh():
        # FACT - TRUNCATE
        # DIM - DELETE (bezpieczne przy FK)
        sql = """
        TRUNCATE TABLE dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;

        DELETE FROM dwh.DIM_PLANOWANIE;
        DELETE FROM dwh.DIM_DOSTEPNOSC_TRANSPORTU;
        DELETE FROM dwh.DIM_PRZEZNACZENIE;
        DELETE FROM dwh.DIM_BUDYNEK;
        DELETE FROM dwh.DIM_TYP_NIERUCHOMOSCI;
        DELETE FROM dwh.DIM_LOKALIZACJA;
        DELETE FROM dwh.DIM_CZAS;
        """
        cn = get_conn()
        cur = cn.cursor()
        cur.execute(sql)
        cn.commit()
        cur.close()
        cn.close()

    @task
    def load_dimensions():
        sql = """
        SET NOCOUNT ON;

        -- DIM_CZAS
        INSERT INTO dwh.DIM_CZAS (rok, kwartal)
        SELECT DISTINCT
            COALESCE(r.[Year], -1) AS rok,
            COALESCE(r.[Quarter], 0) AS kwartal
        FROM stg.transactions_raw r;

        -- DIM_LOKALIZACJA (KANONICZNA NORMALIZACJA + GROUP BY)
        INSERT INTO dwh.DIM_LOKALIZACJA (prefektura, gmina_miasto, dzielnica, kod_gminy)
        SELECT
            prefektura,
            gmina_miasto,
            dzielnica,
            kod_gminy
        FROM
        (
            SELECT
                UPPER(LTRIM(RTRIM(REPLACE(REPLACE(COALESCE(NULLIF(r.[Prefecture], ''), N'Nieznana'), CHAR(9), ' '), '  ', ' ')))) AS prefektura,
                UPPER(LTRIM(RTRIM(REPLACE(REPLACE(COALESCE(NULLIF(r.[Municipality], ''), N'Nieznana'), CHAR(9), ' '), '  ', ' ')))) AS gmina_miasto,
                UPPER(LTRIM(RTRIM(
                    REPLACE(
                        REPLACE(
                            REPLACE(COALESCE(NULLIF(r.[DistrictName], ''), N'Nieznana'), ', ', ','),
                            CHAR(9), ' '
                        ),
                        '  ', ' '
                    )
                ))) AS dzielnica,
                COALESCE(r.[MunicipalityCode], -1) AS kod_gminy
            FROM stg.transactions_raw r
        ) s
        GROUP BY
            prefektura, gmina_miasto, dzielnica, kod_gminy;

        -- DIM_TYP_NIERUCHOMOSCI
        INSERT INTO dwh.DIM_TYP_NIERUCHOMOSCI (typ_nieruchomosci, uklad_pomieszczen, ksztalt_dzialki)
        SELECT DISTINCT
            COALESCE(NULLIF(LTRIM(RTRIM(r.[Type])), ''), N'Nieznany')      AS typ_nieruchomosci,
            COALESCE(NULLIF(LTRIM(RTRIM(r.[FloorPlan])), ''), N'Nieznany') AS uklad_pomieszczen,
            COALESCE(NULLIF(LTRIM(RTRIM(r.[LandShape])), ''), N'Nieznany') AS ksztalt_dzialki
        FROM stg.transactions_raw r;

        -- DIM_BUDYNEK
        INSERT INTO dwh.DIM_BUDYNEK (rok_budowy, przedwojenny_flag, konstrukcja, remont_flag)
        SELECT DISTINCT
            COALESCE(r.[BuildingYear], -1) AS rok_budowy,
            COALESCE(r.[PrewarBuilding], CAST(0 AS bit)) AS przedwojenny_flag,
            COALESCE(NULLIF(LTRIM(RTRIM(r.[Structure])), ''), N'Nieznany') AS konstrukcja,
            CASE
                WHEN NULLIF(LTRIM(RTRIM(r.[Renovation])), '') IS NULL THEN CAST(0 AS bit)
                WHEN UPPER(LTRIM(RTRIM(r.[Renovation]))) IN ('YES','Y','TRUE','1') THEN CAST(1 AS bit)
                ELSE CAST(0 AS bit)
            END AS remont_flag
        FROM stg.transactions_raw r;

        -- DIM_PRZEZNACZENIE
        INSERT INTO dwh.DIM_PRZEZNACZENIE (rodzaj_uzytkowania, cel_uzytkowania)
        SELECT DISTINCT
            COALESCE(NULLIF(LTRIM(RTRIM(r.[Use])), ''), N'Nieznany')     AS rodzaj_uzytkowania,
            COALESCE(NULLIF(LTRIM(RTRIM(r.[Purpose])), ''), N'Nieznany') AS cel_uzytkowania
        FROM stg.transactions_raw r;

        -- DIM_DOSTEPNOSC_TRANSPORTU
        INSERT INTO dwh.DIM_DOSTEPNOSC_TRANSPORTU (przedzial_czasu_do_stacji)
        SELECT DISTINCT
            CASE
                WHEN r.[MinTimeToNearestStation] IS NULL THEN N'Nieznany'
                WHEN r.[MinTimeToNearestStation] <= 5  THEN N'0-5 min'
                WHEN r.[MinTimeToNearestStation] <= 10 THEN N'6-10 min'
                WHEN r.[MinTimeToNearestStation] <= 15 THEN N'11-15 min'
                WHEN r.[MinTimeToNearestStation] <= 20 THEN N'16-20 min'
                WHEN r.[MinTimeToNearestStation] <= 30 THEN N'21-30 min'
                ELSE N'31+ min'
            END
        FROM stg.transactions_raw r;

        -- DIM_PLANOWANIE
        INSERT INTO dwh.DIM_PLANOWANIE
        (
            plan_miejscowy,
            kierunek_drogi,
            klasyfikacja_terenu,
            klasa_wskaznika_zabudowy,
            klasa_intensywnosci_zabudowy,
            klasa_szerokosci_drogi
        )
        SELECT DISTINCT
            COALESCE(NULLIF(LTRIM(RTRIM(r.[CityPlanning])), ''), N'Nieznany')    AS plan_miejscowy,
            COALESCE(NULLIF(LTRIM(RTRIM(r.[Direction])), ''), N'Nieznany')       AS kierunek_drogi,
            COALESCE(NULLIF(LTRIM(RTRIM(r.[Classification])), ''), N'Nieznany')  AS klasyfikacja_terenu,
            CASE
                WHEN r.[CoverageRatio] IS NULL THEN N'BRAK'
                WHEN r.[CoverageRatio] <= 30 THEN N'0-30'
                WHEN r.[CoverageRatio] <= 50 THEN N'31-50'
                WHEN r.[CoverageRatio] <= 70 THEN N'51-70'
                ELSE N'71+'
            END AS klasa_wskaznika_zabudowy,
            CASE
                WHEN r.[FloorAreaRatio] IS NULL THEN N'BRAK'
                WHEN r.[FloorAreaRatio] <= 100 THEN N'0-100'
                WHEN r.[FloorAreaRatio] <= 200 THEN N'101-200'
                WHEN r.[FloorAreaRatio] <= 300 THEN N'201-300'
                ELSE N'301+'
            END AS klasa_intensywnosci_zabudowy,
            CASE
                WHEN r.[Breadth] IS NULL THEN N'BRAK'
                WHEN r.[Breadth] < 4 THEN N'<4'
                WHEN r.[Breadth] < 6 THEN N'4-<6'
                WHEN r.[Breadth] < 8 THEN N'6-<8'
                ELSE N'8+'
            END AS klasa_szerokosci_drogi
        FROM stg.transactions_raw r;
        """
        cn = get_conn()
        cur = cn.cursor()
        cur.execute(sql)
        cn.commit()
        cur.close()
        cn.close()

    @task
    def load_fact():
        sql = """
        SET NOCOUNT ON;

        INSERT INTO dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI
        (
            czas_id,
            lokalizacja_id,
            typ_nieruchomosci_id,
            budynek_id,
            przeznaczenie_id,
            dostepnosc_id,
            planowanie_id,
            liczba_transakcji,
            cena_transakcji,
            powierzchnia,
            powierzchnia_calkowita,
            front_dzialki
        )
        SELECT
            c.czas_id,
            l.lokalizacja_id,
            t.typ_nieruchomosci_id,
            b.budynek_id,
            p.przeznaczenie_id,
            d.dostepnosc_id,
            pl.planowanie_id,
            1 AS liczba_transakcji,
            COALESCE(r.TradePrice, 0) AS cena_transakcji,
            COALESCE(r.Area, 0) AS powierzchnia,
            COALESCE(r.TotalFloorArea, 0) AS powierzchnia_calkowita,
            COALESCE(r.Frontage, 0) AS front_dzialki
        FROM stg.transactions_raw r
        JOIN dwh.DIM_CZAS c
          ON c.rok = COALESCE(r.[Year], -1)
         AND c.kwartal = COALESCE(r.[Quarter], 0)
        JOIN dwh.DIM_LOKALIZACJA l
          ON l.prefektura = UPPER(LTRIM(RTRIM(REPLACE(REPLACE(COALESCE(NULLIF(r.[Prefecture], ''), N'Nieznana'), CHAR(9), ' '), '  ', ' '))))
         AND l.gmina_miasto = UPPER(LTRIM(RTRIM(REPLACE(REPLACE(COALESCE(NULLIF(r.[Municipality], ''), N'Nieznana'), CHAR(9), ' '), '  ', ' '))))
         AND l.dzielnica = UPPER(LTRIM(RTRIM(
                REPLACE(
                    REPLACE(
                        REPLACE(COALESCE(NULLIF(r.[DistrictName], ''), N'Nieznana'), ', ', ','),
                        CHAR(9), ' '
                    ),
                    '  ', ' '
                )
            )))
         AND l.kod_gminy = COALESCE(r.[MunicipalityCode], -1)
        JOIN dwh.DIM_TYP_NIERUCHOMOSCI t
          ON t.typ_nieruchomosci = COALESCE(NULLIF(LTRIM(RTRIM(r.[Type])), ''), N'Nieznany')
         AND t.uklad_pomieszczen = COALESCE(NULLIF(LTRIM(RTRIM(r.[FloorPlan])), ''), N'Nieznany')
         AND t.ksztalt_dzialki = COALESCE(NULLIF(LTRIM(RTRIM(r.[LandShape])), ''), N'Nieznany')
        JOIN dwh.DIM_BUDYNEK b
          ON b.rok_budowy = COALESCE(r.[BuildingYear], -1)
         AND b.przedwojenny_flag = COALESCE(r.[PrewarBuilding], CAST(0 AS bit))
         AND b.konstrukcja = COALESCE(NULLIF(LTRIM(RTRIM(r.[Structure])), ''), N'Nieznany')
         AND b.remont_flag = CASE
                WHEN NULLIF(LTRIM(RTRIM(r.[Renovation])), '') IS NULL THEN CAST(0 AS bit)
                WHEN UPPER(LTRIM(RTRIM(r.[Renovation]))) IN ('YES','Y','TRUE','1') THEN CAST(1 AS bit)
                ELSE CAST(0 AS bit)
            END
        JOIN dwh.DIM_PRZEZNACZENIE p
          ON p.rodzaj_uzytkowania = COALESCE(NULLIF(LTRIM(RTRIM(r.[Use])), ''), N'Nieznany')
         AND p.cel_uzytkowania = COALESCE(NULLIF(LTRIM(RTRIM(r.[Purpose])), ''), N'Nieznany')
        JOIN dwh.DIM_DOSTEPNOSC_TRANSPORTU d
          ON d.przedzial_czasu_do_stacji = CASE
                WHEN r.[MinTimeToNearestStation] IS NULL THEN N'Nieznany'
                WHEN r.[MinTimeToNearestStation] <= 5  THEN N'0-5 min'
                WHEN r.[MinTimeToNearestStation] <= 10 THEN N'6-10 min'
                WHEN r.[MinTimeToNearestStation] <= 15 THEN N'11-15 min'
                WHEN r.[MinTimeToNearestStation] <= 20 THEN N'16-20 min'
                WHEN r.[MinTimeToNearestStation] <= 30 THEN N'21-30 min'
                ELSE N'31+ min'
            END
        JOIN dwh.DIM_PLANOWANIE pl
          ON pl.plan_miejscowy = COALESCE(NULLIF(LTRIM(RTRIM(r.[CityPlanning])), ''), N'Nieznany')
         AND pl.kierunek_drogi = COALESCE(NULLIF(LTRIM(RTRIM(r.[Direction])), ''), N'Nieznany')
         AND pl.klasyfikacja_terenu = COALESCE(NULLIF(LTRIM(RTRIM(r.[Classification])), ''), N'Nieznany')
         AND pl.klasa_wskaznika_zabudowy = CASE
                WHEN r.[CoverageRatio] IS NULL THEN N'BRAK'
                WHEN r.[CoverageRatio] <= 30 THEN N'0-30'
                WHEN r.[CoverageRatio] <= 50 THEN N'31-50'
                WHEN r.[CoverageRatio] <= 70 THEN N'51-70'
                ELSE N'71+'
            END
         AND pl.klasa_intensywnosci_zabudowy = CASE
                WHEN r.[FloorAreaRatio] IS NULL THEN N'BRAK'
                WHEN r.[FloorAreaRatio] <= 100 THEN N'0-100'
                WHEN r.[FloorAreaRatio] <= 200 THEN N'101-200'
                WHEN r.[FloorAreaRatio] <= 300 THEN N'201-300'
                ELSE N'301+'
            END
         AND pl.klasa_szerokosci_drogi = CASE
                WHEN r.[Breadth] IS NULL THEN N'BRAK'
                WHEN r.[Breadth] < 4 THEN N'<4'
                WHEN r.[Breadth] < 6 THEN N'4-<6'
                WHEN r.[Breadth] < 8 THEN N'6-<8'
                ELSE N'8+'
            END;
        """
        cn = get_conn()
        cur = cn.cursor()
        cur.execute(sql)
        cn.commit()
        cur.close()
        cn.close()

    @task
    def create_fact_indexes():
        sql = """
        SET NOCOUNT ON;

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FACT_CZAS' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            CREATE INDEX IX_FACT_CZAS ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI (czas_id);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FACT_LOKALIZACJA' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            CREATE INDEX IX_FACT_LOKALIZACJA ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI (lokalizacja_id);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FACT_TYP_NIERUCHOMOSCI' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            CREATE INDEX IX_FACT_TYP_NIERUCHOMOSCI ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI (typ_nieruchomosci_id);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FACT_BUDYNEK' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            CREATE INDEX IX_FACT_BUDYNEK ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI (budynek_id);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FACT_PRZEZNACZENIE' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            CREATE INDEX IX_FACT_PRZEZNACZENIE ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI (przeznaczenie_id);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FACT_DOSTEPNOSC' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            CREATE INDEX IX_FACT_DOSTEPNOSC ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI (dostepnosc_id);

        IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_FACT_PLANOWANIE' AND object_id = OBJECT_ID('dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI'))
            CREATE INDEX IX_FACT_PLANOWANIE ON dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI (planowanie_id);
        """
        cn = get_conn()
        cur = cn.cursor()
        cur.execute(sql)
        cn.commit()
        cur.close()
        cn.close()

    @task
    def counts_check():
        cn = get_conn()
        cur = cn.cursor()

        for q in [
            "SELECT COUNT(*) FROM stg.transactions_raw;",
            "SELECT COUNT(*) FROM dwh.DIM_CZAS;",
            "SELECT COUNT(*) FROM dwh.DIM_LOKALIZACJA;",
            "SELECT COUNT(*) FROM dwh.DIM_TYP_NIERUCHOMOSCI;",
            "SELECT COUNT(*) FROM dwh.DIM_BUDYNEK;",
            "SELECT COUNT(*) FROM dwh.DIM_PRZEZNACZENIE;",
            "SELECT COUNT(*) FROM dwh.DIM_DOSTEPNOSC_TRANSPORTU;",
            "SELECT COUNT(*) FROM dwh.DIM_PLANOWANIE;",
            "SELECT COUNT(*) FROM dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI;",
        ]:
            cur.execute(q)
            print(q, cur.fetchone()[0])

        # sanity: stg vs fact (czy joiny nie ucinaja)
        cur.execute("""
            SELECT
              (SELECT COUNT(*) FROM stg.transactions_raw) AS stg_cnt,
              (SELECT COUNT(*) FROM dwh.FAKT_TRANSAKCJE_NIERUCHOMOSCI) AS fact_cnt;
        """)
        row = cur.fetchone()
        print("stg_cnt:", row[0], "fact_cnt:", row[1])

        cur.close()
        cn.close()

    drop_i = drop_fact_indexes()
    trunc = truncate_dwh()
    dims  = load_dimensions()
    fact  = load_fact()
    idx   = create_fact_indexes()
    chk   = counts_check()

    drop_i >> trunc >> dims >> fact >> idx >> chk

load_dwh_star()
