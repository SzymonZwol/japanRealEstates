from __future__ import annotations

import os
from glob import glob
from datetime import datetime

import pandas as pd
import pyodbc

from airflow.sdk import dag, task, Variable
import logging

log = logging.getLogger(__name__)

# Pliki CSV są w repo: include/data/*.csv
# W kontenerze: /usr/local/airflow/include/data/*.csv
DATA_DIR = "/usr/local/airflow/include/data"

# Jeśli w tym samym folderze masz też prefecture_code.csv,
# to go odfiltrujemy (i ewentualnie inne nie-transactions).
EXCLUDE_FILES = {"prefecture_code.csv"}

TARGET_COLS = [
    "source_file",
    "No","Type","Region","MunicipalityCode","Prefecture","Municipality","DistrictName",
    "NearestStation","TimeToNearestStation","MinTimeToNearestStation","MaxTimeToNearestStation",
    "TradePrice","FloorPlan","Area","AreaIsGreaterFlag","UnitPrice","PricePerTsubo","LandShape",
    "Frontage","FrontageIsGreaterFlag","TotalFloorArea","TotalFloorAreaIsGreaterFlag",
    "BuildingYear","PrewarBuilding","Structure","Use","Purpose","Direction","Classification",
    "Breadth","CityPlanning","CoverageRatio","FloorAreaRatio","Period","Year","Quarter",
    "Renovation","Remarks"
]

INT_COLS = [
    "No","MunicipalityCode","MinTimeToNearestStation","MaxTimeToNearestStation",
    "TradePrice","Year","Quarter","BuildingYear"
]
FLOAT_COLS = [
    "Area","UnitPrice","PricePerTsubo","Frontage","TotalFloorArea",
    "CoverageRatio","FloorAreaRatio","Breadth"
]
BIT_COLS = [
    "AreaIsGreaterFlag","FrontageIsGreaterFlag","TotalFloorAreaIsGreaterFlag","PrewarBuilding"
]

BATCH_SIZE = 10_000


def to_int_safe(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")


def to_float_safe(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def py_value(x):
    """Convert pandas/numpy scalars to pure Python types for pyodbc."""
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp,)):
        return x.to_pydatetime()
    if isinstance(x, (int, float, str, bool)):
        return x
    try:
        if hasattr(x, "item"):
            v = x.item()
            if isinstance(v, (int, float, str, bool)) or v is None:
                return v
            return v
    except Exception:
        pass
    return str(x)


def build_conn_str(server: str, db: str, user: str, pwd: str) -> str:
    return (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={db};"
        f"Uid={user};"
        f"Pwd={pwd};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,   # manual trigger
    catchup=False,
    tags=["etl", "sqlserver", "transactions"],
    max_active_runs=1
)
def load_transactions_raw():

    @task
    def list_csv_files() -> list[str]:
        files = sorted(glob(os.path.join(DATA_DIR, "*.csv")))
        files = [f for f in files if os.path.basename(f) not in EXCLUDE_FILES]

        if not files:
            raise ValueError(f"No transaction CSV files found in {DATA_DIR}. "
                             f"Excluded: {sorted(EXCLUDE_FILES)}")

        log.info("Found %d CSV files in %s", len(files), DATA_DIR)
        for f in files[:10]:
            log.info("File: %s", os.path.basename(f))
        if len(files) > 10:
            log.info("... and %d more", len(files) - 10)

        return files

    @task
    def truncate_staging():
        server = Variable.get("DB_SERVER")
        db = Variable.get("DB_NAME")
        user = Variable.get("DB_USER")
        pwd = Variable.get("DB_PASSWORD")

        cn = pyodbc.connect(build_conn_str(server, db, user, pwd))
        try:
            cur = cn.cursor()
            cur.execute("TRUNCATE TABLE stg.transactions_raw;")
            cn.commit()
            log.info("Truncated table stg.transactions_raw")
        finally:
            cn.close()

    @task(pool="sqlserver_load")
    def load_one_file(file_path: str) -> dict:
        server = Variable.get("DB_SERVER")
        db = Variable.get("DB_NAME")
        user = Variable.get("DB_USER")
        pwd = Variable.get("DB_PASSWORD")

        conn_str = build_conn_str(server, db, user, pwd)

        file_name = os.path.basename(file_path)
        start_ts = datetime.now()

        log.info("==== START file %s ====", file_name)
        log.info("Reading CSV: %s", file_path)

        df = pd.read_csv(file_path, low_memory=False)
        df["source_file"] = file_name

        # brakujące kolumny -> NA
        for c in TARGET_COLS:
            if c not in df.columns:
                df[c] = pd.NA
        df = df[TARGET_COLS]

        # normalizacja typów
        for c in INT_COLS:
            df[c] = to_int_safe(df[c])
        for c in FLOAT_COLS:
            df[c] = to_float_safe(df[c])
        for c in BIT_COLS:
            df[c] = to_int_safe(df[c])

        total = len(df)
        log.info("File %s rows: %s", file_name, f"{total:,}")

        insert_sql = f"""
        INSERT INTO stg.transactions_raw ({",".join("["+c+"]" for c in TARGET_COLS)})
        VALUES ({",".join("?" for _ in TARGET_COLS)})
        """

        cn = pyodbc.connect(conn_str)
        try:
            cur = cn.cursor()
            cur.fast_executemany = True

            t0 = datetime.now()

            # batch insert, żeby nie robić ogromnej listy na raz
            for start in range(0, total, BATCH_SIZE):
                end = min(start + BATCH_SIZE, total)
                chunk = df.iloc[start:end]

                rows = [
                    tuple(py_value(v) for v in r)
                    for r in chunk.itertuples(index=False, name=None)
                ]

                cur.executemany(insert_sql, rows)
                cn.commit()

                done = end
                elapsed = (datetime.now() - t0).total_seconds()
                rps = int(done / elapsed) if elapsed > 0 else 0

                log.info(
                    "[%s] progress %s/%s | elapsed %ss | rate %s rows/s",
                    file_name,
                    f"{done:,}",
                    f"{total:,}",
                    int(elapsed),
                    f"{rps:,}",
                )

            total_elapsed = (datetime.now() - start_ts).total_seconds()
            log.info("==== DONE file %s | time %ss ====", file_name, int(total_elapsed))

            return {"file": file_name, "rows": total, "seconds": int(total_elapsed)}
        finally:
            cn.close()

    @task
    def summarize(results: list[dict]):
        total_files = len(results)
        total_rows = sum(r["rows"] for r in results)
        total_seconds = sum(r["seconds"] for r in results)

        log.info("===== LOAD SUMMARY =====")
        log.info("Files processed : %d", total_files)
        log.info("Total rows      : %s", f"{total_rows:,}")
        log.info("Total time (s)  : %d", total_seconds)

    files = list_csv_files()
    trunc = truncate_staging()

    # Dynamic mapping: jeden task na plik
    loads = load_one_file.expand(file_path=files)

    trunc >> loads
    summarize(loads)


load_transactions_raw()
