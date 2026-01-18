from __future__ import annotations

import os
from glob import glob
from datetime import datetime

import pandas as pd
import pyodbc

from airflow.sdk import dag, task, Variable
import logging

log = logging.getLogger(__name__)

DATA_DIR = "/usr/local/airflow/include/data"
EXCLUDE_FILES = {"prefecture_code.csv"}
BATCH_SIZE = 50_000  # większy batch = mniej commitów

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
INT_COLS = ["No","MunicipalityCode","MinTimeToNearestStation","MaxTimeToNearestStation","TradePrice","Year","Quarter","BuildingYear"]
FLOAT_COLS = ["Area","UnitPrice","PricePerTsubo","Frontage","TotalFloorArea","CoverageRatio","FloorAreaRatio","Breadth"]
BIT_COLS = ["AreaIsGreaterFlag","FrontageIsGreaterFlag","TotalFloorAreaIsGreaterFlag","PrewarBuilding"]

def to_int_safe(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").astype("Int64")

def to_float_safe(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def py_value(x):
    if pd.isna(x):
        return None
    if isinstance(x, (pd.Timestamp,)):
        return x.to_pydatetime()
    if isinstance(x, (int, float, str, bool)):
        return x
    try:
        if hasattr(x, "item"):
            return x.item()
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
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["etl", "sqlserver", "transactions"],
)
def load_transactions_raw_sequential():

    @task
    def load_all_files() -> dict:
        files = sorted(glob(os.path.join(DATA_DIR, "*.csv")))
        files = [f for f in files if os.path.basename(f) not in EXCLUDE_FILES]
        if not files:
            raise ValueError(f"No CSV files found in {DATA_DIR} (excluded: {sorted(EXCLUDE_FILES)})")

        server = Variable.get("DB_SERVER")
        db = Variable.get("DB_NAME")
        user = Variable.get("DB_USER")
        pwd = Variable.get("DB_PASSWORD")

        conn_str = build_conn_str(server, db, user, pwd)

        start_all = datetime.now()
        total_rows_all = 0

        cn = pyodbc.connect(conn_str)
        try:
            cur = cn.cursor()
            cur.fast_executemany = True

            # TRUNCATE raz na starcie
            log.info("Truncating stg.transactions_raw ...")
            cur.execute("TRUNCATE TABLE stg.transactions_raw;")
            cn.commit()

            insert_sql = f"""
            INSERT INTO stg.transactions_raw ({",".join("["+c+"]" for c in TARGET_COLS)})
            VALUES ({",".join("?" for _ in TARGET_COLS)})
            """

            for idx, file_path in enumerate(files, start=1):
                file_name = os.path.basename(file_path)
                start_file = datetime.now()

                log.info("==== [%d/%d] START %s ====", idx, len(files), file_name)

                df = pd.read_csv(file_path, low_memory=False)
                df["source_file"] = file_name

                for c in TARGET_COLS:
                    if c not in df.columns:
                        df[c] = pd.NA
                df = df[TARGET_COLS]

                for c in INT_COLS:
                    df[c] = to_int_safe(df[c])
                for c in FLOAT_COLS:
                    df[c] = to_float_safe(df[c])
                for c in BIT_COLS:
                    df[c] = to_int_safe(df[c])

                total = len(df)
                total_rows_all += total
                log.info("[%s] rows=%s", file_name, f"{total:,}")

                t0 = datetime.now()
                done = 0
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
                    log.info("[%s] progress %s/%s | elapsed %ss | rate %s rows/s",
                             file_name, f"{done:,}", f"{total:,}", int(elapsed), f"{rps:,}")

                file_elapsed = int((datetime.now() - start_file).total_seconds())
                log.info("==== [%d/%d] DONE %s | time %ss ====", idx, len(files), file_name, file_elapsed)

            all_elapsed = int((datetime.now() - start_all).total_seconds())
            log.info("===== SUMMARY =====")
            log.info("Files processed: %d", len(files))
            log.info("Total rows     : %s", f"{total_rows_all:,}")
            log.info("Total time (s) : %d", all_elapsed)

            return {"files": len(files), "rows": total_rows_all, "seconds": all_elapsed}
        finally:
            cn.close()

    load_all_files()

load_transactions_raw_sequential()
