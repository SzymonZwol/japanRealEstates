from __future__ import annotations

import os
from glob import glob
from datetime import datetime, timedelta

import pandas as pd
import pyodbc

from airflow.sdk import dag, task, Variable
import logging

log = logging.getLogger(__name__)

DATA_DIR = "/usr/local/airflow/include/data"

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
    if pd.isna(x):
        return None
    if isinstance(x, pd.Timestamp):
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
    tags=["etl", "sqlserver", "transactions"],
)
def load_transactions_raw():

    @task
    def list_csv_files() -> list[str]:
        files = sorted(glob(os.path.join(DATA_DIR, "*.csv")))
        files = [f for f in files if os.path.basename(f) not in EXCLUDE_FILES]

        if not files:
            raise ValueError("No transaction CSV files found")

        log.info("Found %d CSV files", len(files))
        return files

    @task
    def truncate_staging():
        cn = pyodbc.connect(build_conn_str(
            Variable.get("DB_SERVER"),
            Variable.get("DB_NAME"),
            Variable.get("DB_USER"),
            Variable.get("DB_PASSWORD"),
        ))
        try:
            cur = cn.cursor()
            cur.execute("TRUNCATE TABLE stg.transactions_raw;")
            cn.commit()
            log.info("Truncated stg.transactions_raw")
        finally:
            cn.close()
    
    @task
    def load_one_file(file_path: str) -> dict:
        conn_str = build_conn_str(
            Variable.get("DB_SERVER"),
            Variable.get("DB_NAME"),
            Variable.get("DB_USER"),
            Variable.get("DB_PASSWORD"),
        )
    
        file_name = os.path.basename(file_path)
        start_ts = datetime.now()
    
        log.info("==== START %s ====", file_name)
    
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
        log.info("[%s] rows: %s", file_name, f"{total:,}")
    
        insert_sql = f"""
        INSERT INTO stg.transactions_raw ({",".join("["+c+"]" for c in TARGET_COLS)})
        VALUES ({",".join("?" for _ in TARGET_COLS)})
        """
    
        cn = pyodbc.connect(conn_str)
        try:
            cur = cn.cursor()
            cur.fast_executemany = True
    
            t0 = datetime.now()
            next_heartbeat = t0 + timedelta(seconds=60)
            done = 0
    
            for start in range(0, total, BATCH_SIZE):
                end = min(start + BATCH_SIZE, total)
    
                now = datetime.now()
                if now >= next_heartbeat:
                    elapsed = (now - t0).total_seconds()
                    rps = int(done / elapsed) if elapsed > 0 else 0
                    log.info(
                        "[%s] HEARTBEAT %s/%s | elapsed %ss | rate %s rows/s",
                        file_name,
                        f"{done:,}",
                        f"{total:,}",
                        int(elapsed),
                        f"{rps:,}",
                    )
                    next_heartbeat = now + timedelta(seconds=60)
    
                log.info(
                    "[%s] batch start rows %s-%s (size=%s)",
                    file_name,
                    start,
                    end,
                    f"{end - start:,}",
                )
    
                chunk = df.iloc[start:end]
    
                rows = [
                    tuple(py_value(v) for v in r)
                    for r in chunk.itertuples(index=False, name=None)
                ]
    
                log.info("[%s] executemany start (rows=%s)", file_name, f"{len(rows):,}")
                cur.executemany(insert_sql, rows)
    
                log.info("[%s] commit start", file_name)
                cn.commit()
    
                done = end
    
                # log po batchu
                now = datetime.now()
                elapsed = (now - t0).total_seconds()
                rps = int(done / elapsed) if elapsed > 0 else 0
                log.info(
                    "[%s] batch done %s/%s | elapsed %ss | rate %s rows/s",
                    file_name,
                    f"{done:,}",
                    f"{total:,}",
                    int(elapsed),
                    f"{rps:,}",
                )
    
            total_elapsed = int((datetime.now() - start_ts).total_seconds())
            log.info("==== DONE %s | %ss ====", file_name, total_elapsed)
    
            return {"file": file_name, "rows": total, "seconds": total_elapsed}
        finally:
            cn.close()


    @task
    def summarize(results: list[dict]):
        log.info("===== SUMMARY =====")
        log.info("Files : %d", len(results))
        log.info("Rows  : %s", f"{sum(r['rows'] for r in results):,}")
        log.info("Time  : %ss", sum(r["seconds"] for r in results))

    files = list_csv_files()
    trunc = truncate_staging()
    loads = load_one_file.expand(file_path=files)

    trunc >> loads
    summarize(loads)


load_transactions_raw()
