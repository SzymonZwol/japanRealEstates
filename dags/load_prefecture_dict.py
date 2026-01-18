from airflow.decorators import dag, task
from pendulum import datetime
import pandas as pd
import pyodbc
import os
from airflow.models import Variable

CSV_PATH = "/usr/local/airflow/include/data/prefecture_code.csv"

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,   # manual
    catchup=False,
    tags=["etl", "sqlserver", "japan"],
)
def load_prefecture_dict():

    @task
    def load_csv_to_sql():
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

        cn = pyodbc.connect(conn_str)
        cur = cn.cursor()
        cur.fast_executemany = True

        # 1) truncate staging
        cur.execute("TRUNCATE TABLE stg.prefecture_dict;")
        cn.commit()

        # 2) load csv
        df = pd.read_csv(CSV_PATH)

        col_code = next(c for c in df.columns if c.lower() in ["code", "prefecture_code"])
        col_en   = next(c for c in df.columns if c.lower() in ["enname", "prefecture_name_en", "en_name"])

        df = df[[col_code, col_en]].copy()
        df.columns = ["prefecture_code", "prefecture_name_en"]

        df["prefecture_code"] = pd.to_numeric(df["prefecture_code"], errors="coerce")
        df["prefecture_name_en"] = df["prefecture_name_en"].astype(str).str.strip()
        df = df.dropna(subset=["prefecture_code", "prefecture_name_en"])

        rows = [(int(code), name) for code, name in df.to_numpy()]

        cur.executemany(
            """
            INSERT INTO stg.prefecture_dict (prefecture_code, prefecture_name_en)
            VALUES (?, ?)
            """,
            rows
        )
        cn.commit()

        cur.execute("SELECT COUNT(*) FROM stg.prefecture_dict;")
        print("Rows loaded:", cur.fetchone()[0])

        cur.close()
        cn.close()

    load_csv_to_sql()

load_prefecture_dict()
