"""Load: data/clean/ → data/serving/observatorio.sqlite (+ opcional Parquet)."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from .paths import CLEAN, SERVING, ensure_dirs

DB_PATH = SERVING / "observatorio.sqlite"
TABLES = ("countries", "languages", "indicators", "join_orphans")


def run_load(write_parquet: bool = False) -> Path:
    ensure_dirs()
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    try:
        for name in TABLES:
            if name == "join_orphans":
                p = CLEAN / "join_orphans.csv"
                if not p.exists():
                    print(f"[load] omitido: {name}")
                    continue
                df = pd.read_csv(p)
            elif name == "indicators":
                df = pd.read_parquet(CLEAN / "indicators")
                df["indicator_id"] = df["indicator_id"].astype(str)  # viene como categoría
            else:
                df = pd.read_parquet(CLEAN / f"{name}.parquet")
            df.to_sql(name, conn, index=False, if_exists="replace")
            print(f"[load] {name}: {len(df)} filas → sqlite")
    finally:
        conn.close()

    print(f"[load] base → {DB_PATH}")
    return DB_PATH


if __name__ == "__main__":
    run_load()
