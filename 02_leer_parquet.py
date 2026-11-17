#!/usr/bin/env python3
"""02 · Leer Parquet y comprobar el esquema (idea de load.py).

    python 01_csv_a_parquet.py   # si aún no habéis generado out/
    python 02_leer_parquet.py
"""
from __future__ import annotations

import sqlite3

import pandas as pd

from _paths import PARQUET_COUNTRIES, PARQUET_INDICATORS, OUT, ensure_out


def main() -> None:
    ensure_out()
    if not PARQUET_INDICATORS.exists():
        raise SystemExit("Falta out/indicators.parquet → ejecutad antes 01_csv_a_parquet.py")

    ind = pd.read_parquet(PARQUET_INDICATORS)
    countries = pd.read_parquet(PARQUET_COUNTRIES)

    print("=== Esquema indicators ===")
    print(ind.dtypes)
    print("\nPrimeras filas:")
    print(ind.head(3).to_string(index=False))

    # Igual que load.py: Parquet → SQLite de serving
    db = OUT / "demo_serving.sqlite"
    with sqlite3.connect(db) as conn:
        countries.to_sql("countries", conn, index=False, if_exists="replace")
        ind.to_sql("indicators", conn, index=False, if_exists="replace")
        n = conn.execute("SELECT COUNT(*) FROM indicators WHERE value IS NOT NULL").fetchone()[0]
    print(f"\nSQLite {db.name}: filas indicators con valor = {n}")


if __name__ == "__main__":
    main()
