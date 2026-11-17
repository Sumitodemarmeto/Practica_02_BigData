#!/usr/bin/env python3
"""03 · Particionado estilo Hive por indicator_id.

Crea out/indicators_hive/indicator_id=.../part-*.parquet
(como data/clean/indicators/ en la práctica).

    python 01_csv_a_parquet.py
    python 03_particionado_hive.py
"""
from __future__ import annotations

import shutil

import pandas as pd

from _paths import HIVE_INDICATORS, PARQUET_INDICATORS, ensure_out


def main() -> None:
    ensure_out()
    if not PARQUET_INDICATORS.exists():
        raise SystemExit("Falta out/indicators.parquet → ejecutad antes 01_csv_a_parquet.py")

    df = pd.read_parquet(PARQUET_INDICATORS)
    if HIVE_INDICATORS.exists():
        shutil.rmtree(HIVE_INDICATORS)

    # partition_cols: la columna sale del fichero y va al nombre de carpeta
    df.to_parquet(
        HIVE_INDICATORS,
        index=False,
        partition_cols=["indicator_id"],
    )

    print("Árbol Hive bajo", HIVE_INDICATORS)
    for path in sorted(HIVE_INDICATORS.rglob("*")):
        rel = path.relative_to(HIVE_INDICATORS)
        marca = "/" if path.is_dir() else f"  ({path.stat().st_size} bytes)"
        print(f"  {rel}{marca if path.is_dir() else marca}")

    print("\nValores distintos de indicator_id:", sorted(df["indicator_id"].unique()))
    print("→ tantas carpetas como indicadores (buena partición).")
    print("Si particionáramos por iso3 saldrían", df["iso3"].nunique(), "carpetas (mala idea aquí).")


if __name__ == "__main__":
    main()
