#!/usr/bin/env python3
"""05 · MapReduce a mano (sin Spark): cobertura por (indicador, década).

Misma lógica que la figura del enunciado y que spark/job_01.py, pero
en Python puro para ver Map → Shuffle → Reduce.

    python 01_csv_a_parquet.py
    python 05_mapreduce_puro.py
"""
from __future__ import annotations

from collections import defaultdict

import pandas as pd

from _paths import PARQUET_INDICATORS


def decade_of(year: int) -> str:
    return f"{(year // 10) * 10}s"


def main() -> None:
    if not PARQUET_INDICATORS.exists():
        raise SystemExit("Falta out/indicators.parquet → ejecutad antes 01_csv_a_parquet.py")

    rows = pd.read_parquet(PARQUET_INDICATORS).to_dict(orient="records")

    # --- MAP: cada fila, sola, emite (clave, 1) si value no es nulo ---
    mapped: list[tuple[tuple[str, str], int]] = []
    for row in rows:
        if pd.isna(row["value"]):
            continue  # nulo: no cuenta en cobertura
        key = (row["indicator_id"], decade_of(int(row["year"])))
        mapped.append((key, 1))
        print(f"MAP  {row['iso3']}, {row['year']} → {key} → 1")

    # --- SHUFFLE: juntar valores con la misma clave ---
    buckets: dict[tuple[str, str], list[int]] = defaultdict(list)
    for key, val in mapped:
        buckets[key].append(val)
    print("\nSHUFFLE (montones):")
    for key, vals in sorted(buckets.items()):
        print(f"  {key}: {vals}")

    # --- REDUCE: sumar cada montón ---
    print("\nREDUCE (cobertura):")
    for key, vals in sorted(buckets.items()):
        total = sum(vals)
        print(f"  {key} → {total}")


if __name__ == "__main__":
    main()
