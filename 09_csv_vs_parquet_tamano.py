#!/usr/bin/env python3
"""09 · Misma tabla en CSV y en Parquet: comparar bytes (pregunta de teoría).

Amplía el mini-CSV varias veces para que la diferencia se note, escribe
ambos formatos y muestra tamaños. No sustituye vuestra cifra del universo
real: es solo para ver el efecto columnar.

    python 09_csv_vs_parquet_tamano.py
"""
from __future__ import annotations

import pandas as pd

from _paths import CSV_INDICATORS, OUT, ensure_out


def dir_size(path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def main() -> None:
    ensure_out()
    base = pd.read_csv(CSV_INDICATORS)
    base["year"] = base["year"].astype("int64")
    base["value"] = pd.to_numeric(base["value"], errors="coerce")

    # Repetir el bloque para hinchar un poco (sigue siendo toy)
    chunks = []
    for i in range(200):
        chunk = base.copy()
        chunk["year"] = chunk["year"] + (i % 3)  # variación mínima
        chunks.append(chunk)
    big = pd.concat(chunks, ignore_index=True)

    csv_path = OUT / "indicators_big.csv"
    parquet_path = OUT / "indicators_big.parquet"
    big.to_csv(csv_path, index=False)
    big.to_parquet(parquet_path, index=False)

    n_csv = dir_size(csv_path)
    n_parq = dir_size(parquet_path)
    ratio = n_csv / n_parq if n_parq else float("inf")

    print(f"Filas: {len(big)}")
    print(f"CSV:     {n_csv:>10} bytes  → {csv_path.name}")
    print(f"Parquet: {n_parq:>10} bytes  → {parquet_path.name}")
    print(f"CSV / Parquet ≈ {ratio:.2f}× (en este toy; en el universo medid lo vuestro)")


if __name__ == "__main__":
    main()
