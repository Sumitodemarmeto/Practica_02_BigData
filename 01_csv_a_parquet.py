#!/usr/bin/env python3
"""01 · CSV → Parquet tipado (lo que hará transform.py en la práctica).

Lee los CSV de datos/, fuerza tipos (year entero, value float con nulos)
y escribe Parquet en out/. Ejecutad desde esta carpeta:

    python 01_csv_a_parquet.py
"""
from __future__ import annotations

import pandas as pd

from _paths import (
    CSV_COUNTRIES,
    CSV_INDICATORS,
    PARQUET_COUNTRIES,
    PARQUET_INDICATORS,
    ensure_out,
)


def main() -> None:
    ensure_out()

    countries = pd.read_csv(CSV_COUNTRIES)
    # Tipado explícito: booleano real, no texto "true"/"false" suelto
    countries["independent"] = countries["independent"].astype(bool)

    indicators = pd.read_csv(CSV_INDICATORS)
    indicators["year"] = indicators["year"].astype("int64")
    # value vacío en CSV → NaN (nulo real), no cero inventado
    indicators["value"] = pd.to_numeric(indicators["value"], errors="coerce")

    countries.to_parquet(PARQUET_COUNTRIES, index=False)
    indicators.to_parquet(PARQUET_INDICATORS, index=False)

    print("Escrito:", PARQUET_COUNTRIES)
    print("Escrito:", PARQUET_INDICATORS)
    print("Filas countries:", len(countries))
    print("Filas indicators:", len(indicators))
    print("Nulos en value:", int(indicators["value"].isna().sum()))
    print("dtypes indicators:\n", indicators.dtypes, sep="")


if __name__ == "__main__":
    main()
