#!/usr/bin/env python3
"""04 · Consultar el dataset Hive con pyarrow (poda de particiones).

    python 03_particionado_hive.py
    python 04_consulta_pyarrow.py
"""
from __future__ import annotations

import pyarrow.dataset as ds

from _paths import HIVE_INDICATORS


def main() -> None:
    if not HIVE_INDICATORS.exists():
        raise SystemExit("Falta out/indicators_hive/ → ejecutad antes 03_particionado_hive.py")

    dataset = ds.dataset(
        str(HIVE_INDICATORS),
        format="parquet",
        partitioning="hive",
    )
    print("Esquema:", dataset.schema)
    print("Filas totales:", dataset.count_rows())

    # Filtro por columna de partición → solo abre esa carpeta
    solo_pib = dataset.to_table(
        filter=(ds.field("indicator_id") == "NY.GDP.PCAP.CD")
    )
    print("Filas solo PIB (NY.GDP.PCAP.CD):", solo_pib.num_rows)
    print(solo_pib.to_pandas()[["iso3", "year", "value"]].to_string(index=False))

    # Filtro que NO es partición: mira dentro de los ficheros
    esp = dataset.to_table(filter=(ds.field("iso3") == "ESP"))
    print("\nFilas iso3=ESP (sin poda por carpeta):", esp.num_rows)


if __name__ == "__main__":
    main()
