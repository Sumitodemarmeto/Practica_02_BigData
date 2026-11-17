#!/usr/bin/env python3
"""06 · Arrancar Spark en local[*] (mismo patrón que el enunciado).

Requiere Java 17/21/25. Ejecutad:

    python 06_spark_session.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from _paths import EVENT_LOG_DIR, ensure_out


def main() -> None:
    ensure_out()
    EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("prototipo-06-session")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG_DIR.as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("Spark version:", spark.version)
    print("Master:", spark.sparkContext.master)
    print("Default parallelism:", spark.sparkContext.defaultParallelism)
    print("Event log:", EVENT_LOG_DIR)

    # Humo mínimo: crear un DataFrame y contar
    df = spark.range(0, 10)
    print("spark.range(10).count() =", df.count())

    spark.stop()
    print("OK — sesión cerrada.")


if __name__ == "__main__":
    main()
