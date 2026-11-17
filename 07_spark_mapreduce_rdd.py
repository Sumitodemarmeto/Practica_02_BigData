#!/usr/bin/env python3
"""07 · MapReduce en Spark RDD (map + reduceByKey) sobre Parquet Hive.

Equivalente al tramo RDD de spark/job_01.py de la práctica.

    python 03_particionado_hive.py
    python 07_spark_mapreduce_rdd.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession

from _paths import EVENT_LOG_DIR, HIVE_INDICATORS, OUT, ensure_out


def decade_of(year: int) -> str:
    return f"{(year // 10) * 10}s"


def main() -> None:
    ensure_out()
    if not HIVE_INDICATORS.exists():
        raise SystemExit("Falta out/indicators_hive/ → ejecutad antes 03_particionado_hive.py")

    EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("prototipo-07-rdd")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG_DIR.as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(str(HIVE_INDICATORS))
    # Solo filas con valor (cobertura)
    rdd = (
        df.filter(df.value.isNotNull())
        .select("indicator_id", "year")
        .rdd.map(lambda row: ((row.indicator_id, decade_of(int(row.year))), 1))
        .reduceByKey(lambda a, b: a + b)
    )

    result = sorted(rdd.collect(), key=lambda x: x[0])
    print("Cobertura RDD (indicator_id, década) → count:")
    for key, total in result:
        print(f"  {key} → {total}")

    out = OUT / "cobertura_rdd.csv"
    spark.createDataFrame(
        [(k[0], k[1], v) for k, v in result],
        ["indicator_id", "decade", "n"],
    ).toPandas().to_csv(out, index=False)
    print("Escrito:", out)

    spark.stop()


if __name__ == "__main__":
    main()
