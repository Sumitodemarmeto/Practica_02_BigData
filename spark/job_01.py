"""Cobertura (indicator_id, década) con RDD, DataFrame y SQL; validada contra SQLite."""
import shutil
import sqlite3
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
HIVE = ROOT / "data" / "clean" / "indicators"
SQLITE = ROOT / "data" / "serving" / "observatorio.sqlite"
OUT = ROOT / "data" / "serving" / "spark" / "cobertura_decada"
LOGS = ROOT / "logs" / "spark-events"


def decade_of(year: int) -> str:
    return f"{(int(year) // 10) * 10}s"


def build_spark() -> SparkSession:
    LOGS.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName("practica02_job01")
        .master("local[*]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", LOGS.as_uri())
        .config("spark.eventLog.rolling.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> int:
    if not HIVE.exists():
        print(f"[job_01] No existe {HIVE}. Ejecuta antes: python -m src.pipeline")
        return 1
    if not SQLITE.exists():
        print(f"[job_01] No existe {SQLITE}. Ejecuta antes: python -m src.pipeline")
        return 1

    spark = build_spark()
    try:
        df = (
            spark.read.parquet(str(HIVE))
            .filter(F.col("value").isNotNull() & F.col("year").isNotNull())
        )
        print(f"[job_01] Particiones: {df.rdd.getNumPartitions()}  Filas: {df.count()}")


        rdd_pairs = df.select("indicator_id", "year", "iso3").rdd.map(
            lambda r: ((r.indicator_id, decade_of(int(r.year))), (1, {r.iso3}))
        )
        rdd_reduced = rdd_pairs.reduceByKey(lambda a, b: (a[0] + b[0], a[1] | b[1]))
        res_rdd = {(k[0], k[1], v[0], len(v[1])) for k, v in rdd_reduced.collect()}


        d = df.withColumn(
            "decade",
            F.concat(F.floor(F.col("year") / 10).cast("int") * 10, F.lit("s")),
        )
        by_df = (
            d.groupBy("indicator_id", "decade")
            .agg(
                F.count("*").alias("n"),
                F.countDistinct("iso3").alias("paises")
            )
            .orderBy("indicator_id", "decade")
        )
        res_df = {(r.indicator_id, r.decade, r.n, r.paises) for r in by_df.collect()}


        d.createOrReplaceTempView("indicators")
        by_sql = spark.sql(
            """
            SELECT indicator_id, decade, COUNT(*) AS n, COUNT(DISTINCT iso3) AS paises
            FROM indicators GROUP BY indicator_id, decade
            """
        )
        res_sql = {(r.indicator_id, r.decade, r.n, r.paises) for r in by_sql.collect()}


        with sqlite3.connect(SQLITE) as conn:
            rows = conn.execute(
                """
                SELECT indicator_id, CAST((year/10)*10 AS TEXT) || 's',
                       COUNT(*), COUNT(DISTINCT iso3)
                FROM indicators WHERE value IS NOT NULL
                GROUP BY indicator_id, year/10
                """
            ).fetchall()
        res_sqlite = {(r[0], r[1], r[2], r[3]) for r in rows}

        print("[job_01] Plan físico DataFrame:")
        by_df.explain(mode="formatted")

        if OUT.exists():
            shutil.rmtree(OUT)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        by_df.write.mode("overwrite").parquet(str(OUT))
        print(f"[job_01] Escrito: {OUT}")

        ok = res_rdd == res_df == res_sql == res_sqlite
        print(f"[job_01] RDD={len(res_rdd)} DF={len(res_df)} "
              f"SQL={len(res_sql)} SQLite={len(res_sqlite)} → coinciden: {ok}")
        return 0 if ok else 1
    finally:
        spark.stop()
        print("[job_01] SparkSession cerrada.")


if __name__ == "__main__":
    sys.exit(main())
