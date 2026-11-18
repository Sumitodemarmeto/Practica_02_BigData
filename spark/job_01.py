"""Cobertura (indicator_id, década) con RDD, DataFrame y SQL; comparada con SQLite."""
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
    return f"{(year // 10) * 10}s"


def main() -> int:
    LOGS.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.appName("job01").master("local[*]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", LOGS.as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(str(HIVE)).filter(F.col("value").isNotNull())
    print("Particiones al leer:", df.rdd.getNumPartitions())

    # 1. RDD
    rdd = (
        df.select("indicator_id", "year").rdd
        .map(lambda r: ((r.indicator_id, decade_of(int(r.year))), 1))
        .reduceByKey(lambda a, b: a + b)
    )
    res_rdd = {(k[0], k[1], v) for k, v in rdd.collect()}

    # 2. DataFrame
    d = df.withColumn("decade", F.concat(F.floor(F.col("year") / 10) * 10, F.lit("s")))
    by_df = d.groupBy("indicator_id", "decade").agg(F.count("*").alias("n")).orderBy("indicator_id", "decade")
    res_df = {(r.indicator_id, r.decade, r.n) for r in by_df.collect()}

    # 3. SQL
    d.createOrReplaceTempView("indicators")
    by_sql = spark.sql("""
        SELECT indicator_id, decade, COUNT(*) AS n
        FROM indicators GROUP BY indicator_id, decade
    """)
    res_sql = {(r.indicator_id, r.decade, r.n) for r in by_sql.collect()}

    # 4. SQLite
    with sqlite3.connect(SQLITE) as conn:
        rows = conn.execute("""
            SELECT indicator_id, CAST((year/10)*10 AS TEXT) || 's', COUNT(*)
            FROM indicators WHERE value IS NOT NULL
            GROUP BY indicator_id, year/10
        """).fetchall()
    res_sqlite = set(rows)

    print("Plan DataFrame:")
    by_df.explain(mode="formatted")

    if OUT.exists():
        shutil.rmtree(OUT)
    by_df.write.mode("overwrite").parquet(str(OUT))

    ok = res_rdd == res_df == res_sql == res_sqlite
    print(f"RDD={len(res_rdd)} DF={len(res_df)} SQL={len(res_sql)} SQLite={len(res_sqlite)} → coinciden: {ok}")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
