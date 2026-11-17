#!/usr/bin/env python3
"""08 · Misma cobertura con DataFrame y Spark SQL (deben coincidir).

    python 03_particionado_hive.py
    python 07_spark_mapreduce_rdd.py   # opcional, para comparar CSV
    python 08_spark_dataframe_sql.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession, functions as F

from _paths import EVENT_LOG_DIR, HIVE_INDICATORS, OUT, ensure_out


def main() -> None:
    ensure_out()
    if not HIVE_INDICATORS.exists():
        raise SystemExit("Falta out/indicators_hive/ → ejecutad antes 03_particionado_hive.py")

    EVENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("prototipo-08-df-sql")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", EVENT_LOG_DIR.as_uri())
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(str(HIVE_INDICATORS)).filter(F.col("value").isNotNull())
    df = df.withColumn("decade", F.concat(F.floor(F.col("year") / 10) * 10, F.lit("s")))

    # --- DataFrame ---
    by_df = (
        df.groupBy("indicator_id", "decade")
        .agg(F.count("*").alias("n"))
        .orderBy("indicator_id", "decade")
    )
    print("=== DataFrame groupBy ===")
    by_df.show(truncate=False)

    # --- Spark SQL ---
    df.createOrReplaceTempView("indicators")
    by_sql = spark.sql(
        """
        SELECT indicator_id,
               CONCAT(CAST(FLOOR(year / 10) * 10 AS STRING), 's') AS decade,
               COUNT(*) AS n
        FROM indicators
        GROUP BY indicator_id, FLOOR(year / 10)
        ORDER BY indicator_id, decade
        """
    )
    print("=== Spark SQL ===")
    by_sql.show(truncate=False)

    pdf_df = {tuple(r) for r in by_df.collect()}
    pdf_sql = {tuple(r) for r in by_sql.collect()}
    print("¿DataFrame == SQL?", pdf_df == pdf_sql)

    dest = OUT / "cobertura_decada"
    if dest.exists():
        import shutil

        shutil.rmtree(dest)
    by_df.write.mode("overwrite").parquet(str(dest))
    print("Escrito (estilo serving/spark/cobertura_decada/):", dest)
    print("Plan físico (fragmento):")
    by_df.explain(mode="simple")

    spark.stop()


if __name__ == "__main__":
    main()
