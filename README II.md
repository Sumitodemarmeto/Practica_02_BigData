# Prototipos Práctica 2 (Parquet + MapReduce + Spark)

Scripts mínimos para ensayar Parquet, Hive, MapReduce y Spark en el
portátil antes de aplicarlos al repositorio del equipo. No forman parte
del entregable ni sustituyen a `spark/job_01.py`. Datos de ejemplo en
`datos/`. Referenciados desde `practica_02_universo_spark.pdf`.

## Arranque

```bash
# Tras descomprimir practica02_prototipos.zip:
cd practica02_prototipos
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Java 17, 21 o 25 en PATH (necesario para 06–08)
./run_all.sh
```

O uno a uno: `python 01_csv_a_parquet.py`, luego `02_…`, etc.

Los artefactos salen en `out/` (ignorado por git).

## Orden de los scripts

| Script | Contenido | Encaje en la práctica |
| --- | --- | --- |
| `01_csv_a_parquet.py` | CSV → Parquet tipado (`year` int, `value` float/nulo) | `src/transform.py` |
| `02_leer_parquet.py` | Leer Parquet + cargar SQLite | `src/load.py` |
| `03_particionado_hive.py` | Carpetas `indicator_id=…/` | `data/clean/indicators/` |
| `04_consulta_pyarrow.py` | Dataset Hive + filtro (poda) | comprobar clean |
| `05_mapreduce_puro.py` | Map → Shuffle → Reduce a mano | figura MapReduce del PDF |
| `06_spark_session.py` | `SparkSession` `local[*]` + event log | arranque de sesión |
| `07_spark_mapreduce_rdd.py` | RDD `map` + `reduceByKey` | tramo RDD de `job_01.py` |
| `08_spark_dataframe_sql.py` | DataFrame + SQL + Parquet de salida | tramos DF/SQL; `cobertura_decada/` |
| `09_csv_vs_parquet_tamano.py` | Comparar bytes CSV vs Parquet | pregunta de teoría |

Datos de entrada: `datos/indicators_mini.csv`, `datos/countries_mini.csv`
(pocos países, varios años/décadas, un nulo a propósito).

## Requisitos

- Python ≥ 3.10
- Dependencias: `requirements.txt` (`pandas`, `pyarrow`, `pyspark`)
- Java 17 / 21 / 25 para los scripts Spark (06–08). Comprobar: `java -version`
  - Linux (Debian/Ubuntu): `sudo apt install -y openjdk-21-jdk`
  - Windows: Temurin 21 (.msi) desde https://adoptium.net/ (marcar PATH / JAVA_HOME)

## Aviso

Estos números (tamaños, conteos) son del mini-dataset. En la entrega usad
**vuestros** datos del universo y el job del repo del equipo.
