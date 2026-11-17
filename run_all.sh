#!/usr/bin/env bash
# Ejecuta los prototipos en orden. Desde practica02_prototipos/:
#   ./run_all.sh
set -euo pipefail
cd "$(dirname "$0")"

if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
elif command -v python3 >/dev/null; then
  PY=python3
else
  echo "No hay python3"; exit 1
fi

echo "Usando: $PY"
"$PY" -c "import pandas, pyarrow, pyspark; print('deps OK')"

for script in \
  01_csv_a_parquet.py \
  02_leer_parquet.py \
  03_particionado_hive.py \
  04_consulta_pyarrow.py \
  05_mapreduce_puro.py \
  06_spark_session.py \
  07_spark_mapreduce_rdd.py \
  08_spark_dataframe_sql.py \
  09_csv_vs_parquet_tamano.py
do
  echo
  echo "======== $script ========"
  "$PY" "$script"
done

echo
echo "Todos los prototipos terminaron OK."
