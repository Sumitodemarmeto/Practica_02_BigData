"""Rutas comunes de los prototipos (carpeta de este kit)."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATOS = ROOT / "datos"
OUT = ROOT / "out"

CSV_INDICATORS = DATOS / "indicators_mini.csv"
CSV_COUNTRIES = DATOS / "countries_mini.csv"

PARQUET_COUNTRIES = OUT / "countries.parquet"
PARQUET_INDICATORS = OUT / "indicators.parquet"
HIVE_INDICATORS = OUT / "indicators_hive"
EVENT_LOG_DIR = ROOT / ".spark_event_logs"


def ensure_out() -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    return OUT
