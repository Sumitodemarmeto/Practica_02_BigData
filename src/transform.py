"""Transform: data/raw/ → data/clean/ (tablas normalizadas).

Obligatorio en la Práctica 1 Big Data (ver enunciado):
  - clave canónica iso3 (+ mapeo iso2↔iso3 vía focus_countries)
  - perfil aplanado → countries
  - explosión de idiomas → languages (tabla larga)
  - indicadores en esquema común → indicators
  - tipado / nulos; join por iso3; join_orphans.csv

Para una URL nueva: implementad una función y registradla en TRANSFORMERS
con el mismo nombre que `transformer:` en sources.yaml.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import shutil

import pandas as pd

from .config import Config, Source, load_config
from .paths import CLEAN, RAW, ensure_dirs

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]


def _read_json(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.json"
    payload = _read_json(raw_path)
    wanted = {i["cca3"] for i in payload if i.get("independent")}

    countries_rows = []
    language_rows = []
    for item in payload:
        iso3 = item.get("cca3")
        if iso3 not in wanted:
            continue
        name = item.get("name") or {}
        countries_rows.append(
            {
                "iso3": iso3,
                "iso2": item.get("cca2"),
                "name_common": name.get("common"),
                "name_official": name.get("official"),
                "region": item.get("region"),
                "subregion": item.get("subregion"),
            }
        )
        for code, lang_name in (item.get("languages") or {}).items():
            language_rows.append(
                {
                    "iso3": iso3,
                    "language_code": code,
                    "language_name": lang_name,
                }
            )

    return {
        "countries": pd.DataFrame(countries_rows),
        "languages": pd.DataFrame(language_rows),
    }

def _iso2_by_iso3() -> dict[str, str]:
    payload = _read_json(RAW / "countries_profile.json")
    return {i["cca3"]: i["cca2"] for i in payload if i.get("independent")}


def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    iso2_map = _iso2_by_iso3()
    meta = source.meta or {}
    payload = _read_json(RAW / f"{source.id}.json")
    rows = []
    for rec in payload[1]:
        wb_code = rec.get("countryiso3code")
        iso3 = cfg.code_equivalences.get(wb_code, wb_code)
        if iso3 not in iso2_map:          # agregados y no-países
            continue
        rows.append(
            {
                "iso3": iso3,
                "iso2": iso2_map[iso3],
                "indicator_id": meta.get("indicator_id", source.id),
                "indicator_label": meta.get("indicator_label"),
                "year": int(rec["date"]),
                "value": rec.get("value"),   # None → vacío, nunca 0
                "unit": meta.get("unit"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows)}

def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.csv"
    df = pd.read_csv(raw_path)
    meta = source.meta or {}
    year = str(meta.get("year", "2023"))
    wanted = {c.iso3 for c in cfg.focus_countries}
    # Columnas OWID: Entity, Code, Year, Life expectancy
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]
    iso2_by_iso3 = {c.iso3: c.iso2 for c in cfg.focus_countries}
    sub = df[(df[code_col].isin(wanted)) & (df[year_col].astype(str) == year)]
    rows = []
    for _, r in sub.iterrows():
        iso3 = r[code_col]
        rows.append(
            {
                "iso3": iso3,
                "iso2": iso2_by_iso3.get(iso3),
                "indicator_id": meta.get("indicator_id", "life_expectancy"),
                "indicator_label": meta.get("indicator_label", value_col),
                "year": str(r[year_col]),
                "value": r[value_col],
                "unit": meta.get("unit", "years"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows)}


def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    rows = []
    meta = source.meta or {}
    for country in cfg.focus_countries:
        path = RAW / f"{source.id}_{country.iso2}.json"
        if not path.exists():
            continue
        payload = _read_json(path)
        values = payload.get("value") or {}
        if not values:
            # País fuera de cobertura Eurostat: huérfano documentado vía ausencia
            continue
        # Con una sola celda filtrada, la clave suele ser "0"
        value = next(iter(values.values()))
        rows.append(
            {
                "iso3": country.iso3,
                "iso2": country.iso2,
                "indicator_id": meta.get("indicator_id", "demo_pjan"),
                "indicator_label": meta.get(
                    "indicator_label", payload.get("label")
                ),
                "year": meta.get("time"),
                "value": value,
                "unit": meta.get("unit", "persons"),
                "source_id": source.id,
            }
        )
    return {"indicators": pd.DataFrame(rows)}


def transform_generic_passthrough(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    """Plantilla: lee raw y deja un DataFrame mínimo para que lo adapteis."""
    paths = sorted(RAW.glob(f"{source.id}*"))
    rows = []
    for path in paths:
        rows.append(
            {
                "source_id": source.id,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "note": "Reemplazad este transformer por uno específico",
            }
        )
    return {"custom_raw_index": pd.DataFrame(rows)}


TRANSFORMERS: dict[str, Transformer] = {
    "countries_profile": transform_countries_profile,
    "worldbank_indicator": transform_worldbank_indicator,
    "eurostat_demo_pjan": transform_eurostat_demo_pjan,
    "owid_life_expectancy": transform_owid_life_expectancy,
    "generic_passthrough": transform_generic_passthrough,
}

def _write_clean(table: str, df: pd.DataFrame) -> None:
    if table == "indicators":
        df["year"] = df["year"].astype("int64")
        df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("float64")
        key = ["iso3", "indicator_id", "year", "source_id"]
        if df.duplicated(subset=key).any():
            raise ValueError(f"Clave duplicada en indicators: {key}")
        dest = CLEAN / "indicators"
        if dest.exists():
            shutil.rmtree(dest)
        df.to_parquet(dest, index=False, partition_cols=["indicator_id"])
    else:
        dest = CLEAN / f"{table}.parquet"
        df.to_parquet(dest, index=False)
    print(f"[transform] {table}: {len(df)} filas → {dest}")

def build_excluded(cfg: Config) -> pd.DataFrame:
    profile = _read_json(RAW / "countries_profile.json")
    aceptados = {i["cca3"] for i in profile if i.get("independent")}
    en_perfil = {i["cca3"] for i in profile}
    rows: dict[str, dict] = {}

    for i in profile:
        if not i.get("independent"):
            rows[i["cca3"]] = {
                "iso3": i["cca3"], "name": (i.get("name") or {}).get("common"),
                "motivo": "no independiente (mledoze)", "fuente": "countries_profile",
            }

    for s in cfg.sources:
        if s.transformer != "worldbank_indicator":
            continue
        for rec in _read_json(RAW / f"{s.id}.json")[1]:
            code = rec.get("countryiso3code") or ""
            iso3 = cfg.code_equivalences.get(code, code)
            key = iso3 or rec["country"]["value"]
            if iso3 in aceptados or iso3 in en_perfil or key in rows:
                continue
            rows[key] = {
                "iso3": iso3, "name": rec["country"]["value"],
                "motivo": "agregado WB o entidad sin perfil", "fuente": s.id,
            }
    return pd.DataFrame(rows.values())

def run_transform(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    ensure_dirs()
    buckets: dict[str, list[pd.DataFrame]] = {}

    for source in cfg.sources:
        name = source.transformer
        if name not in TRANSFORMERS:
            raise KeyError(
                f"Transformer '{name}' no registrado. "
                f"Disponibles: {sorted(TRANSFORMERS)}"
            )
        print(f"[transform] {source.id} → {name}")
        out = TRANSFORMERS[name](source, cfg)
        for table, df in out.items():
            buckets.setdefault(table, []).append(df)

    merged: dict[str, pd.DataFrame] = {}
    for table, parts in buckets.items():
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        _write_clean(table, df)
        merged[table] = df

    if "countries" in merged and "indicators" in merged:
        ind = merged["indicators"]
        ok = ind[ind["value"].notna()]
        con_dato = set(zip(ok["iso3"], ok["source_id"]))
        rows = []
        for iso3 in merged["countries"]["iso3"]:
            for s in cfg.sources:
                if s.transformer == "worldbank_indicator" and (iso3, s.id) not in con_dato:
                    rows.append({"iso3": iso3, "source_id": s.id,
                                 "indicator_id": (s.meta or {}).get("indicator_id"),
                                 "missing_in": "indicators"})
        report = pd.DataFrame(rows, columns=["iso3", "source_id", "indicator_id", "missing_in"])
        report.to_csv(CLEAN / "join_orphans.csv", index=False)
        print(f"[transform] huérfanos por fuente: {len(report)}")

        excl = build_excluded(cfg)
        excl.to_csv(CLEAN / "excluded_entities.csv", index=False)
        print(f"[transform] entidades excluidas: {len(excl)}")


if __name__ == "__main__":
    run_transform()
