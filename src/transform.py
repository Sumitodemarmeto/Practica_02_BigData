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
import shutil
from pathlib import Path
from typing import Callable

import pandas as pd

from .config import Config, Source, load_config
from .paths import CLEAN, RAW, ensure_dirs

Transformer = Callable[[Source, Config], dict[str, pd.DataFrame]]


def _read_json(path: Path):
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _read_profile() -> list[dict]:
    p = RAW / "countries_profile.json"
    if not p.exists():
        raise FileNotFoundError(f"No existe {p}. Ejecutad extract antes que transform.")
    return _read_json(p)


def _accepted_iso3(cfg: Config) -> set[str]:
    return {i["cca3"] for i in _read_profile() if cfg.country_criterion.accept_entity(i)}


def _iso2_by_iso3(cfg: Config) -> dict[str, str]:
    return {i["cca3"]: i["cca2"] for i in _read_profile()
            if cfg.country_criterion.accept_entity(i)}


def _to_int_year(raw) -> int | None:
    if raw is None:
        return None
    try:
        return int(float(raw))
    except (ValueError, TypeError):
        return None


def transform_countries_profile(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    payload = _read_profile()
    countries_rows, language_rows = [], []
    for item in payload:
        if not cfg.country_criterion.accept_entity(item):
            continue
        iso3 = item["cca3"]
        name = item.get("name") or {}
        countries_rows.append({
            "iso3": iso3,
            "iso2": item.get("cca2"),
            "name_common": name.get("common"),
            "name_official": name.get("official"),
            "region": item.get("region"),
            "subregion": item.get("subregion"),
        })
        for code, lang_name in (item.get("languages") or {}).items():
            language_rows.append({
                "iso3": iso3, "language_code": code, "language_name": lang_name,
            })
    return {
        "countries": pd.DataFrame(countries_rows),
        "languages": pd.DataFrame(language_rows),
    }


def transform_worldbank_indicator(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    iso2_map = _iso2_by_iso3(cfg)
    meta = source.meta or {}
    payload = _read_json(RAW / f"{source.id}.json")
    rows = []
    for rec in payload[1] or []:
        wb_code = rec.get("countryiso3code")
        iso3 = cfg.code_equivalences.get(wb_code, wb_code)
        if iso3 not in iso2_map:
            continue
        rows.append({
            "iso3": iso3,
            "iso2": iso2_map.get(iso3),
            "indicator_id": meta.get("indicator_id", source.id),
            "indicator_label": meta.get("indicator_label"),
            "year": _to_int_year(rec.get("date")),
            "value": rec.get("value"),
            "unit": meta.get("unit"),
            "source_id": source.id,
        })
    return {"indicators": pd.DataFrame(rows)}


def transform_owid_life_expectancy(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    raw_path = RAW / f"{source.id}.csv"
    if not raw_path.exists():
        print(f"[transform] {source.id}: no existe {raw_path.name}, se omite")
        return {"indicators": pd.DataFrame()}
    df = pd.read_csv(raw_path)
    meta = source.meta or {}
    aceptados = _accepted_iso3(cfg)
    iso2_by = _iso2_by_iso3(cfg)
    code_col = "Code" if "Code" in df.columns else "code"
    year_col = "Year" if "Year" in df.columns else "year"
    value_col = "Life expectancy" if "Life expectancy" in df.columns else df.columns[-1]

    sub = df[df[code_col].isin(aceptados)].copy()
    if meta.get("year") is not None:
        sub = sub[sub[year_col].astype(str) == str(meta["year"])]

    rows = [{
        "iso3": r[code_col],
        "iso2": iso2_by.get(r[code_col]),
        "indicator_id": meta.get("indicator_id", "life_expectancy"),
        "indicator_label": meta.get("indicator_label", value_col),
        "year": _to_int_year(r[year_col]),
        "value": r[value_col],
        "unit": meta.get("unit", "years"),
        "source_id": source.id,
    } for _, r in sub.iterrows()]
    return {"indicators": pd.DataFrame(rows)}


def transform_eurostat_demo_pjan(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    meta = source.meta or {}
    iso2_by = _iso2_by_iso3(cfg)
    rows = []
    for path in sorted(RAW.glob(f"{source.id}_*.json")):
        iso2 = path.stem.split("_")[-1]
        iso3 = next((k for k, v in iso2_by.items() if v == iso2), None)
        if not iso3:
            continue
        payload = _read_json(path)
        values = (payload or {}).get("value") or {}
        if not values:
            continue
        value = next(iter(values.values()))
        year_raw = meta.get("time") or meta.get("year")
        if isinstance(year_raw, list):
            year_raw = year_raw[0] if year_raw else None
        rows.append({
            "iso3": iso3,
            "iso2": iso2,
            "indicator_id": meta.get("indicator_id", "demo_pjan"),
            "indicator_label": meta.get("indicator_label", payload.get("label")),
            "year": _to_int_year(year_raw),
            "value": value,
            "unit": meta.get("unit", "persons"),
            "source_id": source.id,
        })
    return {"indicators": pd.DataFrame(rows)}


def transform_generic_passthrough(source: Source, cfg: Config) -> dict[str, pd.DataFrame]:
    paths = sorted(RAW.glob(f"{source.id}*"))
    rows = [{
        "source_id": source.id,
        "filename": p.name,
        "bytes": p.stat().st_size,
        "note": "Reemplazad este transformer por uno específico",
    } for p in paths]
    return {"custom_raw_index": pd.DataFrame(rows)}


TRANSFORMERS: dict[str, Transformer] = {
    "countries_profile": transform_countries_profile,
    "worldbank_indicator": transform_worldbank_indicator,
    "eurostat_demo_pjan": transform_eurostat_demo_pjan,
    "owid_life_expectancy": transform_owid_life_expectancy,
    "generic_passthrough": transform_generic_passthrough,
}


def _write_clean(table: str, df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or df.shape[1] == 0:
        print(f"[transform] {table}: sin filas, no se escribe")
        return pd.DataFrame()

    if table == "indicators":
        years = pd.to_numeric(df["year"], errors="coerce")
        n_before = len(df)
        df = df[years.notna()].copy()
        df["year"] = years[years.notna()].astype("int64")
        if len(df) < n_before:
            print(f"[transform] indicators: {n_before - len(df)} filas sin year válido descartadas")
        df["value"] = pd.to_numeric(df["value"], errors="coerce").astype("float64")
        key = ["iso3", "indicator_id", "year", "source_id"]
        dup = df.duplicated(subset=key)
        if dup.any():
            raise ValueError(f"Clave duplicada en indicators ({int(dup.sum())} filas).\n"
                             f"{df[dup].head(5).to_string(index=False)}")
        dest = CLEAN / "indicators"
        if dest.exists():
            shutil.rmtree(dest)
        df.to_parquet(dest, index=False, partition_cols=["indicator_id"])
    else:
        if table == "countries" and df.duplicated("iso3").any():
            raise ValueError("countries: iso3 duplicado")
        dest = CLEAN / f"{table}.parquet"
        df.to_parquet(dest, index=False)

    print(f"[transform] {table}: {len(df)} filas → {dest}")
    return df


def build_excluded(cfg: Config) -> pd.DataFrame:
    profile = _read_profile()
    aceptados = {i["cca3"] for i in profile if cfg.country_criterion.accept_entity(i)}
    en_perfil = {i["cca3"] for i in profile}
    rows: dict[str, dict] = {}

    for i in profile:
        if cfg.country_criterion.accept_entity(i):
            continue
        rows[i["cca3"]] = {
            "iso3": i["cca3"],
            "name": (i.get("name") or {}).get("common"),
            "motivo": "rechazado por country_criterion (perfil)",
            "fuente": "countries_profile",
        }

    for s in cfg.sources:
        if s.transformer != "worldbank_indicator":
            continue
        path = RAW / f"{s.id}.json"
        if not path.exists():
            continue
        for rec in _read_json(path)[1] or []:
            code = rec.get("countryiso3code") or ""
            iso3 = cfg.code_equivalences.get(code, code)
            key = iso3 or (rec.get("country") or {}).get("value", "")
            if not key or iso3 in aceptados or iso3 in en_perfil or key in rows:
                continue
            rows[key] = {
                "iso3": iso3,
                "name": (rec.get("country") or {}).get("value"),
                "motivo": "agregado WB o entidad sin perfil",
                "fuente": s.id,
            }

    return pd.DataFrame(list(rows.values()))


def _is_indicator_source(s: Source) -> bool:
    if s.role == "indicator":
        return True
    if (s.meta or {}).get("indicator_id"):
        return True
    return False


def build_orphans(cfg: Config, countries: pd.DataFrame,
                  indicators: pd.DataFrame) -> pd.DataFrame:
    cols = ["iso3", "source_id", "indicator_id", "missing_in"]
    if countries.empty:
        return pd.DataFrame(columns=cols)
    if indicators.empty:
        con_dato: set[tuple] = set()
    else:
        ok = indicators[indicators["value"].notna()]
        con_dato = set(zip(ok["iso3"], ok["source_id"], ok["indicator_id"]))

    rows = []
    for iso3 in countries["iso3"].tolist():
        for s in cfg.sources:
            if not _is_indicator_source(s):
                continue
            ind_id = (s.meta or {}).get("indicator_id", s.id)
            if (iso3, s.id, ind_id) not in con_dato:
                rows.append({"iso3": iso3, "source_id": s.id,
                             "indicator_id": ind_id, "missing_in": "indicators"})
    return pd.DataFrame(rows, columns=cols)


def run_transform(cfg: Config | None = None) -> dict[str, pd.DataFrame]:
    cfg = cfg or load_config()
    ensure_dirs()
    buckets: dict[str, list[pd.DataFrame]] = {}

    for source in cfg.sources:
        name = source.transformer
        if name not in TRANSFORMERS:
            raise KeyError(f"Transformer '{name}' no registrado. "
                           f"Disponibles: {sorted(TRANSFORMERS)}")
        print(f"[transform] {source.id} → {name}")
        out = TRANSFORMERS[name](source, cfg)
        for table, df in out.items():
            buckets.setdefault(table, []).append(df)

    merged: dict[str, pd.DataFrame] = {}
    for table, parts in buckets.items():
        parts = [p for p in parts if not p.empty and p.shape[1] > 0]
        if not parts:
            print(f"[transform] {table}: sin partes no vacías, se omite")
            continue
        df = pd.concat(parts, ignore_index=True)
        written = _write_clean(table, df)
        if not written.empty:
            merged[table] = written

    excl = build_excluded(cfg)
    excl.to_csv(CLEAN / "excluded_entities.csv", index=False)
    print(f"[transform] excluidos: {len(excl)}")

    orphans = build_orphans(
        cfg,
        merged.get("countries", pd.DataFrame()),
        merged.get("indicators", pd.DataFrame()),
    )
    orphans.to_csv(CLEAN / "join_orphans.csv", index=False)
    print(f"[transform] huérfanos: {len(orphans)}")

    return merged


if __name__ == "__main__":
    run_transform()
