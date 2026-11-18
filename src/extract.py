"""Extract: descarga intacta → data/raw/ + manifiesto (idempotente, --force)."""

from __future__ import annotations

import csv
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

from .config import Config, Source, load_config
from .paths import MANIFEST, RAW, ensure_dirs

USER_AGENT = "UCJC-DSBD-practica2/2.0 (equipo; contacto: equipo@ucjc.edu)"
TIMEOUT = 60
RETRY_CODES = {429, 500, 501, 502, 503, 504, 505}
PAUSE = 0.2
MAX_RETRIES = 5

_last_request_ts: float = 0.0


def _throttle() -> None:
    global _last_request_ts
    delta = time.time() - _last_request_ts
    if delta < PAUSE:
        time.sleep(PAUSE - delta)
    _last_request_ts = time.time()


def _get(url: str, retries: int = MAX_RETRIES) -> requests.Response:
    last_err = ""
    for attempt in range(retries):
        _throttle()
        try:
            r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
            if r.status_code == 200:
                return r
            if r.status_code not in RETRY_CODES:
                raise RuntimeError(f"HTTP {r.status_code} no reintentable: {url}")
            last_err = f"HTTP {r.status_code}"
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_err = type(exc).__name__
        if attempt < retries - 1:
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Reintentos agotados ({last_err}): {url}")


def _is_valid_cached(path: Path) -> bool:
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        if path.suffix == ".json":
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, list) and len(data) == 2:
                meta, records = data
                return isinstance(meta, dict) and isinstance(records, list) and len(records) > 0
            return bool(data)
        return True
    except (json.JSONDecodeError, OSError, UnicodeDecodeError):
        return False


def _download_paginated(url: str, per_page: int, dest: Path) -> None:
    sep = "&" if "?" in url else "?"
    records: list = []
    page, pages, total = 1, 1, 0
    meta: dict = {}
    while page <= pages:
        payload = _get(f"{url}{sep}per_page={per_page}&page={page}").json()
        if not isinstance(payload, list) or len(payload) < 1:
            raise RuntimeError(f"Respuesta inesperada: {url} pág {page}")
        meta = payload[0] or {}
        data = (payload[1] if len(payload) > 1 else None) or []
        pages = int(meta.get("pages", 1))
        total = int(meta.get("total", 0))
        records.extend(data)
        page += 1
    if len(records) != total:
        raise RuntimeError(f"Guardadas {len(records)} ≠ total API {total}: {url}")
    dest.write_text(json.dumps([meta, records]), encoding="utf-8")


def _fetch_one(source: Source, url: str, dest: Path, run_id: str,
               iso2: str, iso3: str, now: str, force: bool) -> dict:
    nbytes, note, status = 0, "", 200
    if dest.exists() and not force and _is_valid_cached(dest):
        nbytes, note = dest.stat().st_size, "cached"
    else:
        try:
            if source.paginated:
                _download_paginated(url, source.per_page, dest)
            else:
                dest.write_bytes(_get(url).content)
            nbytes = dest.stat().st_size
            if force:
                note = "forced"
        except RuntimeError as exc:
            status, note = 0, str(exc)
    return {
        "run_id": run_id, "timestamp_utc": now, "source_id": source.id,
        "url": url, "http_status": status, "bytes": nbytes,
        "filename": dest.name, "iso2": iso2, "iso3": iso3, "note": note,
    }


def _is_per_country(source: Source) -> bool:
    tpl = source.url_template or ""
    return "{iso2}" in tpl or "{iso3}" in tpl


def extract_source(source: Source, cfg: Config, run_id: str,
                   force: bool = False) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    if _is_per_country(source):
        covers = list((source.meta or {}).get("covers") or [])
        if not covers:
            covers = [c.iso2 for c in cfg.focus_countries]
        if not covers:
            print(f"[extract] {source.id}: sin países (ni meta.covers ni focus_countries)")
            return []
        iso2_to_iso3 = {c.iso2: c.iso3 for c in cfg.focus_countries}
        rows = []
        for iso2 in covers:
            iso3 = iso2_to_iso3.get(iso2, "")
            try:
                url = source.resolve_url(iso2=iso2, iso3=iso3)
            except ValueError as exc:
                print(f"[extract] {source.id} ({iso2}): {exc}")
                continue
            dest = RAW / f"{source.id}_{iso2}.{source.format}"
            rows.append(_fetch_one(source, url, dest, run_id, iso2, iso3, now, force))
        return rows

    try:
        url = source.resolve_url()
    except ValueError as exc:
        print(f"[extract] {source.id}: {exc}")
        return []
    dest = RAW / f"{source.id}.{source.format}"
    return [_fetch_one(source, url, dest, run_id, "", "", now, force)]


def run_extract(cfg: Config | None = None, force: bool = False) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    run_id = (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
              + "-" + uuid.uuid4().hex[:6])
    all_rows: list[dict] = []
    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}" + (" [FORCE]" if force else ""))
        try:
            rows = extract_source(source, cfg, run_id, force=force)
        except Exception as exc:  # noqa: BLE001
            print(f"[extract] {source.id}: ERROR {exc}")
            rows = [{
                "run_id": run_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "source_id": source.id, "url": "", "http_status": 0,
                "bytes": 0, "filename": "", "iso2": "", "iso3": "",
                "note": f"exception: {exc}",
            }]
        all_rows.extend(rows)

    fieldnames = ["run_id", "timestamp_utc", "source_id", "url", "http_status",
                  "bytes", "filename", "iso2", "iso3", "note"]
    nuevo = not MANIFEST.exists()
    with MANIFEST.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if nuevo:
            writer.writeheader()
        writer.writerows(all_rows)

    cached = sum(1 for r in all_rows if r["note"] == "cached")
    forced = sum(1 for r in all_rows if r["note"] == "forced")
    errores = sum(1 for r in all_rows if r["http_status"] == 0)
    print(f"[extract] run {run_id}: {len(all_rows)} ficheros "
          f"(cached={cached}, forced={forced}, errores={errores}) → {MANIFEST}")
    return MANIFEST


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    run_extract(force=args.force)
