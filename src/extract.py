"""Extract: descarga intacta → data/raw/ + manifiesto."""

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

USER_AGENT = "UCJC-DSBD-practica2/1.0 (educational ETL framework)"
TIMEOUT = 60
RETRY_CODES = {429, 500, 502, 503, 504}
PAUSE = 0.25  # ≤ 5 peticiones/s


def _get(url: str, retries: int = 5) -> requests.Response:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
            if r.status_code == 200:
                return r
            if r.status_code not in RETRY_CODES:
                raise RuntimeError(f"HTTP {r.status_code} no reintentable: {url}")
        except (requests.Timeout, requests.ConnectionError):
            pass
        time.sleep(2 ** attempt)  # 1, 2, 4, 8, 16 s
    raise RuntimeError(f"Reintentos agotados: {url}")


def _download_paginated(url: str, per_page: int, dest: Path) -> None:
    sep = "&" if "?" in url else "?"
    records, page, pages, total, meta = [], 1, 1, 0, {}
    while page <= pages:
        payload = _get(f"{url}{sep}per_page={per_page}&page={page}").json()
        meta, data = payload[0], payload[1] or []
        pages, total = int(meta["pages"]), int(meta["total"])
        records.extend(data)
        page += 1
        time.sleep(PAUSE)
    if len(records) != total:
        raise RuntimeError(f"Guardadas {len(records)} ≠ total API {total}: {url}")
    # Mismo formato que devuelve el Banco Mundial: [meta, [registros]]
    dest.write_text(json.dumps([meta, records]), encoding="utf-8")

def extract_source(source: Source, cfg: Config, run_id: str) -> list[dict]:
    now = datetime.now(timezone.utc).isoformat()
    url = source.resolve_url()
    filename = f"{source.id}.{source.format}"
    dest = RAW / filename
    status, nbytes, note = 200, 0, ""

    if dest.exists():                      # idempotencia
        nbytes, note = dest.stat().st_size, "cached"
    else:
        try:
            if source.paginated:
                _download_paginated(url, source.per_page, dest)
            else:
                dest.write_bytes(_get(url).content)
            nbytes = dest.stat().st_size
        except RuntimeError as exc:
            status, note = 0, str(exc)

    return [{
        "run_id": run_id,
        "timestamp_utc": now, "source_id": source.id, "url": url,
        "http_status": status, "bytes": nbytes, "filename": filename,
        "iso2": "", "iso3": "", "note": note,
    }]

def run_extract(cfg: Config | None = None) -> Path:
    cfg = cfg or load_config()
    ensure_dirs()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    all_rows: list[dict] = []
    for source in cfg.sources:
        print(f"[extract] {source.id} — {source.name}")
        all_rows.extend(extract_source(source, cfg, run_id))

    fieldnames = ["run_id", "timestamp_utc", "source_id", "url", "http_status",
                  "bytes", "filename", "iso2", "iso3", "note"]
    nuevo = not MANIFEST.exists()
    with MANIFEST.open("a", encoding="utf-8", newline="") as f:   # "a": no borra historia
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if nuevo:
            writer.writeheader()
        writer.writerows(all_rows)

    print(f"[extract] manifiesto (run {run_id}) → {MANIFEST}")
    return MANIFEST

if __name__ == "__main__":
    run_extract()
