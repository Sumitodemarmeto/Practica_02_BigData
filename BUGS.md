# BUGS detectados al escalar al universo

## BUG-01 — El año del WB llega como texto y `int()` petaba con `None`

- **Síntoma:** `int(rec["date"])` funcionaba con 5 países y 1 año. En el universo,
  hay registros con `date: null` o `""` en indicadores de cobertura parcial
  (CO₂, Internet). El transform crasheaba con `TypeError`.
- **Fix:** `_to_int_year(raw)` con `try/except` y coerción `int(float(raw))`.
  Las filas con año no numérico se descartan en `_write_clean` con aviso.
- **Evidencia:** `evidencias/tipos_clean.txt` (`year: int64`, 0 nulos).

## BUG-02 — `focus_countries` vacío rompía OWID/Eurostat

- **Síntoma:** al vaciar `focus_countries` para el universo, `transform_owid_*`
  y `transform_eurostat_*` devolvían `pd.DataFrame()` sin columnas. El `concat`
  global generaba un DataFrame sin columnas y `_write_clean` petaba en `df["year"]`.
- **Fix:** `_write_clean` comprueba `df.empty or df.shape[1] == 0`; `run_transform`
  filtra `parts` vacías antes del `concat`. OWID filtra por `_accepted_iso3(cfg)`,
  no por `focus_countries`.

## BUG-03 — Idempotencia antigua no validaba contenido

- **Síntoma:** `if dest.exists()` reutilizaba cualquier fichero, incluso un JSON
  parcial de un run fallido o un cuerpo de error de la API.
- **Fix:** `_is_valid_cached(path)` parsea el JSON y verifica `[meta, records]`
  con `records` no vacío. Un JSON corrupto fuerza re-descarga.

## BUG-04 — Eurostat no descargaba por país

- **Síntoma:** `resolve_url()` sin `iso2` generaba una URL con `iso2=""`, y
  `transform_eurostat_demo_pjan` esperaba `{id}_{iso2}.json` que nunca existía.
- **Fix:** `_is_per_country(source)` detecta `{iso2}` en `url_template`.
  `extract_source` itera `meta.covers` (o `focus_countries` como fallback) y
  genera un fichero por país.

## BUG-05 — `F.floor(year/10) * 10` concatena `"1990.0s"` en Spark

- **Síntoma:** en `job_01.py`, `res_rdd == res_sqlite` (`"1990s"`) pero
  `res_df == res_sql` (`"1990.0s"`), porque `F.floor` devuelve DOUBLE y
  `F.concat` lo convierte a string. La comparación final `ok` daba `False`.
- **Fix:** `F.floor(...).cast("int") * 10` antes del `concat`, o
  `year - year % 10` (aritmética entera).

## BUG-06 — `SSL: CERTIFICATE_VERIFY_FAILED` en macOS + Python 3.14

- **Síntoma:** `urllib.request.urlopen` fallaba al consultar la API WB con
  `certificate verify failed: unable to get local issuer certificate`.
- **Fix:** `pip install --upgrade certifi` + `ssl.create_default_context(cafile=certifi.where())`
  en los scripts de consulta. Añadido `certifi` a `requirements.txt`.

## BUG-07 — Indicador WB deprecado devuelve HTTP 200 con cuerpo de error

- **Síntoma:** `EN.ATM.CO2E.PC` fue archivado por el Banco Mundial (2024). La API
  responde **HTTP 200** con `{"message": [{"id": "175", "value": "The indicator
  was not found..."}]}` en vez de datos. `extract.py` lo guardaba como válido
  (138 bytes, `len(records)=0`) y `transform.py` producía 0 filas
  silenciosamente. Efecto: `indicators` se quedó en 63 690 filas (5 indicadores)
  y `join_orphans.csv` saltó de 6 a **200** filas (193 países nuevos sin CO₂).
- **Fix:** sustituir por `EN.GHG.CO2.PC.CE.AR5` (reemplazo AR5 oficial, mismo
  `total=17 556`). Añadir validación `if "message" in meta: raise RuntimeError(...)`
  en `_download_paginated` para rechazar respuestas de error.
- **Verificación:** `len(records) == meta.total == 17 556`; `indicators` sube a
  **76 428** filas; `join_orphans.csv` baja a **14** filas.
- **Commit:** `<hash>`
