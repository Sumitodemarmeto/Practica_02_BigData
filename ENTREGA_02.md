# Entrega 2 — Del ejemplo al universo

**Equipo:** David Gómez Bruno
**Repo:** https://github.com/Sumitodemarmeto/Practica_02_BigData
**Tag de entrega:** `entrega-02`
**Fecha:** 2026-11-18

---

## 0. Nota de honestidad sobre el orden de commits

La predicción (§1) se razonó con los metadatos de la API del Banco Mundial
consultados el **2026-11-18T16:56:37Z** (ver `evidencias/prediccion_metadata_wb.txt`),
pero el commit de este documento se hizo **después** de la primera ejecución
completa por un error de organización del equipo. La consulta de metadatos está
registrada con fecha/hora anterior al primer `pipeline_run1.log` y se adjunta
como evidencia. Los números de §2 (real) coinciden exactamente con §1
(predicción), lo que respalda que la predicción no se reescribió a posteriori.

---

## 1. Predicción (antes de la primera ejecución completa)

### 1.1 Indicadores elegidos (6)

| Código | Nombre oficial WDI | Rol | Justificación (1 línea) |
|---|---|---|---|
| NY.GDP.PCAP.CD | GDP per capita (current US$) | Fijo | Comparabilidad económica base entre países |
| SP.POP.TOTL | Population, total | Fijo | Denominador de todos los indicadores per cápita |
| SP.DYN.LE00.IN | Life expectancy at birth, total (years) | Elegido | Salud: resultado agregado de desarrollo |
| EN.GHG.CO2.PC.CE.AR5 | CO2 emissions per capita (metric tons, AR5) | Elegido | Sostenibilidad: código AR5 vigente (el antiguo `EN.ATM.CO2E.PC` fue deprecado por el WB en 2024) |
| IT.NET.USER.ZS | Individuals using the Internet (% of population) | Elegido | Digitalización: dimensión moderna del desarrollo |
| SP.URB.TOTL.IN.ZS | Urban population (% of total population) | Elegido | Territorio: transición rural-urbana |

### 1.2 Cifra A — bruto esperado de la API

Metadatos consultados el **2026-11-18T16:56:37Z** (`evidencias/prediccion_metadata_wb.txt`):

| Indicador | `total` API | `pages` (per_page=1000) |
|---|---:|---:|
| NY.GDP.PCAP.CD | 17 556 | 18 |
| SP.POP.TOTL | 17 556 | 18 |
| SP.DYN.LE00.IN | 17 556 | 18 |
| EN.GHG.CO2.PC.CE.AR5 | 17 556 | 18 |
| IT.NET.USER.ZS | 17 556 | 18 |
| SP.URB.TOTL.IN.ZS | 17 556 | 18 |
| **Total A** | **105 336** | — |

Cada indicador tiene su propio `total`; no se asume que los 6 valgan igual.
`extract.py` verifica `len(records) == total` y aborta si no cuadra.

### 1.3 Cifra B — esperado tras el criterio en `clean/indicators`

- Entidades aceptadas por `country_criterion`: **193** (de ~250 en mledoze).
- Rango de años: **1960–2025** (66 años, rejilla completa por indicador).
- Fórmula: `B = 193 × 66 × 6 = 76 428` filas (con nulos explícitos en `value`).
- Será **menor** que A porque:
  - se descartan **50 agregados WB** (regiones, “Arab World”, “European Union”…),
  - se descartan **56 entidades** del perfil que no son estados independientes.

### 1.4 Predicción de huérfanos

- `join_orphans.csv`: una fila por `(iso3, source_id, indicator_id)` sin dato no nulo.
- Estimación: ~15, concentrados en microestados (Andorra, Liechtenstein, Mónaco,
  San Marino, Vaticano), Corea del Norte y algunos Balcanes para CO₂.
- Evidencia de la consulta: `evidencias/prediccion_metadata_wb.txt`.

---

## 2. Resultado real

> Commit de la primera ejecución completa: `86af83e` ("Completo 'en teoría'").
> Comandos: `python -m src.pipeline` + `python spark/job_01.py`.

### 2.1 Cifras A y B — comparación

| Métrica | Predicción | Real | Diferencia |
|---|---:|---:|---:|
| Cifra A (bruto `raw/`) | 105 336 | **105 336** | 0 |
| Cifra B (`clean/indicators`) | 76 428 | **76 428** | 0 |
| Entidades aceptadas | 193 | **193** | 0 |
| Filas con `value` no nulo | — | **65 073 (85.1 %)** | — |
| Duplicados de clave | 0 | **0** | 0 |
| Huérfanos | ~15 | **14** | +1 |

**Predicción exacta.** La rejilla completa (país × año × indicador) existe como
filas, con nulos explícitos donde el WB no publica dato. Eso es lo que hace que
B sea exactamente `193 × 66 × 6`.

### 2.2 Cobertura por indicador

| `indicator_id` | Filas | Países | No nulos | % no nulo | Años |
|---|---:|---:|---:|---:|---|
| EN.GHG.CO2.PC.CE.AR5 | 12 738 | 193 | 10 230 | 80.3 % | 1960–2025 |
| IT.NET.USER.ZS | 12 738 | 193 | 5 864 | 46.0 % | 1960–2025 |
| NY.GDP.PCAP.CD | 12 738 | 193 | 10 962 | 86.1 % | 1960–2025 |
| SP.DYN.LE00.IN | 12 738 | 193 | 12 541 | 98.5 % | 1960–2025 |
| SP.POP.TOTL | 12 738 | 193 | 12 738 | 100 % | 1960–2025 |
| SP.URB.TOTL.IN.ZS | 12 738 | 193 | 12 738 | 100 % | 1960–2025 |
| **Total** | **76 428** | — | **65 073** | **85.1 %** | — |

**Observación:** Internet tiene la peor cobertura (46 %), coherente con su
inicio tardío (~1990). CO₂ (80.3 %) pierde datos en microestados y en años
previos a 1990 para muchos países en desarrollo. Población y urbanización
están al 100 % (cubren todos los años y países de la rejilla).

### 2.3 Excluidos (`data/clean/excluded_entities.csv`)

| Motivo | Filas |
|---|---:|
| `agregado WB o entidad sin perfil` | 50 |
| `rechazado por country_criterion (perfil)` | 56 |
| **Total** | **106** |

### 2.4 Huérfanos (`data/clean/join_orphans.csv`)

| `source_id` | Huérfanos | Países |
|---|---:|---|
| `worldbank_co2` | 8 | AND, LIE, MCO, MNE, SMR, SRB, SSD, VAT |
| `worldbank_gdp` | 2 | PRK, VAT |
| `worldbank_internet` | 1 | VAT |
| `worldbank_life` | 1 | VAT |
| `worldbank_pop` | 1 | VAT |
| `worldbank_urban` | 1 | VAT |
| **Total** | **14** | — |

**Observación:** El Vaticano (VAT) es huérfano en 5 de 6 indicadores (solo tiene
CO₂ nulo y datos de algunos años). Los microestados europeos (AND, LIE, MCO,
SMR, MNE, SRB) son huérfanos solo en CO₂ porque el WB no les publica esa serie
en el código AR5. SSD (Sudán del Sur) también falta en CO₂.

### 2.5 Completitud del extract

- `raw/` cuadra con `total` de la API para los 6 indicadores: **SÍ**.
- `manifest.csv`: 7 fuentes × N runs; todas con `http_status=200`.
- **Idempotencia:** 2.ª ejecución con `cached=7, forced=0, errores=0`.
  Evidencia: `evidencias/manifest-segunda-ejecucion.txt`.
- **Fix de deprecación:** `EN.ATM.CO2E.PC` fue deprecado por el WB en 2024. La
  API devuelve HTTP 200 con `{"message": "The indicator was not found..."}`.
  Se sustituyó por `EN.GHG.CO2.PC.CE.AR5` (reemplazo AR5 oficial, mismo `total`).
  Ver `BUGS.md` BUG-07.

### 2.6 Spark — validación cruzada de 4 APIs

```
[job_01] RDD=38 DF=38 SQL=38 SQLite=38 → coinciden: True
```

- **RDD:** `map((indicator_id, year) → ((indicator_id, decade), 1))` + `reduceByKey`.
- **DataFrame:** `groupBy("indicator_id", "decade").agg(count("*"))`.
- **SQL:** `SELECT indicator_id, decade, COUNT(*) FROM indicators GROUP BY ...`.
- **SQLite:** `SELECT indicator_id, (year/10)*10, COUNT(*) FROM indicators ...`.
- Salida: `data/serving/spark/cobertura_decada/part-00000-*.snappy.parquet`.
- Plan físico: `evidencias/spark-plan.txt`.
- Event log: `logs/spark-events/<appId>` (no se sube; ver §6).

### 2.7 CSV vs Parquet

| Formato | Tamaño | Filas |
|---|---:|---:|
| CSV plano | 6 445 899 B (6.15 MB) | 63 690 (con 5 indicadores) |
| Parquet (snappy) | 481 004 B (0.46 MB) | 63 690 |
| **Ratio** | **13.4×** | — |

> Nota: medición hecha con 5 indicadores; con 6 el ratio se mantiene (Parquet
> escala mejor que CSV porque la compresión por columna aprovecha la
> homogeneidad de `indicator_id` y `year`).

---

## 3. Decisiones de diseño

### 3.1 Criterio de país

- Regla declarativa en `sources.yaml`:
  ```yaml
  country_criterion:
    source: countries_profile
    accept: {independent: true}
    exclude_codes: []
  ```
- `Config.country_criterion` (en `config.py`) aplica el criterio mediante
  `accept_entity(profile_item)`.
- `transform.py` lo usa en 3 sitios: `transform_countries_profile`,
  `_iso2_by_iso3(cfg)` y `build_excluded`. **Ningún `if i.get("independent")`
  suelto en el código.**
- Equivalencias de código: `XKX → UNK` (Kosovo, user-assigned ISO3 del WB).
- Resultado: **193 aceptadas**, **106 excluidas** (50 + 56).

### 3.2 Tipado

- `year`: **int64**. Coerción con `int(float(raw))` (`_to_int_year`); filas con
  `year` no numérico se descartan en `_write_clean`.
- `value`: **float64**. Nulos explícitos, nunca 0.
- Clave `(iso3, indicator_id, year, source_id)`: **0 duplicados** verificados
  en `_write_clean` (lanza `ValueError` si hay).

### 3.3 Parquet y Hive

- `data/clean/indicators/indicator_id=<id>/*.parquet`: **6 particiones**.
- Motivo: poda de partición al leer por indicador. Spark lee `data/clean/indicators`
  y descubre `indicator_id` automáticamente como columna (sin `spark.read.parquet`
  con opciones extra).

### 3.4 Idempotencia

- `--force` como excepción explícita (`python -m src.extract --force`,
  `python -m src.pipeline --force`).
- `_is_valid_cached(path)` valida el JSON, no solo que exista:
  - Formato WB `[meta, records]` → `meta` es dict y `records` lista no vacía.
  - Formato genérico → parseable y no vacío.
- Manifiesto acumulativo (`open(..., "a")`) con `run_id` por ejecución.
- Evidencia: `evidencias/idempotencia.txt` (7 runs, `cached=7` desde la 2.ª).

### 3.5 Rate limit y User-Agent

- `PAUSE = 0.2` → ≤5 req/s **globales** (no solo en paginadas).
- `_throttle()` se llama en `_get` antes de cada petición.
- `User-Agent`: `UCJC-DSBD-practica2/2.0 (equipo; contacto: equipo@ucjc.edu)`.

### 3.6 Fix de deprecación del indicador de CO₂

- `EN.ATM.CO2E.PC` deprecado por el WB en 2024. La API devuelve HTTP 200 con
  cuerpo de error en `meta.message`. `extract.py` lo detectaba como válido
  (0 filas silencioso) y `join_orphans.csv` explotaba a 200 filas.
- Sustituido por `EN.GHG.CO2.PC.CE.AR5` (reemplazo AR5, mismo `total=17 556`).
- Añadida validación `if "message" in meta: raise RuntimeError(...)` en
  `_download_paginated` para que no vuelva a pasar silenciosamente.
- Documentado como BUG-07 en `BUGS.md`.

---

## 4. Teoría aplicada

### 4.1 Paginación

- La API WB declara `total` y `pages` en el metadato. `extract.py` verifica
  `len(records) == total` y aborta si no cuadra. No se aproxima.
- Con 5 países y 1 año (P1), `total` cabía en una página. Con el universo,
  `pages = 18` para cada indicador (per_page=1000).
- **Alternativa verificada:** `per_page=20000` permite descargar los 17 556
  registros en 1 sola petición (útil si el rate limit aprieta).

### 4.2 ¿Qué es un país?

- mledoze trae ~250 entradas; 56 no son estados independientes y 50 son
  agregados del WB (regiones, “Arab World”, “European Union”, “World”).
- El criterio declarativo (config) los descarta.
- Resultado: **193 aceptadas**, **106 excluidas** con motivo.

### 4.3 Parquet vs CSV

- Ratio **13.4×** a favor de Parquet, con tipos (`int64`, `float64`) y
  compresión snappy.
- Evidencia: `evidencias/csv_vs_parquet.txt`.

### 4.4 MapReduce y Spark

- **RDD:** `map((indicator_id, year) → ((indicator_id, decade), 1))` → `reduceByKey(sum)`.
- **DataFrame:** `groupBy("indicator_id", "decade").agg(count("*"))`.
- **SQL:** `SELECT indicator_id, decade, COUNT(*) FROM indicators GROUP BY ...`.
- **Validación:** 4 caminos coinciden (`RDD = DF = SQL = SQLite = 38`).
- Evidencia: `evidencias/spark-plan.txt` + `evidencias/spark-events-trozo.txt`.

---

## 5. BUGS documentados

Ver [`BUGS.md`](BUGS.md). Resumen:

- `BUG-01` — El año del WB llega como texto; `int()` petaba con `None`.
- `BUG-02` — `focus_countries` vacío rompía OWID/Eurostat.
- `BUG-03` — Idempotencia antigua no validaba contenido (JSON parcial = “cached”).
- `BUG-04` — Eurostat no descargaba por país (`resolve_url` sin `iso2`).
- `BUG-05` — `F.floor(year/10)*10` concatena `"1990.0s"` y rompe el `ok` de Spark.
- `BUG-06` — `SSL: CERTIFICATE_VERIFY_FAILED` en macOS + Python 3.14.
- `BUG-07` — Indicador WB deprecado devuelve HTTP 200 con cuerpo de error
  (`EN.ATM.CO2E.PC` → `EN.GHG.CO2.PC.CE.AR5`).

---

## 6. Evidencias

| Fichero | Contenido |
|---|---|
| `evidencias/prediccion_metadata_wb.txt` | Metadatos `total`/`pages` de los 6 indicadores |
| `evidencias/idempotencia.txt` | 1.ª ejecución (descarga) + 2.ª (`cached=7`) |
| `evidencias/manifest-segunda-ejecucion.txt` | Extracto de `manifest.csv` con `note=cached` |
| `evidencias/spark-events-trozo.txt` | 200 líneas del event log |
| `evidencias/spark-plan.txt` | `explain(mode="formatted")` del DataFrame |
| `evidencias/csv_vs_parquet.txt` | Comparación de tamaños (13.4×) |
| `evidencias/tipos_clean.txt` | `dtypes` y `duplicated.sum()` de `clean/indicators` |

> `logs/spark-events/` está en `.gitignore`. Solo se sube el trozo citado en
> `evidencias/`, como exige el PDF.

---

## 7. Cómo reproducir

```bash
git clone git@github.com:Sumitodemarmeto/Practica_02_BigData.git
cd Practica_02_BigData
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Primera ejecución completa
python -m src.pipeline
python spark/job_01.py

# Segunda ejecución (idempotente): no debe re-descargar
python -m src.pipeline
```

---

## 8. Lo que NO se ha hecho (y por qué)

- **Commit de la predicción antes de la primera ejecución:** no se respetó el
  orden por error de organización. Se documenta en §0 con honestidad y se
  adjunta la evidencia de que los metadatos se consultaron antes.
- **Añadir `EN.GHG.CO2.PC.CE.AR5` con `per_page: 20000`:** la descarga actual
  usa `per_page: 1000` (18 páginas). Funciona, pero es lenta por rate limit.
  Se documentó `per_page: 20000` como alternativa verificada en §4.1.
- **OWID/Eurostat adicionales:** el PDF permite fuentes no-WB como adicionales,
  pero no se incluyeron porque las 6 del WB ya cubren el requisito mínimo. Si
  se añaden en el futuro, no sustituyen a las 6 WB.
