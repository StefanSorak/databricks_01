# CLAUDE.md

Guidance for Claude Code when working in this repo.

## What this is

A portfolio data engineering project: an end-to-end NYC 311 pipeline on **Databricks Free Edition**
(serverless compute) using PySpark, Delta Lake, and a medallion architecture.

```
NYC Open Data 311 API (Socrata)
  → landing zone   (UC Volume, gzipped NDJSON)
  → bronze         (raw Delta, all strings)
  → silver         (cleaned/typed/deduped Delta)
  → gold           (aggregates)          [not built yet]
  → Databricks SQL dashboard             [not built yet]
```

Source API: `https://data.cityofnewyork.us/resource/erm2-nwe9.json` (no key required; a free app
token via `X-App-Token` header raises rate limits and should live in a Databricks secret, never git).
Phase 4 enrichment: Open-Meteo historical weather API (free, no key), daily grain, NYC.

The full design rationale, decision log, and business requirements live in
**`NYC 311 Pipeline — Project Plan`** (the planning doc the user maintains). This file is the
quick-orientation summary; when they conflict, trust the code and re-confirm against the plan.

## Core principle

**Notebooks orchestrate, `src/` implements.** Notebooks read config, call one function, print the
result. All real logic lives in pure, importable, testable functions in `src/nyc311/`.

- Transform functions are **pure**: no `spark.read`, no writes, no config lookups, no prints.
  Params via defaulted args or module constants.
- One *concern* per function, not one column per function.
- Everything **idempotent**: `IF NOT EXISTS`, watermark, `MERGE`. Any step is safe to rerun.
- Dropping columns in silver is cheap and reversible — bronze retains all 44 source columns.

## Repo layout (actual state)

```
conf/config.yaml            catalog: nyc311 / schema: pipeline / landing_volume: landing
                            start_date: "2026-07-01T00:00:00"
notebooks/                  *.py  (Databricks source format — see "Notebook format" below)
  00_setup.py               creates catalog/schema/volume from config
  01_ingest.py              calls run_ingestion()
  02_bronze.py              Auto Loader landing NDJSON w/ explicit schema → bronze_complaints + verify cells
  03_silver.py              build_silver() → MERGE into silver_complaints
src/nyc311/
  api_client.py             ✅ Socrata client: read/write_watermark, fetch_pages, write_ndjson_gz, run_ingestion
  schema.py                 ✅ BRONZE_COLUMNS (44) + bronze_schema() all-string StructType
  transformations.py        ✅ silver: KEEP_COLUMNS + 6 pure steps + build_silver()
  quality.py                ⬜ empty — BR-5 checks not yet centralized here
  __init__.py               empty
requirements.txt            empty
README.md                   stub only ("# databricks_01")
```

**Not yet created:** `src/nyc311/aggregations.py`, `src/nyc311/weather.py`, `notebooks/04_gold`,
`notebooks/99_analysis`, `tests/`, `resources/nyc311_job.yaml`.

## Conventions

- **Unity Catalog:** dedicated catalog `nyc311`, schema `pipeline`, volume `landing`.
  Landing path: `/Volumes/nyc311/pipeline/landing/`. (Workspace default catalog is `workspace`.)
- **Medallion layers = table prefixes** (`bronze_`, `silver_`, `gold_`) in one schema, not
  schema-per-layer. Deliberate choice for a solo project.
- **Tables are managed** (`saveAsTable`, no explicit paths). External tables are a parked S3 stretch goal.
- **Landing layout:** `ingest_date=YYYY-MM-DD/page_NNNN.json.gz` (gzipped NDJSON, one record per
  line). `key=value` folder naming drives Spark partition discovery. `_watermark/watermark.json`
  sits alongside; the `ingest_date=*/` glob in bronze skips it.
  - Catalog Explorer cannot preview `.gz` — inspect via `gzip.open` or `spark.read.json` in a notebook.
- **Config is the single source of truth** — `conf/config.yaml`. Every notebook reads it.
- **Notebook format:** notebooks are committed as `.py` in **Databricks source format** — first
  line `# Databricks notebook source`, cells separated by `# COMMAND ----------`, magic commands
  prefixed `# MAGIC`. Chosen over `.ipynb` for clean git diffs and nvim editing; cell outputs
  aren't committed and the pipeline can't run locally anyway (Free Edition compute is remote).
  Databricks Git folders import these as real notebooks. `03_silver` calls
  `dbutils.library.restartPython()` in its first cell.
- **Import workaround** for Free Edition — inconsistent across notebooks, tolerate both:
  - `02`/`03`: `sys.path.append(os.path.abspath("../src"))` then `from nyc311.x import ...`
  - `01`: `from src.nyc311.api_client import ...`

## Pipeline stage details

### Ingestion (`api_client.py`) — ✅ working

- **Watermark / high-water-mark:** stores max `created_date` landed, in
  `_watermark/watermark.json` (JSON key `max_created_date`). Cycle: read watermark → fetch records
  `created_date > watermark` (SoQL `$where`, `$order=:id` for stable pagination, `$limit`/`$offset`,
  `PAGE_SIZE = 50_000`) → land files → **then** advance watermark. Gives at-least-once delivery
  (crash re-fetches cheap dupes, never gaps; silver dedupes).
- **Known limitation:** a `created_date` watermark misses *updates* to existing complaints (a
  closure doesn't change `created_date`). v2 fix: trailing re-pull window on Socrata `:updated_at`.
- **Backfill:** `start_date` in config, not full 35M-row history. Widening = edit config + rerun.
  BR-3 (heating season) will need `start_date` moved back to cover a full winter before phase 4.
- **Verification ritual:** run ingestion twice; second run lands ~zero new records.

### Bronze (`02_bronze`, `schema.py`) — Auto Loader rewrite written, not yet run on Databricks

- Explicit **all-string** schema (44 cols) from `schema.py` — no inference (deterministic columns,
  no scan cost, no drift). Auto Loader adds a `_rescued_data` column, so a source field missing
  from `BRONZE_COLUMNS` now lands there instead of being silently dropped. `02_bronze` cell 2 is a
  one-time inference diff (`set(inferred) - set(BRONZE_COLUMNS)`) — **still not run**.
- Metadata cols: `_ingested_at = current_timestamp()`, `_source_file = _metadata.file_path`.
- **Read: Auto Loader** — `spark.readStream.format("cloudFiles")`, `cloudFiles.format=json`,
  explicit `.schema(bronze_schema())`, `.load("{landing}/ingest_date=*/")` (glob skips
  `_watermark/` and `_checkpoints/`). Write: `.writeStream` with `trigger(availableNow=True)` +
  `checkpointLocation = {landing}/_checkpoints/bronze`, `.toTable(bronze_table)`, then
  `query.awaitTermination()`. The checkpoint tracks ingested files → **rerun-safe, no duplicate
  appends**. Replaced `spark.read.json(...).mode("append")`, which re-read every landing folder
  on each run.
- `02_bronze` cell 3 reconciles `spark.read.table(bronze_table).count()` against ingestion's
  reported record total (expect bronze ≥ total; at-least-once dupes get absorbed by silver) —
  **not yet run**.

### Silver (`03_silver`, `transformations.py`) — ✅ functionally complete

`KEEP_COLUMNS` (source → silver name) is the column-decision record and drives `select_and_rename`.
There is **no `schema.py` for silver, by design** — input is already a typed Delta table, output
shape is whatever the chain produces.

**Transform order** — `shape → standardize → type → dedupe → validate → derive`. Each step relies
on invariants from the prior one.

```python
def build_silver(df):
    return (df
        .transform(select_and_rename)   # 1. structural: select + rename via KEEP_COLUMNS
        .transform(normalize_strings)   # 2. trim, collapse whitespace, casing, sentinel→null
                                        #    (sentinels: "", "n/a", "unspecified", "unknown";
                                        #     nullify_sentinels was MERGED into this step, not separate)
        .transform(cast_types)          # 3. complaint_id→long, timestamps via try_to_timestamp (ANSI-safe)
        .transform(deduplicate)         # 4. Window partitionBy complaint_id
                                        #    orderBy last_updated_at desc_nulls_last, _ingested_at desc
        .transform(validate_bounds)     # 5. adds _duration_valid bool — FLAGS, never drops
        .transform(derive_metrics))     # 6. adds response_time_hours DECIMAL(10,2)
```

- **Dedup uses a Window row_number, not `dropDuplicates`** — need a guarantee about *which* row
  survives (latest lifecycle state). `_ingested_at` breaks ties for deterministic reruns.
  SCD Type 2 / status history was explicitly rejected (conflicts with BR-5, no BR needs it).
- **`normalize_strings` internals** (subtle, learned the hard way): builds a `cleaned` expression
  (`regexp_replace(trim(col), r"\s+", " ")`, original case preserved — this is what gets stored)
  separately from a throwaway `is_sentinel` test (`lower(cleaned).isin(...)`, comparison only).
  Reusing one lowercased expression for both silently lowercases every value. All-whitespace
  strings (`" "`, `"  "`) nullify for free because `trim` reduces them to `""` before the sentinel
  check. Timestamp columns (incl. `_ingested_at`) are skipped in the loop.
- `_duration_valid`: `true` if still open (`closed_at` null) or duration within 0–5 years;
  `false` if negative or implausibly long. `MAX_RESOLUTION_YEARS = 5`.
- `response_time_hours`: created→closed hours, rounded 2dp, stored `DECIMAL(10,2)` (not `float` —
  `.cast("float")` after `round(x, 2)` reintroduced base-2 noise like `1.57 → 1.5700000524520874`;
  `F.abs()` was also removed as dead weight now that `validate_bounds` runs first). **Divergence to
  resolve:** the committed `derive_metrics` computes it for every row with both timestamps and does
  *not* null it when `_duration_valid` is false. Chat handoff #2 records a `F.when(F.col("_duration_valid"), …)`
  gated version as the intended target. Until reconciled, Gold must filter on `_duration_valid`
  when aggregating.
- **Write: idempotent Delta `MERGE` on `complaint_id`** (create on first run via `saveAsTable`,
  `whenMatchedUpdateAll`/`whenNotMatchedInsertAll` after, gated on `spark.catalog.tableExists`).
  This replaced a copied-from-bronze `.mode("append")` that would have silently doubled every row
  (dedup only runs *within* one batch). Verified against the run-twice idempotency ritual.
- **`03_silver` logs the BR-5 bounds-violation count inline** right after `build_silver()`:
  `total = df.count()` / `invalid = df.filter(~F.col("_duration_valid")).count()` / print. This
  works only because `validate_bounds` flags rather than drops, so the flag survives to the final
  table and is countable after the fact — it does *not* measure `deduplicate`'s row loss.
- **Verified on real data:** first full run produced 519,870 silver rows; the BR-5 print reported
  `250/519870` rows flagged with implausible duration. Re-run confirmed idempotent (no doubling).
  Clean 2-decimal values, open-complaint handling (`closed_at` null → `response_time_hours` null,
  `_duration_valid` true), and no false-positive long-duration flags all confirmed against notebook
  output. Silver has been pushed to git.
- Column renames worth remembering: `descriptor→description`, `open_data_channel_type→intake_channel`,
  `unique_key→complaint_id` (cast to `long`/bigint, not int).
- Consciously dropped: `street_name`, `police_precinct`, `latitude`/`longitude` (all geo excluded
  from silver v1, recoverable from bronze), `community_board`, and everything not in `KEEP_COLUMNS`.

### Gold / weather — ⏳ not started

Separate module `src/nyc311/aggregations.py` with 4 independent sibling builders (not a chain):
`build_agency_performance`, `build_borough_metrics`, `build_channel_trends`,
`build_weather_complaints(silver_df, weather_df)`. `src/nyc311/weather.py` holds the Open-Meteo
client. Naming signal: `transformations.py` = verbs on one thing; `aggregations.py` = `build_*`
constructors.

## Business requirements (drive every design decision)

| ID | Requirement | Drives |
|---|---|---|
| BR-1 | Agency response performance — median + p90 resolution time per agency & complaint type, monthly. Open complaints tracked as separate aging metric, never dropped. | `response_time_hours`, `gold_agency_performance` |
| BR-2 | Borough equity — comparable resolution times per borough; missing/'Unspecified' borough counted and reported, not dropped. | borough normalization, `gold_borough_metrics` |
| BR-3 | Heating season readiness — daily HEAT/HOT WATER volume joined with temperature, ≥1 full winter. | Open-Meteo join, backfill window, `gold_daily_weather_complaints` |
| BR-4 | Channel shift — volume by `intake_channel` over time. | channel normalization, `gold_channel_trends` |
| BR-5 | Data trust — zero duplicate `complaint_id`, resolution times bounded (≥0, ≤5yr), visible freshness timestamp. Pipeline fails loudly, counts logged. | `quality.py`, dedup, `_duration_valid` |
| BR-6 | Self-service — gold usable by a SQL-literate analyst with no raw-data knowledge. | naming, column comments, README data dictionary |

## Current status & next actions

- ✅ Phase 0 setup, ✅ Phase 1 ingestion, 🔄 Phase 2 bronze (Auto Loader rewrite + verify cells
  written, not yet run on Databricks), ✅ Phase 3 silver (complete, run on real data — 519,870
  rows — and pushed to git).
- ⏳ Phases 4–6 (gold, quality, orchestration, presentation) not started.

**Active task — run & verify bronze.** The Auto Loader rewrite and both verification cells are
written in `02_bronze.py` but not yet run on Databricks. Next session: run `02_bronze`, then
(a) confirm the schema-diff cell prints nothing unexpected — add any surprises to `BRONZE_COLUMNS`;
(b) confirm the count-reconciliation cell shows bronze ≥ ingestion total;
(c) rerun `02_bronze` and confirm zero new rows (checkpoint idempotency).
Then re-run `03_silver` end to end.

**Also unblocked:** **tests** — pytest + local SparkSession, 3-row hand-built DataFrames per
function contract; **gold** — `weather.py`, `aggregations.py` 4 builders, `04_gold` notebook,
move `start_date` back for a full winter and re-backfill.

Smaller outstanding items:
- Bronze: run & verify per "Active task" above.
- Reconcile `derive_metrics` with the gated version recorded in chat handoff #2 (null
  `response_time_hours` when `_duration_valid` is false), or decide the flag-only design stands.
- Dedicated before/after row count around the `deduplicate` step specifically (break the
  `.transform()` chain to measure — it drops rows, unlike the flag-only `validate_bounds`).
- `requirements.txt` and `README.md` are still stubs; README owes architecture diagram, the
  data-quality findings table, a column→BR data dictionary, and the decision log.

## Known data quality issues (from the real bronze sample)

Everything arrives as strings · multiple internal spaces (`34 NORTH    6 STREET`) · nested JSON as
string (`location`) · composite fields (`community_board` = `"01 BROOKLYN"`) · fake nulls
(`"Unspecified"`, `"N/A"`) · sparse context columns (`bridge_highway_*`, `taxi_*`) · garbage
coordinates · duplicates from at-least-once ingestion · negative/extreme durations · inconsistent
casing (`BROOKLYN` vs `Brooklyn`). Handling: cast in silver, regexp whitespace collapse, sentinel→null
map, dedup on `complaint_id`, null+flag+count bad durations, upper/initcap normalize.
