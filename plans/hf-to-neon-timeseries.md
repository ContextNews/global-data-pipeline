# Plan: Load HuggingFace Time Series Data → Neon PostgreSQL

## Goal

Populate the four normalised tables (`ts_sources`, `ts_entities`, `ts_indicators`,
`ts_datapoints`) from the World Bank Parquet files already published to Hugging Face,
without re-downloading locally first.

---

## Existing context

| Concern | Current state |
|---|---|
| Flat schema | `country_code`, `country_name`, `indicator_code`, `indicator_name`, `date` (str), `frequency`, `value`, `source`, `dataset`, `unit`, `last_updated` |
| HF layout | One repo per source, one Parquet file per indicator code, snappy-compressed |
| Old DB loader | `publish/database.py` — loads from *local* Parquet into flat `global_indicators` table |
| New DB tables | `TSSource`, `TSEntity`, `TSIndicator`, `TSDatapoint` (normalised, FK-linked) |

---

## Mapping: flat schema → normalised tables

```
source          → TSSource.name
country_code    → TSEntity.id   (ISO 3166-1 alpha-3)
country_name    → TSEntity.name
indicator_code  → TSIndicator.id
indicator_name  → TSIndicator.name
unit            → TSIndicator.unit
frequency       → TSIndicator.frequency
date (str)      → TSDatapoint.date  ← requires parsing (see below)
value           → TSDatapoint.value
```

`TSEntity.entity_type` = `"country"` for all WB rows (no blocs/regions in WB data).
`TSEntity.id` will use the ISO alpha-3 code directly (no Wikidata QID lookup needed for now — can be enriched later).

---

## Date parsing strategy

WB `date` strings come in three formats. All must be coerced to `datetime.date`:

| Format | Example | Parse rule |
|---|---|---|
| Annual | `"2023"` | Jan 1 → `date(2023, 1, 1)` |
| Quarterly | `"2023Q3"` or `"2023-Q3"` | Start of quarter → `date(2023, 7, 1)` |
| Monthly | `"2023M07"` or `"2023-07"` | First of month → `date(2023, 7, 1)` |

Rows with unparseable dates are logged and skipped.

---

## Implementation plan

### Step 1 — Database migration

Create an Alembic migration (or a simple `CREATE TABLE IF NOT EXISTS` script) so the
four new tables exist in Neon before loading.

File: `src/global_data_pipeline/storage/db/models.py`
Already defined by the user (TSEntity, TSSource, TSIndicator, TSDatapoint).

Action: add `alembic/` scaffold (or a one-off `create_tables.py` script) that calls
`Base.metadata.create_all(engine)`.

### Step 2 — HF streaming loader (`publish/ts_database.py`)

New module with a single public function:

```python
def load_source_from_hf(source_name: str, hf_repo: str, database_url: str) -> None:
    ...
```

#### 2a. Connect & seed `ts_sources`

```python
engine = create_engine(database_url)
source_id = _upsert_source(conn, source_name)
```

Use `INSERT ... ON CONFLICT (name) DO NOTHING RETURNING id` (or SELECT after insert).

#### 2b. Iterate over Parquet files from HF

Use `huggingface_hub.list_repo_files(repo_id, repo_type="dataset")` to enumerate
`*.parquet` files. Then for each file:

```python
path = hf_hub_download(repo_id=hf_repo, filename=parquet_path, repo_type="dataset")
df = pd.read_parquet(path)
```

This downloads one file at a time, keeping memory bounded.
Alternative: use the `datasets` library with `streaming=True` if files are large.

#### 2c. Upsert `ts_entities` (batch per file)

```python
entities = df[["country_code", "country_name"]].drop_duplicates()
# INSERT INTO ts_entities (id, name, entity_type)
# VALUES (...) ON CONFLICT (id) DO NOTHING
```

#### 2d. Upsert `ts_indicators` (once per file — one indicator per Parquet file)

```python
# INSERT INTO ts_indicators (id, name, unit, frequency, source_id)
# VALUES (...) ON CONFLICT (id) DO NOTHING
```

Note: if the same indicator appears in multiple sources in future, this needs
`ON CONFLICT (id) DO UPDATE` to keep `source_id` correct.

#### 2e. Parse dates and upsert `ts_datapoints` in chunks

```python
df["pg_date"] = df["date"].map(_parse_date)   # returns datetime.date or None
df = df.dropna(subset=["pg_date", "value"])

# Chunk into 10k rows; for each chunk:
# INSERT INTO ts_datapoints (indicator_id, entity_id, date, value)
# VALUES (...) ON CONFLICT (indicator_id, entity_id, date) DO UPDATE SET value = EXCLUDED.value
```

Use `psycopg2.extras.execute_values` (via a raw connection) for bulk inserts — much
faster than SQLAlchemy `to_sql` for upserts with conflict handling.

### Step 3 — CLI command

Add `gdp load-ts` to `cli.py`:

```python
@app.command("load-ts")
def load_ts(
    source: str = typer.Argument(..., help=_SOURCE_HELP),
    hf_repo: str = typer.Option("", "--hf-repo", help="HuggingFace repo id (overrides default)."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Load time series data from HuggingFace into Neon PostgreSQL normalised tables."""
    ...
```

The default HF repo per source is derived the same way `publish/huggingface.py` does it
(e.g. `{hf_username}/global-data-{source_name}`). Pull the username from a new
`Settings.hf_username` field or infer it from the token.

### Step 4 — Progress & resumability

Since WB has ~1,600 indicators, a crash mid-run should not restart from scratch.
Reuse `PipelineState` — but keyed to `"ts_load_{source_name}"` so it doesn't collide
with extraction state. After each Parquet file is successfully loaded, record it.

Alternative simpler approach: because `ON CONFLICT DO UPDATE` is idempotent, a re-run
is safe without explicit checkpointing. Only add state tracking if load time is > 10
minutes.

---

## File checklist

```
src/global_data_pipeline/
  storage/
    db/
      __init__.py           NEW (or move models here)
      models.py             NEW — TSEntity, TSSource, TSIndicator, TSDatapoint
      base.py               NEW — declarative Base
  publish/
    ts_database.py          NEW — load_source_from_hf()
  cli.py                    EDIT — add load-ts command
  config.py                 EDIT — add hf_username field (optional)

plans/
  hf-to-neon-timeseries.md  (this file)
```

---

## Dependency notes

- `huggingface_hub` — already a dep (used by `publish/huggingface.py`)
- `psycopg2-binary` — already gated behind `--with phase3`
- `sqlalchemy` — already a dep
- `alembic` — already in phase3 group (per CLAUDE.md)

No new dependencies needed.

---

## Key risks & mitigations

| Risk | Mitigation |
|---|---|
| WB has ~1,600 files × N countries × ~60 years = tens of millions of rows | Stream one file at a time; chunk inserts at 10k rows |
| Date strings don't parse cleanly | Log and skip unparseable rows; surface count at end |
| `ts_entities` ID collision if same country code comes from multiple sources | `ON CONFLICT (id) DO NOTHING` is safe — ISO codes are stable |
| Neon connection pool exhaustion on parallel loads | Keep a single engine / connection pool; don't parallelise the DB writes |
| HF rate limits on file downloads | Sequential download per file is fine; HF limits are generous for authenticated users |

---

## GitHub Actions trigger

A manually-triggered workflow at `.github/workflows/load-timeseries.yml` runs
`gdp load-ts` inside a clean CI environment. Secrets (`DATABASE_URL`, `HF_TOKEN`)
are stored in GitHub repo settings — never on local machines.

Inputs exposed in the UI:
- **source** — dropdown: `world_bank | imf | un_sdg | all`
- **full** — boolean flag to force a full reload (bypasses checkpoint state)

`timeout-minutes: 360` guards against hung runs. A WB full load should complete well
within that. IMF (rate-limited to 1 req/s) may need more if run in full mode.

The workflow can also be added as a `schedule` trigger once the load is stable
(e.g. monthly after a publish run).

---

## Suggested implementation order

1. `storage/db/models.py` + `base.py` — define models, run `create_all` against Neon
2. `publish/ts_database.py` — write and unit-test `_parse_date()` and the upsert helpers
3. `cli.py` — wire up `load-ts` command
4. Smoke test: run against a single WB indicator Parquet file
5. Full WB run with progress logging
