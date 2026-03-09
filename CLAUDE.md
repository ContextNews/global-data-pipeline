# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
poetry install

# Install with optional phase groups
poetry install --with phase2      # UN COMTRADE, UNdata
poetry install --with phase3      # SQLAlchemy, Alembic, psycopg2

# Run all tests
poetry run pytest

# Run a single test file
poetry run pytest tests/test_world_bank.py

# Run with coverage
poetry run pytest --cov=global_data_pipeline

# Lint and format
poetry run ruff check src/ tests/
poetry run ruff format src/ tests/

# CLI (after install)
gdp discover world_bank
gdp extract world_bank [--full] [--workers N]
gdp status
gdp publish world_bank
gdp load world_bank          # Phase 3: load into Neon PostgreSQL
gdp run [--source world_bank] [--no-publish]
```

Line length is 100. Ruff rules: `E, F, I, N, W, UP`.

## Architecture

The pipeline has three phases of development:

- **Phase 1** (current): World Bank, IMF, UN SDG → local Parquet → Hugging Face
- **Phase 2**: UN COMTRADE, UNdata (requires optional deps)
- **Phase 3**: Neon PostgreSQL loader

### Source abstraction (`sources/base.py`)

All data sources implement the `Source` ABC with three methods:

- `discover() -> list[IndicatorInfo]` — queries the API and returns every available indicator
- `extract_indicator(indicator: IndicatorInfo) -> pd.DataFrame` — fetches raw data for one indicator
- `transform(raw, indicator) -> pd.DataFrame` — cleans raw data into the standardised schema

`extract_and_transform()` is a convenience method that chains the two. Each source lives in `sources/{source_name}.py`.

To add a new source: implement `Source`, then register it in `sources/__init__.py`'s `ALL_SOURCES` dict and add a default worker count to `_DEFAULT_WORKERS` in `cli.py`.

### Standardised schema (`transform.py`)

Every source's `transform()` must produce a DataFrame with exactly these columns (enforced by `enforce_schema()`):

`country_code` (ISO 3166-1 alpha-3), `country_name`, `indicator_code`, `indicator_name`, `date` (str — annual/quarterly/monthly), `frequency`, `value` (Float64 nullable), `source`, `dataset`, `unit`, `last_updated`

Use `drop_empty_values()` to remove null-value rows before writing.

### Local storage (`storage/local.py`)

Each indicator is written as an individual Parquet file (snappy-compressed):

```
data/datasets/{source_name}/{indicator_code}.parquet
```

Key functions: `write_indicator()`, `read_indicator()`, `read_source()` (loads all into one DataFrame), `source_stats()`.

### Incremental state (`storage/state.py`)

`PipelineState` persists extraction state to `data/state/{source_name}.json`, tracking per-indicator `last_updated`, `row_count`, and `checksum`. The CLI uses `get_indicator(code) is None` to skip already-extracted indicators on resume; `should_skip(code, current_last_updated)` is for skipping based on upstream change detection. Call `save()` after each indicator completes so runs are resumable on crash (the CLI flushes every 10 completions).

### Configuration (`config.py`)

`Settings` (pydantic-settings) reads from `.env`. Key fields: `data_dir` (default `./data`), `hf_token`, `comtrade_subscription_key`, `database_url`. The `ensure_dirs()` method creates `raw/`, `datasets/`, and `state/` subdirs.

### Data directory layout

```
data/
  raw/           # intermediate API cache
  datasets/      # output Parquet, one subdir per source
  state/         # incremental state JSON files
```

All of `data/` is gitignored.

### Publishing (`publish/huggingface.py`)

One Hugging Face dataset repo per source. For large sources (IMF, WB), use `huggingface_hub.upload_folder()` directly rather than `push_to_hub()` to avoid loading everything into memory.

### Logging (`logging.py`)

Use `get_logger(name)` to obtain a logger. Call `setup_logging(verbose=False)` at the start of each CLI command to initialise the root logger.

### IMF rate limiting

The IMF API enforces 10 requests per 5 seconds. A full IMF extraction takes hours. Always design IMF extraction to be resumable via `PipelineState` checkpointing. The default worker count for IMF is 1 (`imfp` manages its own rate limiting); World Bank defaults to 8, UN SDG to 4.
