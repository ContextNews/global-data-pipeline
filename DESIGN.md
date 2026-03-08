# Global Data Pipeline — Design Proposal

## 1. Goal

Build a Python pipeline that:

1. **Discovers** all available indicators/series from World Bank, IMF, and UN sources
2. **Extracts** the full history of every indicator for every country
3. **Transforms** raw API responses into clean, standardised Parquet datasets
4. **Publishes** one Hugging Face dataset repo per source
5. **Supports incremental updates** — only fetches data that changed since the last run
6. *(Phase 3)* Loads data into Neon PostgreSQL

---

## 2. Data Sources

| Source | API | Auth | Python Library | Rate Limits | Estimated Scale |
|--------|-----|------|----------------|-------------|-----------------|
| **World Bank** (WDI + 45 DBs) | REST JSON — `api.worldbank.org/v2/` | None | `wbgapi` | None published | ~1 400 indicators × 265 countries × 60 yrs |
| **IMF** (IFS, BOP, WEO, DOT, PCPS…) | Legacy JSON — `dataservices.imf.org` | None | `imfp` | **10 req / 5 s**, 3 000 series/call | ~100 datasets, millions of series |
| **UN COMTRADE** (trade flows) | REST JSON — `comtradeapi.un.org` | Subscription key (free) | `comtradeapicall` | 1 req/s, 10 000 req/hr | Billions of rows (trade matrix) |
| **UN SDG** | REST JSON — `unstats.un.org/SDGAPI/v1/sdg/` | None | raw `httpx` | None published | ~17 goals, 231 indicators |
| **UNdata** (general stats) | SDMX — `data.un.org/ws/rest/` | None | `sdmx1` | None published | Dozens of databases |

### Scope: all indicators, all countries, all history

No filtering. Each source extracts **everything available** via its API. Indicator discovery happens dynamically at runtime (query the API for the full indicator catalogue, then iterate).

### Phase 1 sources
- World Bank WDI (+ other WB databases accessible via `wbgapi`)
- IMF (IFS, BOP, WEO, DOT, PCPS, and all other datasets exposed by `imfp`)
- UN SDG

### Phase 2 sources
- UN COMTRADE (requires subscription key, trade matrix is enormous)
- UNdata / UNSD (SDMX complexity)

---

## 3. Volume & Performance Implications

Pulling **all** indicators changes the game compared to a curated subset:

| Source | Approx rows (all indicators, all history) | Raw size estimate |
|--------|-------------------------------------------|-------------------|
| World Bank | ~20–50 M | ~2–5 GB Parquet |
| IMF (all datasets) | ~50–200 M | ~5–20 GB Parquet |
| UN SDG | ~2–5 M | ~200 MB Parquet |

### Strategies for handling scale

- **Chunked extraction** — process one indicator (WB) or one dataset (IMF) at a time; write incremental Parquet files. Never hold the full dataset in memory.
- **Per-source partitioning** — Parquet files partitioned by dataset/indicator group for efficient partial reads.
- **IMF rate limiting** — 10 requests per 5 seconds is the binding constraint. `imfp` handles this, but a full IMF pull will take hours. Progress must be resumable.
- **Checkpointing** — after each indicator/dataset chunk completes, persist state so a crashed run can resume without re-fetching.

---

## 4. Project Structure

```
global-data-pipeline/
├── pyproject.toml              # Poetry project: metadata, deps, CLI entry points
├── poetry.lock
├── LICENSE
├── DESIGN.md
│
├── src/
│   └── global_data_pipeline/
│       ├── __init__.py
│       ├── config.py           # pydantic-settings: env vars, paths, secrets
│       ├── cli.py              # Typer CLI: `gdp extract`, `gdp publish`, `gdp run`
│       │
│       ├── sources/
│       │   ├── __init__.py
│       │   ├── base.py         # Source protocol / ABC
│       │   ├── world_bank.py
│       │   ├── imf.py
│       │   └── un_sdg.py
│       │
│       ├── transform.py        # Cross-source normalisation (ISO codes, dtypes, schema)
│       │
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── local.py        # Read/write Parquet to local datasets/ dir
│       │   └── state.py        # Incremental update state (last run timestamps, checksums)
│       │
│       ├── publish/
│       │   ├── __init__.py
│       │   ├── huggingface.py  # Push Parquet + dataset card to HF Hub
│       │   └── database.py     # Phase 3: Neon PostgreSQL loader
│       │
│       └── logging.py          # Logging config
│
├── data/                       # Local data root (gitignored)
│   ├── raw/                    # Raw API responses / intermediate cache
│   ├── datasets/               # Built Parquet files, partitioned by source
│   └── state/                  # Incremental update state files (JSON)
│
├── tests/
│   ├── conftest.py
│   ├── test_world_bank.py
│   ├── test_imf.py
│   ├── test_un_sdg.py
│   ├── test_transform.py
│   └── test_publish.py
│
├── .env.example
└── .gitignore
```

### Changes from previous draft

- **`storage/` package** replaces the flat `datasets.py` registry. With "all indicators" there's no static registry — discovery is dynamic. Instead, `storage/` handles local Parquet I/O and incremental state tracking.
- **`data/` directory** with `raw/`, `datasets/`, and `state/` subdirs — separates concerns for caching, output, and pipeline state.
- **Poetry** manages the project (`pyproject.toml` + `poetry.lock`).

---

## 5. Source Interface

```python
from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd

class Source(ABC):
    name: str                       # e.g. "world_bank"

    @abstractmethod
    def discover(self) -> list[str]:
        """Return all available indicator/dataset IDs from the API."""
        ...

    @abstractmethod
    def extract(self, indicator_id: str, last_updated: str | None = None) -> pd.DataFrame:
        """Extract data for one indicator. If last_updated is provided,
        only fetch data newer than that timestamp (incremental)."""
        ...

    @abstractmethod
    def transform(self, raw: pd.DataFrame) -> pd.DataFrame:
        """Source-specific cleaning → standardised schema."""
        ...
```

Key change: `discover()` queries the API for the full catalogue. The pipeline iterates over every discovered indicator, calling `extract()` + `transform()` for each, and writing Parquet incrementally.

### Standardised output schema

| Column | Type | Description |
|--------|------|-------------|
| `country_code` | `str` (ISO 3166-1 alpha-3) | `GBR` |
| `country_name` | `str` | `United Kingdom` |
| `indicator_code` | `str` | `NY.GDP.MKTP.CD` |
| `indicator_name` | `str` | `GDP (current US$)` |
| `date` | `str` | `2023` or `2023-Q1` or `2023-01` |
| `frequency` | `str` | `annual`, `quarterly`, `monthly` |
| `value` | `float` (nullable) | `3.07e12` |
| `source` | `str` | `world_bank` |
| `dataset` | `str` | `WDI`, `IFS`, `sdg` |
| `unit` | `str` (nullable) | `current USD` |
| `last_updated` | `str` (ISO 8601) | `2024-06-15T00:00:00Z` |

Note: `date` is `str` not `int` because IMF data includes quarterly and monthly series.

---

## 6. Incremental Update Strategy

Each run must only fetch what changed. Strategy:

### State file (`data/state/{source_name}.json`)

```json
{
  "source": "world_bank",
  "last_run": "2025-02-10T12:00:00Z",
  "indicators": {
    "NY.GDP.MKTP.CD": {
      "last_updated": "2024-12-01",
      "row_count": 15320,
      "checksum": "sha256:abc123..."
    },
    "SP.POP.TOTL": { ... }
  }
}
```

### How it works per source

**World Bank:**
- The WB API returns a `lastUpdated` field per indicator. On incremental runs, compare against stored timestamp — skip indicators that haven't changed.
- For indicators that changed, re-fetch the full indicator (small enough) and overwrite its Parquet partition.

**IMF:**
- IMF doesn't provide per-series update timestamps reliably. Strategy: store a checksum of the extracted data per dataset. On re-run, fetch metadata and compare. If the dataset's structure changed, re-extract.
- Alternatively: always re-extract (IMF datasets are relatively small per-dataset), compare output to existing Parquet, and only write if different.

**UN SDG:**
- The SDG API provides a `timeCoverage` and series metadata. Compare against stored state.
- Fallback: same checksum approach as IMF.

### First run

On the first run (no state file), everything is extracted. This will be a long-running operation, especially for IMF. The checkpointing ensures that if the process crashes mid-run, completed indicators are not re-fetched.

---

## 7. Publishing to Hugging Face

**One HF repo per source** (details like namespace TBD):

```
{namespace}/world-bank-indicators
{namespace}/imf-data
{namespace}/un-sdg-indicators
```

### Publishing workflow

```python
from datasets import Dataset

def publish(source_name: str, data_dir: Path, repo_id: str):
    # Load all Parquet files for this source
    df = pd.read_parquet(data_dir / source_name)
    ds = Dataset.from_pandas(df)
    ds.push_to_hub(repo_id)
```

For large datasets (IMF, WB), consider pushing as **sharded Parquet** using `huggingface_hub.upload_folder()` directly rather than `push_to_hub()`, to avoid loading everything into memory.

### Dataset card (auto-generated)

Each repo gets a `README.md` with:
- Source description and attribution
- License / terms of use
- Column schema
- Row count, country count, indicator count, date range
- Last updated timestamp
- How to load: `load_dataset("{namespace}/world-bank-indicators")`

HF details (token, namespace) will be configured later. For now, the pipeline builds datasets locally.

---

## 8. Configuration & Secrets

**pydantic-settings** loading from `.env`:

```bash
# .env.example

# Hugging Face (required for publishing, not for extraction)
HF_TOKEN=hf_xxxx

# UN COMTRADE (Phase 2)
COMTRADE_SUBSCRIPTION_KEY=xxxx

# Neon PostgreSQL (Phase 3)
DATABASE_URL=postgresql://user:pass@ep-xxxx.region.aws.neon.tech/dbname?sslmode=require

# Paths (optional, defaults shown)
DATA_DIR=./data
```

---

## 9. CLI

**Typer** CLI with entry point `gdp`:

```bash
# Discover available indicators (dry run)
gdp discover world_bank
gdp discover imf
gdp discover all

# Extract + transform + save locally
gdp extract world_bank          # full or incremental (auto-detected)
gdp extract imf
gdp extract all
gdp extract world_bank --full   # force full rebuild (ignore state)

# Show pipeline status
gdp status                      # last run times, indicator counts, data sizes

# Publish to Hugging Face
gdp publish world_bank
gdp publish all

# Full pipeline (extract → publish)
gdp run
gdp run --source world_bank
gdp run --no-publish            # extract only
```

Entry point in `pyproject.toml` (Poetry):
```toml
[tool.poetry.scripts]
gdp = "global_data_pipeline.cli:app"
```

---

## 10. Dependencies

Managed by **Poetry** (`pyproject.toml`):

```toml
[tool.poetry.dependencies]
python = "^3.11"

# Core
pandas = "^2.0"
pyarrow = "*"
pydantic-settings = "*"
typer = {extras = ["all"], version = "*"}
httpx = "*"

# Data sources
wbgapi = "*"                    # World Bank
imfp = "*"                      # IMF

# Publishing
datasets = "*"                  # HF datasets
huggingface_hub = "*"           # HF Hub API

[tool.poetry.group.phase2.dependencies]
comtradeapicall = "*"           # UN COMTRADE
sdmx1 = "*"                    # UNdata

[tool.poetry.group.phase3.dependencies]
sqlalchemy = "*"                # DB ORM
alembic = "*"                   # Migrations
psycopg2-binary = "*"          # PostgreSQL driver

[tool.poetry.group.dev.dependencies]
pytest = "*"
ruff = "*"
```

Phase 2/3 dependencies in optional groups — not installed unless explicitly requested (`poetry install --with phase2`).

---

## 11. Remaining Open Questions

Most questions are now resolved. A few remain:

| # | Question | Notes |
|---|----------|-------|
| 1 | **Python version** — 3.11 or 3.12+? | Defaulting to `^3.11` in the proposal. |
| 2 | **Logging library** — stdlib `logging`, `loguru`, or `structlog`? | Long-running extractions need good logging. `structlog` gives structured JSON logs useful for debugging; `loguru` is simpler. |
| 3 | **HF namespace** | Deferred — you'll provide later. |
| 4 | **PyPI publishing** — is this an internal tool, or should we set up publishing? | Affects packaging choices. |

---

## 12. Resolved Decisions

| Decision | Choice |
|----------|--------|
| Indicator scope | **All indicators** — dynamic discovery from each API |
| Time range | **All available history** — no cutoff |
| Country scope | **All countries** |
| HF repo strategy | **One repo per source** |
| Update cadence | **Manual** (CLI-triggered) |
| Incremental updates | **Yes** — state tracking from Phase 1 |
| Package manager | **Poetry** |
| Target database | **Neon PostgreSQL** (Phase 3) |

---

## 13. Phased Roadmap

### Phase 1 — Full extraction pipeline

- [ ] Project scaffolding (Poetry, src layout, CLI skeleton)
- [ ] Source protocol / ABC with `discover()`, `extract()`, `transform()`
- [ ] World Bank source — discover all indicators, extract all data
- [ ] IMF source — discover all datasets/series, extract all data
- [ ] UN SDG source — discover all goals/indicators, extract all data
- [ ] Standardised output schema + cross-source transform
- [ ] Local Parquet storage (partitioned by source)
- [ ] Incremental update state tracking + checkpointing
- [ ] CLI (`gdp discover`, `gdp extract`, `gdp status`, `gdp run`)
- [ ] HF publishing (push_to_hub + auto-generated dataset cards)
- [ ] Tests
- [ ] `.env.example`, `.gitignore`

### Phase 2 — Expand sources

- [ ] UN COMTRADE source (subscription key, async/bulk download for volume)
- [ ] UNdata / UNSD source (SDMX via `sdmx1`)
- [ ] Cross-source deduplication (same indicator from WB + IMF)

### Phase 3 — Database

- [ ] SQLAlchemy models matching the standardised schema
- [ ] Alembic migrations
- [ ] Neon PostgreSQL loader (`gdp load` CLI command)
- [ ] Upsert logic for incremental DB updates
