# Global Data Pipeline

Extracts macroeconomic and development timeseries from the World Bank, IMF, and UN — transforms them into a standardised schema, and publishes them as open datasets on Hugging Face.

## Published datasets

| Source | Hugging Face |
|--------|-------------|
| World Bank (WDI + all databases) | [ContextNews/world-bank-indicators](https://huggingface.co/datasets/ContextNews/world-bank-indicators) |
| IMF (IFS, BOP, WEO, DOT, PCPS, …) | [ContextNews/imf-data](https://huggingface.co/datasets/ContextNews/imf-data) |
| UN SDG | [ContextNews/un-sdg-indicators](https://huggingface.co/datasets/ContextNews/un-sdg-indicators) |

## Data schema

All sources are normalised to the same Parquet schema:

| Column | Type | Example |
|--------|------|---------|
| `country_code` | string | `GBR` |
| `country_name` | string | `United Kingdom` |
| `indicator_code` | string | `NY.GDP.MKTP.CD` |
| `indicator_name` | string | `GDP (current US$)` |
| `date` | string | `2023`, `2023Q1`, `2023-01` |
| `frequency` | string | `annual`, `quarterly`, `monthly` |
| `value` | Float64 (nullable) | `3.07e12` |
| `source` | string | `world_bank` |
| `dataset` | string | `WDI`, `IFS`, `sdg_1` |
| `unit` | string (nullable) | `current USD` |
| `last_updated` | string (nullable) | `2024-06-15T00:00:00Z` |

> **Note:** UN SDG `country_code` uses UN M49 numeric codes rather than ISO 3166-1 alpha-3.

## Setup

```bash
# Install (requires Poetry)
poetry install

# Copy and fill in secrets
cp .env.example .env
# Set HF_TOKEN for publishing; all other vars are optional
```

## CLI

```bash
# Discover available indicators (no data written)
gdp discover world_bank
gdp discover all

# Extract, transform, and save locally (auto-detects incremental vs full)
gdp extract world_bank
gdp extract all
gdp extract world_bank --full   # force rebuild, ignore saved state

# Show pipeline status
gdp status

# Publish local data to Hugging Face
gdp publish world_bank
gdp publish all

# Full pipeline (extract → publish)
gdp run
gdp run --source world_bank
gdp run --no-publish            # extract only

# Load into Neon PostgreSQL (Phase 3)
gdp load world_bank
gdp load all
```

## Data sources

| Source | API | Rate limit | Scale |
|--------|-----|------------|-------|
| World Bank | `api.worldbank.org/v2/` | None | ~1 400 indicators × 265 countries |
| IMF | `dataservices.imf.org` | 10 req / 5 s | ~100 datasets |
| UN SDG | `unstats.un.org/SDGAPI/v1/` | None | 231 indicators |

Full history for all indicators and all countries is extracted on first run. Subsequent runs are incremental — only changed indicators are re-fetched.

## Local data layout

```
data/
  raw/                          # intermediate API cache
  datasets/{source}/            # one Parquet file per indicator
  state/{source}.json           # incremental state (checksums, timestamps)
```

## Development

```bash
poetry install
poetry run pytest
poetry run ruff check src/ tests/
```

## Phased roadmap

- **Phase 1** (complete): World Bank, IMF, UN SDG → Parquet → Hugging Face
- **Phase 2**: UN COMTRADE, UNdata/UNSD (`poetry install --with phase2`)
- **Phase 3**: Neon PostgreSQL loader (`poetry install --with phase3`)

## License

MIT
