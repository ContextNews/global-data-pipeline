"""Load local Parquet data into Neon PostgreSQL.

Uses a delete-then-insert strategy per source — appropriate for a batch
pipeline where each run rebuilds the source's data. Requires psycopg2-binary
(poetry install --with phase3).

Connection string format: postgresql://user:pass@ep-xxxx.region.aws.neon.tech/dbname?sslmode=require
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

import global_data_pipeline.storage.local as local_store
from global_data_pipeline.logging import get_logger

log = get_logger("publish.database")

_TABLE = "global_indicators"

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS global_indicators (
    country_code   TEXT    NOT NULL,
    country_name   TEXT,
    indicator_code TEXT    NOT NULL,
    indicator_name TEXT,
    date           TEXT    NOT NULL,
    frequency      TEXT,
    value          DOUBLE PRECISION,
    source         TEXT    NOT NULL,
    dataset        TEXT,
    unit           TEXT,
    last_updated   TEXT,
    PRIMARY KEY (country_code, indicator_code, date, source)
);
"""

_CHUNK_SIZE = 50_000


def load_source(source_name: str, datasets_dir: Path, database_url: str) -> None:
    """Upsert all Parquet data for a source into the Neon PostgreSQL table."""
    engine = create_engine(database_url)

    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE_SQL))

    df = local_store.read_source(datasets_dir, source_name)
    if df.empty:
        log.warning("No data found for '%s'", source_name)
        return

    # Cast nullable pandas types to plain Python types for SQLAlchemy compat
    df = _prepare_for_db(df)

    log.info("Loading %d rows for '%s' into '%s'", len(df), source_name, _TABLE)

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM global_indicators WHERE source = :source"), {"source": source_name})

    total = 0
    for i in range(0, len(df), _CHUNK_SIZE):
        chunk = df.iloc[i : i + _CHUNK_SIZE]
        chunk.to_sql(_TABLE, engine, if_exists="append", index=False, method="multi")
        total += len(chunk)
        log.debug("Inserted %d / %d rows", total, len(df))

    log.info("Loaded '%s' — %d rows", source_name, len(df))


def _prepare_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas extension types (StringDtype, Float64) to plain types."""
    df = df.copy()
    for col in df.select_dtypes(include="string").columns:
        df[col] = df[col].astype(object)
    if "value" in df.columns:
        df["value"] = df["value"].astype(float)
    return df
