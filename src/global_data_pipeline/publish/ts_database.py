"""Load time series data from Hugging Face into Neon PostgreSQL normalised tables.

Downloads one Parquet file at a time (one per indicator) from HF, then upserts
into ts_sources / ts_entities / ts_indicators / ts_datapoints.

Requires phase3 deps: sqlalchemy, psycopg2-binary.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from huggingface_hub import HfApi, hf_hub_download
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text

from global_data_pipeline.logging import get_logger
from global_data_pipeline.storage.state import PipelineState

log = get_logger("publish.ts_database")

_REPO_IDS: dict[str, str] = {
    "world_bank": "ContextNews/world-bank-indicators",
    "imf": "ContextNews/imf-data",
    "un_sdg": "ContextNews/un-sdg-indicators",
}

_CHUNK_SIZE = 10_000

# Quarter number → start month
_QUARTER_START = {1: 1, 2: 4, 3: 7, 4: 10}


def load_source_from_hf(
    source_name: str,
    database_url: str,
    state_dir: Path,
    *,
    full: bool = False,
    hf_token: str | None = None,
    hf_repo: str | None = None,
) -> None:
    """Stream each indicator Parquet file from HF and upsert into Neon."""
    repo_id = hf_repo or _REPO_IDS.get(source_name)
    if repo_id is None:
        raise ValueError(f"No HF repo configured for source '{source_name}'")

    engine = create_engine(database_url)

    # Ensure tables exist
    from global_data_pipeline.storage.db import models as _models  # noqa: F401, PLC0415
    from global_data_pipeline.storage.db.base import Base  # noqa: PLC0415

    Base.metadata.create_all(engine)

    state = PipelineState(state_dir, f"ts_load_{source_name}")
    source_id = _upsert_source(engine, source_name)
    log.info("Source '%s' → id=%d", source_name, source_id)

    api = HfApi(token=hf_token)
    all_files = [
        f
        for f in api.list_repo_files(repo_id, repo_type="dataset")
        if f.endswith(".parquet")
    ]
    log.info("Found %d Parquet files in %s", len(all_files), repo_id)

    loaded = skipped = failed = 0
    total = len(all_files)

    for i, filename in enumerate(all_files, 1):
        if not full and state.get_indicator(filename) is not None:
            skipped += 1
            log.debug("[%d/%d] %s — already loaded, skipping", i, total, filename)
            continue

        try:
            local_path = hf_hub_download(
                repo_id=repo_id,
                filename=filename,
                repo_type="dataset",
                token=hf_token,
            )
            df = pd.read_parquet(local_path)
        except Exception as e:
            log.error("[%d/%d] %s — download failed: %s", i, total, filename, e)
            failed += 1
            continue

        if df.empty:
            log.debug("[%d/%d] %s — empty, skipping", i, total, filename)
            skipped += 1
            continue

        try:
            n_rows = _upsert_file(engine, df, source_id)
            state.update_indicator(filename, row_count=n_rows)
            loaded += 1
            log.info("[%d/%d] %s — %d rows", i, total, filename, n_rows)
        except Exception as e:
            log.error("[%d/%d] %s — upsert failed: %s", i, total, filename, e)
            failed += 1

        if i % 10 == 0:
            state.save()

    state.save()
    log.info("Done — loaded: %d, skipped: %d, failed: %d", loaded, skipped, failed)


def _upsert_source(engine, source_name: str) -> int:
    """Insert source if not exists, return its id."""
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO ts_sources (name) VALUES (:name) ON CONFLICT (name) DO NOTHING"),
            {"name": source_name},
        )
        row = conn.execute(
            text("SELECT id FROM ts_sources WHERE name = :name"),
            {"name": source_name},
        ).one()
    return row.id


def _upsert_file(engine, df: pd.DataFrame, source_id: int) -> int:
    """Upsert entities, indicator, and datapoints for one Parquet file."""
    df = df.copy()
    df["pg_date"] = df["date"].map(_parse_date)
    valid = df.dropna(subset=["pg_date", "value"])

    skipped_rows = len(df) - len(valid)
    if skipped_rows:
        log.debug("Skipped %d rows with unparseable dates or null values", skipped_rows)

    entities = df[["country_code", "country_name"]].drop_duplicates()
    entity_tuples = list(zip(
        entities["country_code"],
        entities["country_name"],
        ["country"] * len(entities),
    ))

    indicator_id = str(df["indicator_code"].iloc[0])
    indicator_name = str(df["indicator_name"].iloc[0])
    unit = _str_or_none(df["unit"].iloc[0]) if "unit" in df.columns else None
    frequency = _str_or_none(df["frequency"].iloc[0]) if "frequency" in df.columns else None

    datapoint_tuples = list(zip(
        [indicator_id] * len(valid),
        valid["country_code"],
        valid["pg_date"],
        valid["value"].astype(float),
    ))

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO ts_entities (id, name, entity_type) VALUES %s"
                " ON CONFLICT (id) DO NOTHING",
                entity_tuples,
            )
            cur.execute(
                """
                INSERT INTO ts_indicators (id, name, unit, frequency, source_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
                """,
                (indicator_id, indicator_name, unit, frequency, source_id),
            )
            for i in range(0, len(datapoint_tuples), _CHUNK_SIZE):
                execute_values(
                    cur,
                    """
                    INSERT INTO ts_datapoints (indicator_id, entity_id, date, value)
                    VALUES %s
                    ON CONFLICT (indicator_id, entity_id, date)
                    DO UPDATE SET value = EXCLUDED.value
                    """,
                    datapoint_tuples[i : i + _CHUNK_SIZE],
                )
        raw_conn.commit()
    except Exception:
        raw_conn.rollback()
        raise
    finally:
        raw_conn.close()

    return len(datapoint_tuples)


def _parse_date(date_str) -> date | None:
    """Coerce WB/IMF/UN date strings to datetime.date.

    Handles:
      "2023"       → date(2023, 1, 1)
      "2023Q3"     → date(2023, 7, 1)
      "2023-Q3"    → date(2023, 7, 1)
      "2023M07"    → date(2023, 7, 1)
      "2023-07"    → date(2023, 7, 1)
      "2023-01-01" → date(2023, 1, 1)
    """
    if pd.isna(date_str) or not isinstance(date_str, str):
        return None
    s = date_str.strip()

    if re.fullmatch(r"\d{4}", s):
        return date(int(s), 1, 1)

    m = re.fullmatch(r"(\d{4})-?Q(\d)", s)
    if m:
        year, q = int(m.group(1)), int(m.group(2))
        return date(year, _QUARTER_START[q], 1)

    m = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)

    m = re.fullmatch(r"(\d{4})M(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)

    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        pass

    log.debug("Could not parse date string: %r", s)
    return None


def _str_or_none(val) -> str | None:
    return str(val) if pd.notna(val) else None
