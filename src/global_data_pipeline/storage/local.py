"""Local Parquet storage for built datasets."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from global_data_pipeline.logging import get_logger

log = get_logger("storage.local")


def source_dir(datasets_dir: Path, source_name: str) -> Path:
    d = datasets_dir / source_name
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_indicator(datasets_dir: Path, source_name: str, indicator_code: str, df: pd.DataFrame) -> Path:
    """Write a single indicator's data as a Parquet file.

    File: datasets/{source}/{indicator_code}.parquet
    """
    out_dir = source_dir(datasets_dir, source_name)
    # Sanitise indicator code for use as filename
    safe_name = indicator_code.replace("/", "_").replace("\\", "_").replace(" ", "_")
    path = out_dir / f"{safe_name}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, path, compression="snappy")
    log.debug("Wrote %d rows → %s", len(df), path)
    return path


def read_indicator(datasets_dir: Path, source_name: str, indicator_code: str) -> pd.DataFrame | None:
    """Read a single indicator's Parquet file, or None if it doesn't exist."""
    safe_name = indicator_code.replace("/", "_").replace("\\", "_").replace(" ", "_")
    path = source_dir(datasets_dir, source_name) / f"{safe_name}.parquet"
    if not path.exists():
        return None
    return pd.read_parquet(path)


def read_source(datasets_dir: Path, source_name: str) -> pd.DataFrame:
    """Read all Parquet files for a source into a single DataFrame."""
    d = source_dir(datasets_dir, source_name)
    files = sorted(d.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    tables = [pq.read_table(f) for f in files]
    combined = pa.concat_tables(tables)
    return combined.to_pandas()


def source_stats(datasets_dir: Path, source_name: str) -> dict:
    """Return summary stats for a source's local data."""
    d = datasets_dir / source_name
    if not d.exists():
        return {"files": 0, "total_rows": 0, "size_mb": 0.0}
    files = list(d.glob("*.parquet"))
    total_rows = 0
    total_bytes = 0
    for f in files:
        total_bytes += f.stat().st_size
        meta = pq.read_metadata(f)
        total_rows += meta.num_rows
    return {
        "files": len(files),
        "total_rows": total_rows,
        "size_mb": round(total_bytes / (1024 * 1024), 2),
    }
