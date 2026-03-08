"""IMF data source using imfp.

Each IMF database is treated as one unit of work. imfp handles rate limiting
(10 req / 5 s). Full extractions take hours — PipelineState checkpointing
ensures runs are resumable.
"""

import pandas as pd
import imfp

from global_data_pipeline.logging import get_logger
from global_data_pipeline.sources.base import IndicatorInfo, Source
from global_data_pipeline.transform import drop_empty_values, enforce_schema

log = get_logger("sources.imf")

_FREQ_MAP = {"A": "annual", "Q": "quarterly", "M": "monthly", "B": "biannual"}


class IMFSource(Source):
    name = "imf"

    def discover(self) -> list[IndicatorInfo]:
        dbs = imfp.imf_databases()
        indicators = [
            IndicatorInfo(
                code=row["database_id"],
                name=row["description"],
                source_dataset=row["database_id"],
            )
            for _, row in dbs.iterrows()
        ]
        log.info("Discovered %d IMF databases", len(indicators))
        return indicators

    def extract_indicator(self, indicator: IndicatorInfo) -> pd.DataFrame:
        try:
            df = imfp.imf_dataset(indicator.code)
            return df
        except Exception as e:
            log.warning("Failed to extract IMF %s: %s", indicator.code, e)
            return pd.DataFrame()

    def transform(self, raw: pd.DataFrame, indicator: IndicatorInfo) -> pd.DataFrame:
        if raw.empty:
            return pd.DataFrame()

        # Normalise column names to lowercase for consistent mapping
        df = raw.copy()
        df.columns = df.columns.str.lower()

        # imfp returns dimension codes as columns; common patterns across databases:
        col_map = {
            "ref_area": "country_code",
            "country": "country_code",
            "indicator": "indicator_code",
            "series_code": "indicator_code",
            "unit_mult": "unit",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

        if "date" not in df.columns:
            log.warning("No date column in IMF %s (columns: %s)", indicator.code, list(df.columns))
            return pd.DataFrame()
        if "value" not in df.columns:
            log.warning("No value column in IMF %s (columns: %s)", indicator.code, list(df.columns))
            return pd.DataFrame()

        if "country_code" not in df.columns:
            df["country_code"] = pd.NA
        if "indicator_code" not in df.columns:
            df["indicator_code"] = indicator.code

        # Map FREQ dimension (A/Q/M) to standard frequency strings
        if "freq" in df.columns:
            df["frequency"] = df["freq"].map(_FREQ_MAP).fillna("annual")
        else:
            df["frequency"] = df["date"].map(_infer_frequency)

        df["indicator_name"] = indicator.name
        df["country_name"] = pd.NA
        df["source"] = self.name
        df["dataset"] = indicator.source_dataset
        if "unit" not in df.columns:
            df["unit"] = pd.NA
        df["last_updated"] = pd.NA

        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return drop_empty_values(enforce_schema(df))


def _infer_frequency(date_str: str) -> str:
    if not isinstance(date_str, str):
        return "annual"
    if "Q" in date_str:
        return "quarterly"
    if len(date_str) >= 7 and date_str[4] in ("-", "M"):
        return "monthly"
    return "annual"
