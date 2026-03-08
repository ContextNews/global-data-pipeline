"""UN SDG data source using raw httpx.

API base: https://unstats.un.org/SDGAPI/v1/sdg/
Note: geoAreaCode is UN M49 numeric, not ISO 3166-1 alpha-3.
"""

import httpx
import pandas as pd

from global_data_pipeline.logging import get_logger
from global_data_pipeline.sources.base import IndicatorInfo, Source
from global_data_pipeline.transform import drop_empty_values, enforce_schema

log = get_logger("sources.un_sdg")

_BASE_URL = "https://unstats.un.org/SDGAPI/v1/sdg"
_PAGE_SIZE = 1000


class UNSDGSource(Source):
    name = "un_sdg"

    def __init__(self) -> None:
        self._client = httpx.Client(timeout=60.0)

    def discover(self) -> list[IndicatorInfo]:
        resp = self._client.get(f"{_BASE_URL}/Series/List")
        resp.raise_for_status()
        series_list = resp.json()

        indicators = []
        for s in series_list:
            goals = s.get("goal", [])
            goal_str = goals[0] if isinstance(goals, list) and goals else "all"
            indicators.append(
                IndicatorInfo(
                    code=s["code"],
                    name=s.get("description", s["code"]),
                    source_dataset=f"sdg_{goal_str}",
                )
            )
        log.info("Discovered %d UN SDG series", len(indicators))
        return indicators

    def extract_indicator(self, indicator: IndicatorInfo) -> pd.DataFrame:
        rows: list[dict] = []
        page = 1
        while True:
            try:
                resp = self._client.get(
                    f"{_BASE_URL}/Series/Data",
                    params={"seriesCode": indicator.code, "page": page, "pageSize": _PAGE_SIZE},
                )
                resp.raise_for_status()
                body = resp.json()
                rows.extend(body.get("data", []))
                if page >= body.get("pageCount", 1):
                    break
                page += 1
            except Exception as e:
                log.warning("Failed page %d for %s: %s", page, indicator.code, e)
                break

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def transform(self, raw: pd.DataFrame, indicator: IndicatorInfo) -> pd.DataFrame:
        df = raw.copy()

        rename = {
            "geoAreaCode": "country_code",
            "geoAreaName": "country_name",
            "timePeriodStart": "date",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

        if "country_code" not in df.columns:
            df["country_code"] = pd.NA
        if "country_name" not in df.columns:
            df["country_name"] = pd.NA
        if "date" not in df.columns:
            log.warning("No date column in UN SDG %s", indicator.code)
            return pd.DataFrame()

        df["country_code"] = df["country_code"].astype(str)
        df["date"] = df["date"].astype(str)

        df["indicator_code"] = indicator.code
        df["indicator_name"] = indicator.name
        df["source"] = self.name
        df["dataset"] = indicator.source_dataset
        df["frequency"] = "annual"
        df["unit"] = df["units"] if "units" in df.columns else pd.NA
        df["last_updated"] = pd.NA

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
        else:
            df["value"] = pd.NA

        return drop_empty_values(enforce_schema(df))
