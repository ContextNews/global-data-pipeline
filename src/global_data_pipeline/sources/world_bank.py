"""World Bank data source using wbgapi."""

import pandas as pd
import wbgapi as wb

from global_data_pipeline.logging import get_logger
from global_data_pipeline.sources.base import IndicatorInfo, Source
from global_data_pipeline.transform import drop_empty_values, enforce_schema

log = get_logger("sources.world_bank")


class WorldBankSource(Source):
    name = "world_bank"

    def __init__(self) -> None:
        self._country_names: dict[str, str] = {}

    def _load_country_names(self) -> None:
        if self._country_names:
            return
        for econ in wb.economy.list():
            self._country_names[econ["id"]] = econ["value"]

    def discover(self) -> list[IndicatorInfo]:
        indicators = [
            IndicatorInfo(code=s["id"], name=s["value"], source_dataset="WDI")
            for s in wb.series.list()
        ]
        log.info("Discovered %d World Bank indicators", len(indicators))
        return indicators

    def extract_indicator(self, indicator: IndicatorInfo) -> pd.DataFrame:
        try:
            df = wb.data.DataFrame(indicator.code, economy="all")
            if df.empty:
                return pd.DataFrame()
            return df.reset_index()
        except Exception as e:
            log.warning("Failed to extract %s: %s", indicator.code, e)
            return pd.DataFrame()

    def transform(self, raw: pd.DataFrame, indicator: IndicatorInfo) -> pd.DataFrame:
        self._load_country_names()

        # wbgapi returns wide format: economy index + year columns ('YR2020', 'YR2019', ...)
        long = raw.melt(id_vars="economy", var_name="date", value_name="value")
        long = long.rename(columns={"economy": "country_code"})

        # Strip 'YR' prefix from year column names
        long["date"] = long["date"].astype(str).str.replace(r"^YR", "", regex=True)

        long["country_name"] = long["country_code"].map(self._country_names)
        long["indicator_code"] = indicator.code
        long["indicator_name"] = indicator.name
        long["source"] = self.name
        long["dataset"] = indicator.source_dataset
        long["frequency"] = "annual"
        long["unit"] = pd.NA
        long["last_updated"] = pd.NA

        return drop_empty_values(enforce_schema(long))
