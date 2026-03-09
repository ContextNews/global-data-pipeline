
from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd


@dataclass
class IndicatorInfo:
    """Metadata about a discoverable indicator or dataset."""

    code: str
    name: str
    source_dataset: str  # e.g. "WDI", "IFS", "sdg_1"


class Source(ABC):
    """Base class for all data sources."""

    name: str

    @abstractmethod
    def discover(self) -> list[IndicatorInfo]:
        """Query the API and return all available indicators/datasets."""
        ...

    @abstractmethod
    def extract_indicator(self, indicator: IndicatorInfo) -> pd.DataFrame:
        """Extract all data for a single indicator. Returns a raw DataFrame."""
        ...

    @abstractmethod
    def transform(self, raw: pd.DataFrame, indicator: IndicatorInfo) -> pd.DataFrame:
        """Source-specific cleaning → standardised schema.

        Output columns:
            country_code, country_name, indicator_code, indicator_name,
            date, frequency, value, source, dataset, unit, last_updated
        """
        ...

    def extract_and_transform(self, indicator: IndicatorInfo) -> pd.DataFrame:
        """Convenience: extract then transform a single indicator."""
        raw = self.extract_indicator(indicator)
        if raw.empty:
            return raw
        return self.transform(raw, indicator)
