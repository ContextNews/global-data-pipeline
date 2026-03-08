from unittest.mock import patch

import pandas as pd
import pytest

from global_data_pipeline.sources.base import IndicatorInfo
from global_data_pipeline.sources.world_bank import WorldBankSource
from global_data_pipeline.transform import STANDARD_COLUMNS


@pytest.fixture()
def source():
    return WorldBankSource()


@pytest.fixture()
def indicator():
    return IndicatorInfo(code="NY.GDP.MKTP.CD", name="GDP (current US$)", source_dataset="WDI")


def _raw_wide(countries=("USA", "GBR"), years=("YR2020", "YR2019"), values=None):
    """Build a wide-format DataFrame as wbgapi returns after reset_index()."""
    data = {"economy": list(countries)}
    default = [21.0e12, 2.7e12]
    for i, yr in enumerate(years):
        data[yr] = values[i] if values else default
    return pd.DataFrame(data)


def test_transform_produces_standard_schema(source, indicator):
    raw = _raw_wide()
    source._country_names = {"USA": "United States", "GBR": "United Kingdom"}
    with patch.object(source, "_load_country_names"):
        result = source.transform(raw, indicator)
    assert list(result.columns) == STANDARD_COLUMNS


def test_transform_correct_row_count(source, indicator):
    raw = _raw_wide(countries=("USA", "GBR"), years=("YR2020", "YR2019"))
    source._country_names = {}
    with patch.object(source, "_load_country_names"):
        result = source.transform(raw, indicator)
    # 2 countries × 2 years = 4 rows (all non-null)
    assert len(result) == 4


def test_transform_strips_yr_prefix(source, indicator):
    raw = _raw_wide(years=("YR2020",))
    source._country_names = {}
    with patch.object(source, "_load_country_names"):
        result = source.transform(raw, indicator)
    assert "2020" in result["date"].values


def test_transform_fills_metadata(source, indicator):
    raw = _raw_wide(years=("YR2020",))
    source._country_names = {}
    with patch.object(source, "_load_country_names"):
        result = source.transform(raw, indicator)
    assert (result["source"] == "world_bank").all()
    assert (result["dataset"] == "WDI").all()
    assert (result["indicator_code"] == "NY.GDP.MKTP.CD").all()
    assert (result["frequency"] == "annual").all()


def test_transform_drops_null_values(source, indicator):
    raw = pd.DataFrame({"economy": ["USA", "GBR"], "YR2020": [21.0e12, None]})
    source._country_names = {}
    with patch.object(source, "_load_country_names"):
        result = source.transform(raw, indicator)
    assert len(result) == 1
    assert result.iloc[0]["country_code"] == "USA"


def test_extract_returns_empty_on_api_error(source, indicator):
    with patch("wbgapi.data.DataFrame", side_effect=Exception("API error")):
        result = source.extract_indicator(indicator)
    assert result.empty


def test_discover_returns_indicator_info():
    mock_series = [{"id": "NY.GDP.MKTP.CD", "value": "GDP"}, {"id": "SP.POP.TOTL", "value": "Pop"}]
    mock_economies = []
    with patch("wbgapi.series.list", return_value=mock_series), patch(
        "wbgapi.economy.list", return_value=mock_economies
    ):
        src = WorldBankSource()
        indicators = src.discover()
    assert len(indicators) == 2
    assert indicators[0].code == "NY.GDP.MKTP.CD"
    assert indicators[0].source_dataset == "WDI"
