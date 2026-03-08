from unittest.mock import patch

import pandas as pd
import pytest

from global_data_pipeline.sources.base import IndicatorInfo
from global_data_pipeline.sources.imf import IMFSource, _infer_frequency
from global_data_pipeline.transform import STANDARD_COLUMNS


@pytest.fixture()
def source():
    return IMFSource()


@pytest.fixture()
def indicator():
    return IndicatorInfo(code="IFS", name="International Financial Statistics", source_dataset="IFS")


def _raw_ifs(**kwargs):
    """Minimal imfp-style DataFrame for IFS."""
    defaults = {
        "ref_area": ["US", "GB"],
        "indicator": ["PCPI_IX", "PCPI_IX"],
        "freq": ["A", "A"],
        "date": ["2020", "2020"],
        "value": [102.5, 103.1],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


# ---------------------------------------------------------------------------
# _infer_frequency
# ---------------------------------------------------------------------------


def test_infer_frequency_annual():
    assert _infer_frequency("2020") == "annual"


def test_infer_frequency_quarterly():
    assert _infer_frequency("2020Q1") == "quarterly"


def test_infer_frequency_monthly():
    assert _infer_frequency("2020-01") == "monthly"


def test_infer_frequency_non_string():
    assert _infer_frequency(2020) == "annual"


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------


def test_transform_produces_standard_schema(source, indicator):
    result = source.transform(_raw_ifs(), indicator)
    assert list(result.columns) == STANDARD_COLUMNS


def test_transform_maps_ref_area_to_country_code(source, indicator):
    result = source.transform(_raw_ifs(), indicator)
    assert set(result["country_code"]) == {"US", "GB"}


def test_transform_maps_freq_to_frequency(source, indicator):
    raw = _raw_ifs(freq=["A", "Q"])
    result = source.transform(raw, indicator)
    assert set(result["frequency"]) == {"annual", "quarterly"}


def test_transform_fills_metadata(source, indicator):
    result = source.transform(_raw_ifs(), indicator)
    assert (result["source"] == "imf").all()
    assert (result["dataset"] == "IFS").all()


def test_transform_returns_empty_without_date(source, indicator):
    raw = pd.DataFrame({"ref_area": ["US"], "value": [1.0]})
    result = source.transform(raw, indicator)
    assert result.empty


def test_transform_returns_empty_without_value(source, indicator):
    raw = pd.DataFrame({"ref_area": ["US"], "date": ["2020"]})
    result = source.transform(raw, indicator)
    assert result.empty


def test_transform_drops_non_numeric_values(source, indicator):
    raw = _raw_ifs(value=["N/A", 103.1])
    result = source.transform(raw, indicator)
    assert len(result) == 1


def test_transform_handles_uppercase_columns(source, indicator):
    """imfp may return uppercase dimension codes."""
    raw = pd.DataFrame(
        {
            "REF_AREA": ["US"],
            "INDICATOR": ["PCPI_IX"],
            "date": ["2020"],
            "value": [102.5],
        }
    )
    result = source.transform(raw, indicator)
    assert not result.empty
    assert result.iloc[0]["country_code"] == "US"


def test_extract_returns_empty_on_error(source, indicator):
    with patch("imfp.imf_dataset", side_effect=Exception("timeout")):
        result = source.extract_indicator(indicator)
    assert result.empty


def test_discover_returns_one_per_database():
    mock_dbs = pd.DataFrame(
        {"database_id": ["IFS", "DOT"], "description": ["Intl Fin Stats", "Dir of Trade Stats"]}
    )
    with patch("imfp.imf_databases", return_value=mock_dbs):
        indicators = IMFSource().discover()
    assert len(indicators) == 2
    assert indicators[0].code == "IFS"
    assert indicators[0].source_dataset == "IFS"
