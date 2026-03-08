from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from global_data_pipeline.sources.base import IndicatorInfo
from global_data_pipeline.sources.un_sdg import UNSDGSource
from global_data_pipeline.transform import STANDARD_COLUMNS


@pytest.fixture()
def source():
    return UNSDGSource()


@pytest.fixture()
def indicator():
    return IndicatorInfo(
        code="SI_POV_DAY1",
        name="Proportion below international poverty line",
        source_dataset="sdg_1",
    )


def _raw_sdg(**kwargs):
    defaults = {
        "geoAreaCode": ["840", "826"],
        "geoAreaName": ["United States", "United Kingdom"],
        "timePeriodStart": [2020, 2020],
        "value": ["12.5", "8.3"],
        "units": ["%", "%"],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------


def test_transform_produces_standard_schema(source, indicator):
    result = source.transform(_raw_sdg(), indicator)
    assert list(result.columns) == STANDARD_COLUMNS


def test_transform_renames_columns(source, indicator):
    result = source.transform(_raw_sdg(), indicator)
    assert "840" in result["country_code"].values
    assert "United States" in result["country_name"].values
    assert "2020" in result["date"].values


def test_transform_fills_metadata(source, indicator):
    result = source.transform(_raw_sdg(), indicator)
    assert (result["source"] == "un_sdg").all()
    assert (result["indicator_code"] == "SI_POV_DAY1").all()
    assert (result["frequency"] == "annual").all()


def test_transform_converts_value_to_numeric(source, indicator):
    result = source.transform(_raw_sdg(), indicator)
    assert result["value"].dtype.name == "Float64"
    assert result.iloc[0]["value"] == pytest.approx(12.5)


def test_transform_drops_non_numeric_values(source, indicator):
    result = source.transform(_raw_sdg(value=["N/A", "8.3"]), indicator)
    assert len(result) == 1


def test_transform_returns_empty_without_date(source, indicator):
    raw = pd.DataFrame({"geoAreaCode": ["840"], "value": ["12.5"]})
    result = source.transform(raw, indicator)
    assert result.empty


def test_transform_handles_missing_units(source, indicator):
    raw = _raw_sdg()
    raw = raw.drop(columns=["units"])
    result = source.transform(raw, indicator)
    assert not result.empty
    assert result["unit"].isna().all()


# ---------------------------------------------------------------------------
# extract_indicator (mocked HTTP)
# ---------------------------------------------------------------------------


def test_extract_paginates_correctly(source, indicator):
    page1 = {"data": [{"x": 1}], "pageCount": 2, "currentPage": 1}
    page2 = {"data": [{"x": 2}], "pageCount": 2, "currentPage": 2}

    mock_resp1 = MagicMock()
    mock_resp1.json.return_value = page1
    mock_resp1.raise_for_status.return_value = None

    mock_resp2 = MagicMock()
    mock_resp2.json.return_value = page2
    mock_resp2.raise_for_status.return_value = None

    source._client = MagicMock()
    source._client.get.side_effect = [mock_resp1, mock_resp2]

    result = source.extract_indicator(indicator)
    assert len(result) == 2
    assert source._client.get.call_count == 2


def test_extract_returns_empty_on_http_error(source, indicator):
    source._client = MagicMock()
    source._client.get.side_effect = Exception("connection refused")
    result = source.extract_indicator(indicator)
    assert result.empty


# ---------------------------------------------------------------------------
# discover (mocked HTTP)
# ---------------------------------------------------------------------------


def test_discover_parses_series_list(source):
    mock_series = [
        {"code": "SI_POV_DAY1", "description": "Poverty line", "goal": ["1"]},
        {"code": "SH_DYN_IMRT", "description": "Infant mortality", "goal": ["3"]},
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = mock_series
    mock_resp.raise_for_status.return_value = None
    source._client = MagicMock()
    source._client.get.return_value = mock_resp

    indicators = source.discover()
    assert len(indicators) == 2
    assert indicators[0].code == "SI_POV_DAY1"
    assert indicators[0].source_dataset == "sdg_1"
    assert indicators[1].source_dataset == "sdg_3"
