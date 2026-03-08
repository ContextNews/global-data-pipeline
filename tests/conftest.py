import pandas as pd
import pytest

from global_data_pipeline.transform import enforce_schema


@pytest.fixture()
def tmp_data_dir(tmp_path):
    (tmp_path / "raw").mkdir()
    (tmp_path / "datasets").mkdir()
    (tmp_path / "state").mkdir()
    return tmp_path


@pytest.fixture()
def sample_df():
    """A minimal valid DataFrame conforming to the standard schema."""
    df = pd.DataFrame(
        {
            "country_code": ["USA", "GBR"],
            "country_name": ["United States", "United Kingdom"],
            "indicator_code": ["NY.GDP.MKTP.CD", "NY.GDP.MKTP.CD"],
            "indicator_name": ["GDP (current US$)", "GDP (current US$)"],
            "date": ["2020", "2020"],
            "frequency": ["annual", "annual"],
            "value": [21.0e12, 2.7e12],
            "source": ["world_bank", "world_bank"],
            "dataset": ["WDI", "WDI"],
            "unit": [pd.NA, pd.NA],
            "last_updated": [pd.NA, pd.NA],
        }
    )
    return enforce_schema(df)
