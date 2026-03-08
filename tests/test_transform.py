import pandas as pd
import pytest

from global_data_pipeline.transform import (
    STANDARD_COLUMNS,
    drop_empty_values,
    enforce_schema,
)


def test_enforce_schema_adds_missing_columns():
    df = pd.DataFrame({"country_code": ["USA"], "value": [1.0]})
    result = enforce_schema(df)
    assert list(result.columns) == STANDARD_COLUMNS


def test_enforce_schema_column_order():
    df = pd.DataFrame({col: ["x"] for col in reversed(STANDARD_COLUMNS)})
    df["value"] = 1.0
    result = enforce_schema(df)
    assert list(result.columns) == STANDARD_COLUMNS


def test_enforce_schema_value_dtype():
    df = pd.DataFrame({col: ["test"] for col in STANDARD_COLUMNS})
    df["value"] = "3.14"
    result = enforce_schema(df)
    assert result["value"].dtype.name == "Float64"


def test_enforce_schema_string_dtype():
    df = pd.DataFrame({col: ["test"] for col in STANDARD_COLUMNS})
    df["value"] = 1.0
    result = enforce_schema(df)
    assert result["country_code"].dtype.name == "string"


def test_drop_empty_values_removes_null_rows():
    df = pd.DataFrame({"value": [1.0, None, 2.0], "country_code": ["A", "B", "C"]})
    result = drop_empty_values(df)
    assert len(result) == 2
    assert result["value"].notna().all()


def test_drop_empty_values_resets_index():
    df = pd.DataFrame({"value": [None, 1.0]})
    result = drop_empty_values(df)
    assert list(result.index) == [0]


def test_enforce_schema_drops_extra_columns():
    df = pd.DataFrame({col: ["x"] for col in STANDARD_COLUMNS})
    df["value"] = 1.0
    df["extra_column"] = "should_be_removed"
    result = enforce_schema(df)
    assert "extra_column" not in result.columns
