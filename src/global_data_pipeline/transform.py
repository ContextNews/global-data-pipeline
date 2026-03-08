"""Cross-source normalisation and schema enforcement."""

import pandas as pd

STANDARD_COLUMNS = [
    "country_code",
    "country_name",
    "indicator_code",
    "indicator_name",
    "date",
    "frequency",
    "value",
    "source",
    "dataset",
    "unit",
    "last_updated",
]

STANDARD_DTYPES = {
    "country_code": "string",
    "country_name": "string",
    "indicator_code": "string",
    "indicator_name": "string",
    "date": "string",
    "frequency": "string",
    "value": "Float64",  # nullable float
    "source": "string",
    "dataset": "string",
    "unit": "string",
    "last_updated": "string",
}


def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DataFrame conforms to the standardised schema."""
    # Add missing columns with NaN
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Keep only standard columns, in order
    df = df[STANDARD_COLUMNS]

    # Enforce dtypes
    for col, dtype in STANDARD_DTYPES.items():
        df[col] = df[col].astype(dtype)

    return df


def drop_empty_values(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where value is null — no point storing them."""
    return df.dropna(subset=["value"]).reset_index(drop=True)
