"""CSV ingestion with type inference and null handling."""

import logging
from typing import Generator

import numpy as np
import pandas as pd

from src.entity.sensor_record import SensorRecord

logger = logging.getLogger(__name__)

# Values treated as missing/null in sensor CSV data
NA_VALUES = {"na", "NA", "Na", "nan", "NaN", "null", "NULL", ""}


def read_csv_records(
    file_path: str,
    chunksize: int = 500,
) -> Generator[SensorRecord, None, None]:
    """Yield SensorRecord instances from each row of a CSV file.

    Args:
        file_path: Path to the CSV file.
        chunksize: Number of rows per pandas chunk (higher = faster, more memory).
    """
    chunks = pd.read_csv(file_path, chunksize=chunksize, na_values=NA_VALUES)
    row_count = 0

    for df in chunks:
        for _, row in df.iterrows():
            record = {}
            for col, val in row.items():
                record[col] = _coerce_value(val)
            yield SensorRecord(record)
            row_count += 1

    logger.info("Read %d records from %s", row_count, file_path)


def get_csv_columns(file_path: str) -> list[str]:
    """Return column names from a CSV without reading the full file."""
    return list(pd.read_csv(file_path, nrows=0).columns)


def infer_column_types(file_path: str, sample_rows: int = 1000) -> dict[str, str]:
    """Infer JSON Schema types for each column by sampling data.

    Returns a dict mapping column name → JSON Schema type string.
    Possible types: "number", "string", or ["number", "null"] / ["string", "null"].
    """
    df = pd.read_csv(file_path, nrows=sample_rows, na_values=NA_VALUES)
    type_map = {}

    for col in df.columns:
        non_null = df[col].dropna()
        has_nulls = df[col].isna().any()

        if len(non_null) == 0:
            # All null — default to nullable string
            base_type = "string"
        elif _is_numeric_column(non_null):
            base_type = "number"
        else:
            base_type = "string"

        if has_nulls:
            type_map[col] = [base_type, "null"]
        else:
            type_map[col] = base_type

    return type_map


def _is_numeric_column(series: pd.Series) -> bool:
    """Check if a pandas Series contains numeric data."""
    if pd.api.types.is_numeric_dtype(series):
        return True
    # Try parsing string values as numbers
    try:
        pd.to_numeric(series, errors="raise")
        return True
    except (ValueError, TypeError):
        return False


def _coerce_value(val):
    """Convert a single cell value to an appropriate Python type."""
    if pd.isna(val):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    # Try numeric coercion for string values
    if isinstance(val, str):
        try:
            num = float(val)
            return int(num) if num == int(num) else num
        except (ValueError, OverflowError):
            return val
    return val
