"""Tests for CSV ingestion, type coercion, and column inference."""

import numpy as np
import pandas as pd
import pytest

from src.entity.csv_reader import (
    _coerce_value,
    get_csv_columns,
    infer_column_types,
    read_csv_records,
)
from src.entity.sensor_record import SensorRecord


class TestCoerceValue:
    """Test _coerce_value with various input types."""

    def test_coerce_nan_to_none(self):
        assert _coerce_value(float("nan")) is None

    def test_coerce_pandas_na_to_none(self):
        assert _coerce_value(pd.NA) is None

    def test_coerce_none_to_none(self):
        assert _coerce_value(None) is None

    def test_coerce_numpy_int(self):
        val = np.int64(42)
        result = _coerce_value(val)
        assert result == 42
        assert isinstance(result, int)

    def test_coerce_numpy_float(self):
        val = np.float64(3.14)
        result = _coerce_value(val)
        assert result == pytest.approx(3.14)
        assert isinstance(result, float)

    def test_coerce_string_integer(self):
        """String '123' should become int 123."""
        result = _coerce_value("123")
        assert result == 123
        assert isinstance(result, int)

    def test_coerce_string_float(self):
        """String '3.14' should become float 3.14."""
        result = _coerce_value("3.14")
        assert result == pytest.approx(3.14)
        assert isinstance(result, float)

    def test_coerce_non_numeric_string(self):
        """Non-numeric strings should stay as strings."""
        assert _coerce_value("hello") == "hello"
        assert isinstance(_coerce_value("hello"), str)

    def test_coerce_plain_int(self):
        result = _coerce_value(42)
        assert result == 42

    def test_coerce_plain_float(self):
        result = _coerce_value(3.14)
        assert result == pytest.approx(3.14)

    def test_coerce_empty_string_numeric_attempt(self):
        """Empty string will fail float() conversion, should remain string or be caught."""
        # Empty string causes ValueError in float(), so it stays as-is
        result = _coerce_value("")
        assert result == ""


class TestReadCsvRecords:
    """Test reading CSV files into SensorRecord generators."""

    def test_yields_sensor_records(self, small_csv):
        records = list(read_csv_records(small_csv, chunksize=10))

        assert len(records) == 4
        for r in records:
            assert isinstance(r, SensorRecord)

    def test_record_has_expected_fields(self, small_csv):
        records = list(read_csv_records(small_csv, chunksize=10))
        first = records[0]

        assert hasattr(first, "id")
        assert hasattr(first, "value")
        assert hasattr(first, "label")

    def test_null_handling_na_becomes_none(self, small_csv):
        """'na' values in CSV should be coerced to None."""
        records = list(read_csv_records(small_csv, chunksize=10))
        # Row index 1 has value=na
        second = records[1]
        assert second.value is None

    def test_all_null_column(self, small_csv):
        """Column where every value is 'na' should produce None for all rows."""
        records = list(read_csv_records(small_csv, chunksize=10))
        for r in records:
            assert r.empty_col is None

    def test_numeric_values_coerced(self, small_csv):
        records = list(read_csv_records(small_csv, chunksize=10))
        first = records[0]

        # id=1 should be int, value=10.5 should be float
        assert first.id == 1
        assert isinstance(first.id, (int, float))
        assert first.value == pytest.approx(10.5)

    @pytest.mark.integration
    def test_read_real_sample_csv(self, sample_csv_path):
        """Integration test: read a few records from the actual sample CSV."""
        records = []
        for i, rec in enumerate(read_csv_records(sample_csv_path, chunksize=100)):
            records.append(rec)
            if i >= 9:
                break

        assert len(records) == 10
        for r in records:
            assert isinstance(r, SensorRecord)
            d = r.to_dict()
            assert "class" in d or "aa_000" in d


class TestGetCsvColumns:
    """Test column name extraction."""

    def test_returns_column_names(self, small_csv):
        columns = get_csv_columns(small_csv)

        assert columns == ["id", "value", "label", "empty_col"]

    def test_returns_list(self, small_csv):
        columns = get_csv_columns(small_csv)
        assert isinstance(columns, list)

    @pytest.mark.integration
    def test_real_csv_columns(self, sample_csv_path):
        columns = get_csv_columns(sample_csv_path)

        assert len(columns) > 100
        assert "class" in columns
        assert "aa_000" in columns


class TestInferColumnTypes:
    """Test type inference for CSV columns."""

    def test_detects_numeric_column(self, small_csv):
        type_map = infer_column_types(small_csv)

        # 'id' column has all integers, no nulls
        assert type_map["id"] == "number"

    def test_detects_string_column(self, small_csv):
        type_map = infer_column_types(small_csv)

        # 'label' column has string values, no nulls
        assert type_map["label"] == "string"

    def test_detects_nullable_numeric(self, small_csv):
        type_map = infer_column_types(small_csv)

        # 'value' column has floats and one 'na' -> nullable number
        assert type_map["value"] == ["number", "null"]

    def test_all_null_column_defaults_to_nullable_string(self, small_csv):
        type_map = infer_column_types(small_csv)

        # 'empty_col' is entirely 'na' -> nullable string
        assert type_map["empty_col"] == ["string", "null"]

    def test_returns_dict(self, small_csv):
        type_map = infer_column_types(small_csv)
        assert isinstance(type_map, dict)
        assert len(type_map) == 4
