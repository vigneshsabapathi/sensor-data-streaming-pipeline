"""Tests for JSON schema generation from CSV files."""

import json

import pytest

from src.entity.schema_manager import generate_json_schema


class TestGenerateJsonSchema:
    """Test generate_json_schema produces valid, well-structured schemas."""

    def test_returns_valid_json(self, small_csv):
        schema_str = generate_json_schema(small_csv)
        schema = json.loads(schema_str)

        assert isinstance(schema, dict)

    def test_schema_draft07_structure(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv))

        assert schema["$schema"] == "http://json-schema.org/draft-07/schema#"
        assert schema["type"] == "object"
        assert "properties" in schema
        assert schema["title"] == "SensorRecord"
        assert schema["additionalProperties"] is False
        assert "$id" in schema

    def test_schema_has_all_columns(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv))
        props = schema["properties"]

        assert "id" in props
        assert "value" in props
        assert "label" in props
        assert "empty_col" in props

    def test_each_property_has_description(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv))
        props = schema["properties"]

        for col_name, col_def in props.items():
            assert "description" in col_def
            assert "type" in col_def


class TestSchemaTypeInference:
    """Test that type inference produces correct schema types."""

    def test_numeric_column_gets_number_type(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv, infer_types=True))
        props = schema["properties"]

        # 'id' is a non-nullable numeric column
        assert props["id"]["type"] == "number"

    def test_string_column_gets_string_type(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv, infer_types=True))
        props = schema["properties"]

        assert props["label"]["type"] == "string"

    def test_nullable_field_gets_type_null_array(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv, infer_types=True))
        props = schema["properties"]

        # 'value' has a mix of floats and 'na' -> ["number", "null"]
        assert isinstance(props["value"]["type"], list)
        assert "null" in props["value"]["type"]
        assert "number" in props["value"]["type"]

    def test_all_null_column_is_nullable_string(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv, infer_types=True))
        props = schema["properties"]

        assert isinstance(props["empty_col"]["type"], list)
        assert "string" in props["empty_col"]["type"]
        assert "null" in props["empty_col"]["type"]


class TestSchemaNoInference:
    """Test schema generation with infer_types=False (all strings)."""

    def test_all_fields_are_string(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv, infer_types=False))
        props = schema["properties"]

        for col_name, col_def in props.items():
            assert col_def["type"] == "string", (
                f"Column '{col_name}' should be 'string' when infer_types=False, "
                f"got '{col_def['type']}'"
            )

    def test_no_nullable_arrays_when_no_inference(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv, infer_types=False))
        props = schema["properties"]

        for col_name, col_def in props.items():
            assert isinstance(col_def["type"], str), (
                f"Column '{col_name}' type should be a plain string, not a list"
            )

    def test_schema_still_valid_structure(self, small_csv):
        schema = json.loads(generate_json_schema(small_csv, infer_types=False))

        assert schema["$schema"] == "http://json-schema.org/draft-07/schema#"
        assert schema["type"] == "object"
        assert len(schema["properties"]) == 4


class TestSchemaWithRealData:
    """Integration test with the real sample CSV."""

    @pytest.mark.integration
    def test_real_csv_schema(self, sample_csv_path):
        schema = json.loads(generate_json_schema(sample_csv_path, infer_types=True))

        assert schema["type"] == "object"
        assert len(schema["properties"]) > 100
        assert "class" in schema["properties"]
        assert "aa_000" in schema["properties"]
