"""Tests for SensorRecord data model."""

from src.entity.sensor_record import SensorRecord, record_to_dict


class TestSensorRecordCreation:
    """Test constructing SensorRecord from dicts."""

    def test_creation_from_dict(self):
        data = {"sensor_a": 100, "sensor_b": 200, "label": "pos"}
        record = SensorRecord(data)

        assert record.sensor_a == 100
        assert record.sensor_b == 200
        assert record.label == "pos"

    def test_creation_sets_dynamic_attributes(self):
        data = {"x": 1, "y": 2}
        record = SensorRecord(data)

        assert hasattr(record, "x")
        assert hasattr(record, "y")

    def test_creation_empty_dict(self):
        record = SensorRecord({})
        assert record.to_dict() == {}


class TestSensorRecordToDict:
    """Test serialization back to dict."""

    def test_to_dict_roundtrip(self):
        original = {"sensor_a": 42, "sensor_b": 3.14, "label": "neg"}
        record = SensorRecord(original)
        result = record.to_dict()

        assert result == original

    def test_to_dict_preserves_types(self):
        data = {"int_val": 10, "float_val": 1.5, "str_val": "hello"}
        record = SensorRecord(data)
        result = record.to_dict()

        assert isinstance(result["int_val"], int)
        assert isinstance(result["float_val"], float)
        assert isinstance(result["str_val"], str)


class TestSensorRecordFromDict:
    """Test the static from_dict deserializer callback."""

    def test_from_dict_returns_sensor_record(self):
        data = {"a": 1, "b": 2}
        record = SensorRecord.from_dict(data)

        assert isinstance(record, SensorRecord)
        assert record.a == 1
        assert record.b == 2

    def test_from_dict_with_ctx_argument(self):
        """from_dict accepts an optional ctx parameter (Kafka deserializer protocol)."""
        data = {"x": 99}
        record = SensorRecord.from_dict(data, ctx="some_context")

        assert isinstance(record, SensorRecord)
        assert record.x == 99

    def test_from_dict_roundtrip(self):
        original = {"field1": "abc", "field2": 123}
        record = SensorRecord.from_dict(original)
        assert record.to_dict() == original


class TestRecordToDictCallback:
    """Test the module-level record_to_dict serializer callback."""

    def test_record_to_dict_returns_dict(self):
        data = {"sensor": 42, "status": "ok"}
        record = SensorRecord(data)
        result = record_to_dict(record, ctx=None)

        assert isinstance(result, dict)
        assert result == data

    def test_record_to_dict_with_ctx(self):
        """record_to_dict accepts a ctx parameter (Kafka serializer protocol)."""
        record = SensorRecord({"a": 1})
        result = record_to_dict(record, ctx="context_obj")

        assert result == {"a": 1}


class TestSensorRecordEdgeCases:
    """Test with None values and mixed types."""

    def test_with_none_values(self):
        data = {"present": 10, "missing": None, "also_missing": None}
        record = SensorRecord(data)

        assert record.present == 10
        assert record.missing is None
        assert record.also_missing is None
        assert record.to_dict() == data

    def test_with_mixed_types(self):
        data = {
            "int_field": 42,
            "float_field": 3.14,
            "str_field": "hello",
            "none_field": None,
            "bool_field": True,
            "list_field": [1, 2, 3],
        }
        record = SensorRecord(data)
        result = record.to_dict()

        assert result == data

    def test_str_representation(self):
        record = SensorRecord({"a": 1})
        assert "a" in str(record)
        assert "1" in str(record)

    def test_repr_shows_field_count(self):
        record = SensorRecord({"a": 1, "b": 2, "c": 3})
        assert "3 fields" in repr(record)
