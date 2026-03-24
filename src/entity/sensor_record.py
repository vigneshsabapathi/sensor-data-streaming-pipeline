"""Data model for sensor records flowing through the pipeline."""


class SensorRecord:
    """Represents a single sensor data record.

    Attributes are set dynamically from key-value pairs, preserving
    the original column names from the source data.
    """

    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

    @staticmethod
    def from_dict(data: dict, ctx=None) -> "SensorRecord":
        """Kafka deserializer callback: dict → SensorRecord."""
        return SensorRecord(record=data)

    def to_dict(self) -> dict:
        return self.__dict__

    def __str__(self):
        return f"{self.__dict__}"

    def __repr__(self):
        fields = len(self.__dict__)
        return f"SensorRecord({fields} fields)"


def record_to_dict(instance: "SensorRecord", ctx) -> dict:
    """Kafka serializer callback: SensorRecord → dict."""
    return instance.to_dict()
