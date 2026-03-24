import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)


class Generic:

    def __init__(self, record: dict):
        for k, v in record.items():
            setattr(self, k, v)

    @staticmethod
    def dict_to_object(data: dict, ctx):
        return Generic(record=data)

    def to_dict(self):
        return self.__dict__

    @classmethod
    def get_object(cls, file_path: str):
        """Yield Generic instances from each row of a CSV file."""
        chunk_df = pd.read_csv(file_path, chunksize=10)
        for df in chunk_df:
            for data in df.values:
                yield Generic(dict(zip(df.columns, list(map(str, data)))))

    @classmethod
    def get_schema_to_produce_consume_data(cls, file_path: str) -> str:
        """Generate a JSON Schema (draft-07) string from CSV column headers."""
        columns = next(pd.read_csv(file_path, chunksize=10)).columns

        schema = {
            "$id": "http://example.com/myURI.schema.json",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "additionalProperties": False,
            "description": "Auto-generated schema from CSV columns.",
            "properties": {
                col: {"description": f"Field {col}", "type": "string"}
                for col in columns
            },
            "title": "SampleRecord",
            "type": "object",
        }
        return json.dumps(schema)

    def __str__(self):
        return f"{self.__dict__}"


def instance_to_dict(instance: Generic, ctx):
    return instance.to_dict()
