"""Schema generation and management for Kafka messages."""

import json
import logging

from src.entity.csv_reader import get_csv_columns, infer_column_types

logger = logging.getLogger(__name__)


def generate_json_schema(file_path: str, infer_types: bool = True) -> str:
    """Generate a JSON Schema (draft-07) string from a CSV file.

    Args:
        file_path: Path to the CSV file.
        infer_types: If True, sample data to infer numeric vs string types.
                     If False, all fields default to string (legacy behavior).

    Returns:
        JSON string representing the schema.
    """
    columns = get_csv_columns(file_path)

    if infer_types:
        type_map = infer_column_types(file_path)
    else:
        type_map = {col: "string" for col in columns}

    properties = {}
    for col in columns:
        col_type = type_map.get(col, "string")

        if isinstance(col_type, list):
            # Nullable field, e.g. ["number", "null"]
            properties[col] = {
                "description": f"Field {col}",
                "type": col_type,
            }
        else:
            properties[col] = {
                "description": f"Field {col}",
                "type": col_type,
            }

    schema = {
        "$id": "http://example.com/myURI.schema.json",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "additionalProperties": False,
        "description": "Auto-generated schema from CSV columns.",
        "properties": properties,
        "title": "SensorRecord",
        "type": "object",
    }

    schema_str = json.dumps(schema)
    logger.info("Generated schema with %d fields (type_inference=%s)", len(columns), infer_types)
    return schema_str
