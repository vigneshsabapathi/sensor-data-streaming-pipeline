"""Shared fixtures for the ml-data-pipeline test suite."""

import os
import textwrap

import pytest

SAMPLE_CSV_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    "sample_data",
    "kafka-sensor-topic",
    "aps_failure_training_set1.csv",
)


@pytest.fixture
def sample_csv_path():
    """Return the absolute path to the real sample CSV shipped with the repo."""
    path = os.path.normpath(SAMPLE_CSV_PATH)
    assert os.path.isfile(path), f"Sample CSV not found at {path}"
    return path


@pytest.fixture
def small_csv(tmp_path):
    """Create a tiny temporary CSV with known data for fast unit tests.

    Columns:
        id       - integer, no nulls
        value    - float with one 'na' (null)
        label    - string with no nulls
        empty_col - all 'na' (all null)
    """
    csv_content = textwrap.dedent("""\
        id,value,label,empty_col
        1,10.5,alpha,na
        2,na,beta,na
        3,30.0,gamma,na
        4,40.7,delta,na
    """)
    csv_file = tmp_path / "small_test.csv"
    csv_file.write_text(csv_content)
    return str(csv_file)


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set all required environment variables for Kafka/Mongo/Consumer config.

    Uses monkeypatch so variables are automatically cleaned up after each test.
    """
    env = {
        "API_KEY": "test-api-key",
        "API_SECRET_KEY": "test-api-secret",
        "BOOTSTRAP_SERVER": "localhost:9092",
        "SECURITY_PROTOCOL": "SASL_SSL",
        "SASL_MECHANISM": "PLAIN",
        "ENDPOINT_SCHEMA_URL": "http://localhost:8081",
        "SCHEMA_REGISTRY_API_KEY": "sr-api-key",
        "SCHEMA_REGISTRY_API_SECRET": "sr-api-secret",
        "MONGO_DB_URL": "mongodb://localhost:27017",
        "MONGO_DB_NAME": "test_db",
        "CONSUMER_GROUP_ID": "test-group",
        "CONSUMER_BATCH_SIZE": "100",
        "AUTO_OFFSET_RESET": "earliest",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return env
