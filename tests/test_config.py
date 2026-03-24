"""Tests for Kafka, Mongo, and Consumer configuration classes."""

import pytest
from pydantic import ValidationError

import src.kafka_config as kafka_config
from src.kafka_config import (
    ConsumerSettings,
    KafkaSettings,
    MongoSettings,
    sasl_conf,
    schema_config,
)


@pytest.fixture(autouse=True)
def reset_singletons():
    """Reset the lazy singletons before and after each test so cached
    instances from other tests (or from .env loading) do not leak."""
    kafka_config._kafka_settings = None
    kafka_config._mongo_settings = None
    kafka_config._consumer_settings = None
    yield
    kafka_config._kafka_settings = None
    kafka_config._mongo_settings = None
    kafka_config._consumer_settings = None


class TestKafkaSettings:
    """Test KafkaSettings validation."""

    def test_missing_required_fields_raises_validation_error(self, monkeypatch):
        """KafkaSettings should fail when required env vars are absent."""
        # Clear any env vars that could satisfy the required fields
        for var in [
            "API_KEY", "API_SECRET_KEY", "BOOTSTRAP_SERVER",
            "ENDPOINT_SCHEMA_URL", "SCHEMA_REGISTRY_API_KEY",
            "SCHEMA_REGISTRY_API_SECRET",
        ]:
            monkeypatch.delenv(var, raising=False)

        with pytest.raises(ValidationError):
            KafkaSettings(_env_file=None)

    def test_valid_kafka_settings(self, mock_env_vars):
        settings = KafkaSettings(_env_file=None)

        assert settings.api_key == "test-api-key"
        assert settings.api_secret_key == "test-api-secret"
        assert settings.bootstrap_server == "localhost:9092"
        assert settings.security_protocol == "SASL_SSL"
        assert settings.sasl_mechanism == "PLAIN"
        assert settings.endpoint_schema_url == "http://localhost:8081"
        assert settings.schema_registry_api_key == "sr-api-key"
        assert settings.schema_registry_api_secret == "sr-api-secret"

    def test_defaults_for_optional_fields(self, mock_env_vars):
        settings = KafkaSettings(_env_file=None)

        assert settings.security_protocol == "SASL_SSL"
        assert settings.sasl_mechanism == "PLAIN"


class TestMongoSettings:
    """Test MongoSettings defaults and validation."""

    def test_missing_url_raises_validation_error(self, monkeypatch):
        monkeypatch.delenv("MONGO_DB_URL", raising=False)
        monkeypatch.delenv("MONGO_DB_NAME", raising=False)

        with pytest.raises(ValidationError):
            MongoSettings(_env_file=None)

    def test_default_db_name(self, monkeypatch):
        monkeypatch.setenv("MONGO_DB_URL", "mongodb://localhost:27017")
        # Do NOT set MONGO_DB_NAME to test the default
        monkeypatch.delenv("MONGO_DB_NAME", raising=False)

        settings = MongoSettings(_env_file=None)
        assert settings.mongo_db_name == "ineuron"

    def test_valid_mongo_settings(self, mock_env_vars):
        settings = MongoSettings(_env_file=None)

        assert settings.mongo_db_url == "mongodb://localhost:27017"
        assert settings.mongo_db_name == "test_db"


class TestConsumerSettings:
    """Test ConsumerSettings defaults."""

    def test_defaults_without_env_vars(self, monkeypatch):
        """All ConsumerSettings fields have defaults, so it should work
        even with no env vars set."""
        monkeypatch.delenv("CONSUMER_GROUP_ID", raising=False)
        monkeypatch.delenv("CONSUMER_BATCH_SIZE", raising=False)
        monkeypatch.delenv("AUTO_OFFSET_RESET", raising=False)

        settings = ConsumerSettings(_env_file=None)

        assert settings.consumer_group_id == "sensor-pipeline-group"
        assert settings.consumer_batch_size == 5000
        assert settings.auto_offset_reset == "earliest"

    def test_custom_consumer_settings(self, mock_env_vars):
        settings = ConsumerSettings(_env_file=None)

        assert settings.consumer_group_id == "test-group"
        assert settings.consumer_batch_size == 100
        assert settings.auto_offset_reset == "earliest"


class TestSaslConf:
    """Test sasl_conf() returns correct dict structure."""

    def test_sasl_conf_keys(self, mock_env_vars):
        conf = sasl_conf()

        expected_keys = {
            "sasl.mechanism",
            "bootstrap.servers",
            "security.protocol",
            "sasl.username",
            "sasl.password",
        }
        assert set(conf.keys()) == expected_keys

    def test_sasl_conf_values(self, mock_env_vars):
        conf = sasl_conf()

        assert conf["sasl.mechanism"] == "PLAIN"
        assert conf["bootstrap.servers"] == "localhost:9092"
        assert conf["security.protocol"] == "SASL_SSL"
        assert conf["sasl.username"] == "test-api-key"
        assert conf["sasl.password"] == "test-api-secret"


class TestSchemaConfig:
    """Test schema_config() returns correct dict structure."""

    def test_schema_config_keys(self, mock_env_vars):
        conf = schema_config()

        expected_keys = {"url", "basic.auth.user.info"}
        assert set(conf.keys()) == expected_keys

    def test_schema_config_values(self, mock_env_vars):
        conf = schema_config()

        assert conf["url"] == "http://localhost:8081"
        assert conf["basic.auth.user.info"] == "sr-api-key:sr-api-secret"
