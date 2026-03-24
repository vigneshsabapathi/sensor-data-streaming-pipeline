import logging

from pydantic import Field
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class KafkaSettings(BaseSettings):
    api_key: str = Field(..., description="Confluent Kafka API key")
    api_secret_key: str = Field(..., description="Confluent Kafka API secret")
    bootstrap_server: str = Field(..., description="Kafka bootstrap server URL")
    security_protocol: str = Field(default="SASL_SSL")
    sasl_mechanism: str = Field(default="PLAIN")
    endpoint_schema_url: str = Field(..., description="Schema Registry URL")
    schema_registry_api_key: str = Field(..., description="Schema Registry API key")
    schema_registry_api_secret: str = Field(..., description="Schema Registry API secret")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


class MongoSettings(BaseSettings):
    mongo_db_url: str = Field(..., description="MongoDB connection string")
    mongo_db_name: str = Field(default="ineuron", description="MongoDB database name")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


class ConsumerSettings(BaseSettings):
    consumer_group_id: str = Field(default="sensor-pipeline-group")
    consumer_batch_size: int = Field(default=5000, ge=1)
    auto_offset_reset: str = Field(default="earliest")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


# Lazy singletons — instantiated on first call so tests can set env vars first
_kafka_settings = None
_mongo_settings = None
_consumer_settings = None


def get_kafka_settings() -> KafkaSettings:
    global _kafka_settings
    if _kafka_settings is None:
        _kafka_settings = KafkaSettings()
    return _kafka_settings


def get_mongo_settings() -> MongoSettings:
    global _mongo_settings
    if _mongo_settings is None:
        _mongo_settings = MongoSettings()
    return _mongo_settings


def get_consumer_settings() -> ConsumerSettings:
    global _consumer_settings
    if _consumer_settings is None:
        _consumer_settings = ConsumerSettings()
    return _consumer_settings


def sasl_conf() -> dict:
    settings = get_kafka_settings()
    conf = {
        "bootstrap.servers": settings.bootstrap_server,
        "security.protocol": settings.security_protocol,
    }
    # Only add SASL credentials when using SASL-based protocols
    if "SASL" in settings.security_protocol.upper():
        conf.update({
            "sasl.mechanism": settings.sasl_mechanism,
            "sasl.username": settings.api_key,
            "sasl.password": settings.api_secret_key,
        })
    return conf


def schema_config() -> dict:
    settings = get_kafka_settings()
    conf = {"url": settings.endpoint_schema_url}
    # Only add auth when credentials are real (not placeholder "local")
    if settings.schema_registry_api_key and settings.schema_registry_api_key != "local":
        conf["basic.auth.user.info"] = (
            f"{settings.schema_registry_api_key}:{settings.schema_registry_api_secret}"
        )
    return conf
