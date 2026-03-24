# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

An ML data pipeline that publishes CSV sensor data to Confluent Kafka (JSON-serialized) and consumes it into MongoDB. Uses Confluent Schema Registry for JSON schema validation.

## Setup & Commands

```bash
cp .env.example .env               # fill in your credentials
pip install -r requirements.txt     # installs deps + package in editable mode (-e .)
python producer_main.py             # publish sample CSV data to Kafka topics
python consumer_main.py             # consume from Kafka and insert into MongoDB
```

## Configuration

All config is managed via Pydantic `BaseSettings` in `src/kafka_config/__init__.py`. Variables are loaded from environment or a `.env` file. The app fails fast with a clear error if required vars are missing. See `.env.example` for the full list.

Access settings via `get_kafka_settings()`, `get_mongo_settings()`, `get_consumer_settings()` — these are lazy singletons.

## Architecture

**Data flow:** CSV files → `Generic` entity → Kafka Producer (JSON serialized via Schema Registry) → Kafka Topic → Kafka Consumer (JSON deserialized) → MongoDB

Key modules in `src/`:

- **`kafka_config/`** — Pydantic-validated config. `sasl_conf()` returns producer/consumer config dict; `schema_config()` returns Schema Registry config.
- **`entity/generic.py`** — `Generic` class: dynamic entity mapping CSV rows to objects. Generates JSON Schema (draft-07) on-the-fly from CSV headers. `instance_to_dict` is the serializer callback.
- **`kafka_producer/json_producer.py`** — `produce_data_from_file(topic, file_path)`: reads CSV via `Generic.get_object()`, serializes with `JSONSerializer`, produces to Kafka with `poll()` inside the loop.
- **`kafka_consumer/json_consumer.py`** — `consume_topic(topic, file_path, collection_name)`: deserializes from Kafka, batch-inserts into MongoDB. Flushes remaining records on shutdown. Checks `msg.error()`.
- **`database/mongodb.py`** — `MongodbOperation` class wrapping pymongo. Supports context manager protocol. DB name and URL are constructor params.
- **`kafka_logger/`** — File-based logging to `logs/` with timestamped filenames.
- **`constant/`** — `SAMPLE_DIR` points to `sample_data/`.

**Entry points** (`producer_main.py`, `consumer_main.py`) iterate over subdirectories in `sample_data/` — each subdirectory name is used as the Kafka topic name.

## Key Design Decisions

- Schema is generated dynamically at runtime from CSV column headers (all fields typed as strings).
- Consumer batch size is configurable via `CONSUMER_BATCH_SIZE` env var (default 5000).
- MongoDB collection name defaults to the Kafka topic name.
- Package is installable via `setup.py` as `kafka` version `0.0.3`.
