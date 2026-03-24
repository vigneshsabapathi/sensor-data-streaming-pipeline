"""Kafka consumer with DLQ, retry, manual offset commits, and graceful shutdown.

Delivery guarantee: at-least-once. Offsets are committed only after a
successful MongoDB batch insert. If the consumer crashes mid-batch,
messages will be re-delivered on restart. MongoDB writes are not
idempotent — duplicates are possible on crash recovery.
"""

import logging
import signal
import threading

from confluent_kafka import Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from pymongo.errors import PyMongoError

from src.database.mongodb import MongodbOperation
from src.entity.schema_manager import generate_json_schema
from src.entity.sensor_record import SensorRecord
from src.kafka_config import get_consumer_settings, get_mongo_settings, sasl_conf
from src.kafka_producer.dlq_producer import DLQProducer
from src.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class GracefulShutdown:
    """Handles SIGTERM/SIGINT for clean consumer shutdown."""

    def __init__(self):
        self._shutdown = threading.Event()
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("Received %s — initiating graceful shutdown", sig_name)
        self._shutdown.set()

    @property
    def should_stop(self) -> bool:
        return self._shutdown.is_set()


def consume_topic(
    topic: str,
    file_path: str,
    collection_name: str | None = None,
    consumer: Consumer | None = None,
    mongodb: MongodbOperation | None = None,
    dlq: DLQProducer | None = None,
):
    """Consume messages from a Kafka topic and insert into MongoDB.

    Features:
        - Manual offset commits after successful DB writes (at-least-once)
        - Dead Letter Queue for messages that fail deserialization
        - Retry with exponential backoff for transient MongoDB failures
        - Graceful shutdown on SIGTERM/SIGINT with final flush

    Args:
        topic: Kafka topic to consume from.
        file_path: CSV file path used to derive the JSON schema.
        collection_name: MongoDB collection name. Defaults to the topic name.
        consumer: Optional pre-configured Kafka Consumer (for testing/DI).
        mongodb: Optional pre-configured MongoDB client (for testing/DI).
        dlq: Optional DLQ producer (for testing/DI).
    """
    consumer_settings = get_consumer_settings()
    collection_name = collection_name or topic
    shutdown = GracefulShutdown()

    schema_str = generate_json_schema(file_path=file_path)
    json_deserializer = JSONDeserializer(schema_str, from_dict=SensorRecord.from_dict)

    owns_consumer = consumer is None
    owns_mongodb = mongodb is None
    owns_dlq = dlq is None

    if consumer is None:
        consumer_conf = sasl_conf()
        consumer_conf.update({
            "group.id": consumer_settings.consumer_group_id,
            "auto.offset.reset": consumer_settings.auto_offset_reset,
            "enable.auto.commit": False,  # Manual commits for at-least-once
        })
        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic])

    if mongodb is None:
        mongo_settings = get_mongo_settings()
        mongodb = MongodbOperation(
            db_url=mongo_settings.mongo_db_url,
            db_name=mongo_settings.mongo_db_name,
        )

    if dlq is None:
        dlq = DLQProducer()

    records = []
    record_count = 0
    dlq_count = 0
    batch_size = consumer_settings.consumer_batch_size

    logger.info(
        "Starting consumer: topic=%s, group=%s, batch_size=%d",
        topic, consumer_settings.consumer_group_id, batch_size,
    )

    try:
        while not shutdown.should_stop:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            # Deserialize with DLQ fallback
            try:
                record: SensorRecord = json_deserializer(
                    msg.value(),
                    SerializationContext(msg.topic(), MessageField.VALUE),
                )
            except Exception as e:
                dlq.send_to_dlq(
                    original_topic=topic,
                    original_value=msg.value(),
                    original_key=msg.key(),
                    error=e,
                    error_stage="deserialization",
                )
                dlq_count += 1
                continue

            if record is not None:
                records.append(record.to_dict())
                record_count += 1

                if len(records) >= batch_size:
                    _flush_and_commit(mongodb, consumer, dlq, collection_name, topic, records)
                    records = []

    except KeyboardInterrupt:
        logger.info("Consumer interrupted after %d records", record_count)
    finally:
        # Final flush of remaining records
        if records:
            _flush_and_commit(mongodb, consumer, dlq, collection_name, topic, records)

        if owns_dlq:
            dlq.flush()
        if owns_consumer:
            consumer.close()
        if owns_mongodb:
            mongodb.close()

        logger.info(
            "Consumer shut down. processed=%d, sent_to_dlq=%d",
            record_count, dlq_count,
        )


@retry_with_backoff(max_retries=3, base_delay=1.0, exceptions=(PyMongoError,))
def _insert_batch(mongodb: MongodbOperation, collection_name: str, records: list):
    """Insert a batch into MongoDB with retry on transient errors."""
    mongodb.insert_many(collection_name=collection_name, records=records)


def _flush_and_commit(
    mongodb: MongodbOperation,
    consumer: Consumer,
    dlq: DLQProducer,
    collection_name: str,
    topic: str,
    records: list,
):
    """Insert records into MongoDB, then commit offsets. On failure, send batch to DLQ."""
    try:
        _insert_batch(mongodb, collection_name, records)
        consumer.commit(asynchronous=False)
        logger.info("Flushed %d records and committed offsets", len(records))
    except Exception as e:
        logger.error("Batch insert failed after retries: %s. Sending %d records to DLQ.", e, len(records))
        for record in records:
            dlq.send_to_dlq(
                original_topic=topic,
                original_value=str(record).encode("utf-8"),
                original_key=None,
                error=e,
                error_stage="db_insert",
            )
        # Commit anyway so we don't re-process records already sent to DLQ
        consumer.commit(asynchronous=False)
