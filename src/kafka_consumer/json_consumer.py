import logging

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer

from src.entity.generic import Generic
from src.kafka_config import sasl_conf, get_consumer_settings, get_mongo_settings
from src.database.mongodb import MongodbOperation

logger = logging.getLogger(__name__)


def consume_topic(topic: str, file_path: str, collection_name: str | None = None):
    """Consume messages from a Kafka topic and insert into MongoDB.

    Args:
        topic: Kafka topic to consume from.
        file_path: CSV file path used to derive the JSON schema.
        collection_name: MongoDB collection name. Defaults to the topic name.
    """
    consumer_settings = get_consumer_settings()
    mongo_settings = get_mongo_settings()
    collection_name = collection_name or topic

    schema_str = Generic.get_schema_to_produce_consume_data(file_path=file_path)
    json_deserializer = JSONDeserializer(schema_str, from_dict=Generic.dict_to_object)

    consumer_conf = sasl_conf()
    consumer_conf.update({
        "group.id": consumer_settings.consumer_group_id,
        "auto.offset.reset": consumer_settings.auto_offset_reset,
    })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    mongodb = MongodbOperation(
        db_url=mongo_settings.mongo_db_url,
        db_name=mongo_settings.mongo_db_name,
    )

    records = []
    record_count = 0
    batch_size = consumer_settings.consumer_batch_size

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            record: Generic = json_deserializer(
                msg.value(),
                SerializationContext(msg.topic(), MessageField.VALUE),
            )

            if record is not None:
                records.append(record.to_dict())
                record_count += 1

                if len(records) >= batch_size:
                    _flush_records(mongodb, collection_name, records)
                    records = []

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user after %d records", record_count)
    finally:
        # Flush any remaining records before shutdown
        if records:
            _flush_records(mongodb, collection_name, records)
        consumer.close()
        mongodb.close()
        logger.info("Consumer shut down cleanly. Total records processed: %d", record_count)


def _flush_records(mongodb: MongodbOperation, collection_name: str, records: list):
    """Insert a batch of records into MongoDB."""
    try:
        mongodb.insert_many(collection_name=collection_name, records=records)
        logger.info("Flushed %d records to MongoDB collection '%s'", len(records), collection_name)
    except Exception:
        logger.exception("Failed to insert %d records into '%s'", len(records), collection_name)
        raise
