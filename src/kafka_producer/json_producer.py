import logging
from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from src.entity.csv_reader import read_csv_records
from src.entity.schema_manager import generate_json_schema
from src.entity.sensor_record import record_to_dict
from src.kafka_config import sasl_conf, schema_config

logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    """Reports the success or failure of a message delivery."""
    if err is not None:
        logger.error("Delivery failed for record %s: %s", msg.key(), err)
        return
    logger.info("Record %s produced to %s [%s] at offset %s",
                msg.key(), msg.topic(), msg.partition(), msg.offset())


def produce_data_from_file(
    topic: str,
    file_path: str,
    producer: Producer | None = None,
    schema_registry_client: SchemaRegistryClient | None = None,
):
    """Read a CSV file and produce each row as a JSON message to Kafka.

    Args:
        topic: Kafka topic to produce to.
        file_path: Path to the source CSV file.
        producer: Optional pre-configured Kafka Producer (for testing/DI).
        schema_registry_client: Optional Schema Registry client (for testing/DI).
    """
    schema_str = generate_json_schema(file_path=file_path)

    if schema_registry_client is None:
        schema_registry_client = SchemaRegistryClient(schema_config())
    if producer is None:
        producer_conf = sasl_conf()
        producer_conf["enable.idempotence"] = True  # Exactly-once per partition
        producer = Producer(producer_conf)

    string_serializer = StringSerializer("utf_8")
    json_serializer = JSONSerializer(schema_str, schema_registry_client, record_to_dict)

    logger.info("Producing records to topic %s from %s", topic, file_path)
    produced = 0
    try:
        for record in read_csv_records(file_path=file_path):
            producer.poll(0.0)
            producer.produce(
                topic=topic,
                key=string_serializer(str(uuid4()), SerializationContext(topic, MessageField.KEY)),
                value=json_serializer(record, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=delivery_report,
            )
            produced += 1
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user after %d records", produced)
    except BufferError as e:
        logger.error("Producer buffer full after %d records: %s", produced, e)
        raise
    finally:
        logger.info("Flushing %d remaining records...", len(producer))
        producer.flush()
        logger.info("Producer finished. Total records produced: %d", produced)
