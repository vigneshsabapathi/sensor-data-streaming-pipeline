from uuid import uuid4

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

from src.kafka_config import sasl_conf, schema_config
from src.kafka_logger import logging
from src.entity.generic import Generic, instance_to_dict


def delivery_report(err, msg):
    """Reports the success or failure of a message delivery."""
    if err is not None:
        logging.error("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    logging.info("Record {} produced to {} [{}] at offset {}".format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def produce_data_from_file(topic: str, file_path: str):
    """Read a CSV file and produce each row as a JSON message to Kafka."""
    schema_str = Generic.get_schema_to_produce_consume_data(file_path=file_path)
    schema_registry_client = SchemaRegistryClient(schema_config())

    string_serializer = StringSerializer("utf_8")
    json_serializer = JSONSerializer(schema_str, schema_registry_client, instance_to_dict)

    producer = Producer(sasl_conf())

    logging.info("Producing records to topic %s", topic)
    try:
        for instance in Generic.get_object(file_path=file_path):
            # Service delivery callbacks between produces to avoid buffer overflow
            producer.poll(0.0)
            producer.produce(
                topic=topic,
                key=string_serializer(str(uuid4()), SerializationContext(topic, MessageField.KEY)),
                value=json_serializer(instance, SerializationContext(topic, MessageField.VALUE)),
                on_delivery=delivery_report,
            )
    except KeyboardInterrupt:
        logging.info("Producer interrupted by user")
    except BufferError as e:
        logging.error("Producer buffer full: %s", e)
        raise
    finally:
        logging.info("Flushing remaining records...")
        producer.flush()
