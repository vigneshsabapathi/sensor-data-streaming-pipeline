"""Dead Letter Queue producer for failed messages."""

import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer

from src.kafka_config import sasl_conf

logger = logging.getLogger(__name__)


class DLQProducer:
    """Produces failed messages to a Dead Letter Queue topic.

    Messages are enriched with error metadata (original topic, error type,
    timestamp) so they can be inspected and reprocessed.
    """

    DLQ_SUFFIX = ".dlq"

    def __init__(self, producer: Producer | None = None):
        self._producer = producer or Producer(sasl_conf())
        self._serializer = StringSerializer("utf_8")

    def send_to_dlq(
        self,
        original_topic: str,
        original_value: bytes | None,
        original_key: bytes | None,
        error: Exception,
        error_stage: str = "processing",
    ) -> None:
        """Send a failed message to the DLQ topic.

        Args:
            original_topic: The topic the message was consumed from.
            original_value: Raw message value bytes.
            original_key: Raw message key bytes.
            error: The exception that caused the failure.
            error_stage: Where the failure occurred (e.g., "deserialization", "db_insert").
        """
        dlq_topic = f"{original_topic}{self.DLQ_SUFFIX}"

        dlq_record = {
            "original_topic": original_topic,
            "original_key": original_key.decode("utf-8", errors="replace") if original_key else None,
            "original_value": original_value.decode("utf-8", errors="replace") if original_value else None,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_stage": error_stage,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        try:
            self._producer.produce(
                topic=dlq_topic,
                value=json.dumps(dlq_record).encode("utf-8"),
                on_delivery=self._dlq_delivery_report,
            )
            self._producer.poll(0.0)
            logger.warning(
                "Sent failed message to DLQ topic '%s': %s: %s",
                dlq_topic, type(error).__name__, error,
            )
        except Exception:
            logger.exception("Failed to send message to DLQ topic '%s'", dlq_topic)

    def flush(self):
        self._producer.flush()

    @staticmethod
    def _dlq_delivery_report(err, msg):
        if err:
            logger.error("DLQ delivery failed: %s", err)
        else:
            logger.debug("DLQ message delivered to %s [%s]", msg.topic(), msg.partition())
