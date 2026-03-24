import logging
import os

from src.kafka_logger import setup_logging
from src.kafka_producer.json_producer import produce_data_from_file
from src.constant import SAMPLE_DIR

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    setup_logging()

    topics = [
        d for d in os.listdir(SAMPLE_DIR)
        if os.path.isdir(os.path.join(SAMPLE_DIR, d))
    ]
    logger.info("Discovered topics: %s", topics)

    for topic in topics:
        topic_dir = os.path.join(SAMPLE_DIR, topic)
        files = os.listdir(topic_dir)
        if not files:
            logger.warning("Skipping empty topic directory: %s", topic)
            continue
        sample_file = os.path.join(topic_dir, files[0])
        produce_data_from_file(topic=topic, file_path=sample_file)
