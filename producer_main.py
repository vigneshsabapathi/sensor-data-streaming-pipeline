import os

from src.kafka_producer.json_producer import produce_data_from_file
from src.constant import SAMPLE_DIR


if __name__ == "__main__":
    topics = [
        d for d in os.listdir(SAMPLE_DIR)
        if os.path.isdir(os.path.join(SAMPLE_DIR, d))
    ]
    print(f"Topics: {topics}")

    for topic in topics:
        topic_dir = os.path.join(SAMPLE_DIR, topic)
        files = os.listdir(topic_dir)
        if not files:
            print(f"Skipping empty topic directory: {topic}")
            continue
        sample_file = os.path.join(topic_dir, files[0])
        produce_data_from_file(topic=topic, file_path=sample_file)
