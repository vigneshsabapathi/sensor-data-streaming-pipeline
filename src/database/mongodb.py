import logging

import certifi
import pymongo

logger = logging.getLogger(__name__)


class MongodbOperation:

    def __init__(self, db_url: str, db_name: str = "ineuron") -> None:
        self.client = pymongo.MongoClient(db_url, tlsCAFile=certifi.where())
        self.db_name = db_name

    def insert_many(self, collection_name: str, records: list):
        self.client[self.db_name][collection_name].insert_many(records)

    def insert(self, collection_name: str, record: dict):
        self.client[self.db_name][collection_name].insert_one(record)

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
