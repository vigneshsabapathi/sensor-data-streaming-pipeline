import logging

import certifi
import pymongo

logger = logging.getLogger(__name__)


class MongodbOperation:

    def __init__(self, db_url: str, db_name: str = "ineuron") -> None:
        # Only use TLS for Atlas/SRV connections; local MongoDB doesn't need it
        kwargs = {}
        if "+srv" in db_url or "ssl=true" in db_url.lower():
            kwargs["tlsCAFile"] = certifi.where()
        self.client = pymongo.MongoClient(db_url, **kwargs)
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
