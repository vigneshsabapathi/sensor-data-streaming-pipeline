"""Tests for MongodbOperation with mocked pymongo.MongoClient."""

from unittest.mock import MagicMock, patch


class TestMongodbOperationContextManager:
    """Test that MongodbOperation follows the context manager protocol."""

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_context_manager_enter_returns_self(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        op = MongodbOperation(db_url="mongodb://fake:27017", db_name="testdb")

        with op as ctx:
            assert ctx is op

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_context_manager_exit_calls_close(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        with MongodbOperation(db_url="mongodb://fake:27017"):
            pass

        mock_client.close.assert_called_once()

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_close_calls_client_close(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        op = MongodbOperation(db_url="mongodb://fake:27017")
        op.close()

        mock_client.close.assert_called_once()


class TestMongodbOperationInit:
    """Test constructor behavior."""

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_default_db_name(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        op = MongodbOperation(db_url="mongodb://fake:27017")
        assert op.db_name == "ineuron"

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_custom_db_name(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        op = MongodbOperation(db_url="mongodb://fake:27017", db_name="mydb")
        assert op.db_name == "mydb"

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_client_created_with_url_and_tls(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        MongodbOperation(db_url="mongodb://fake:27017")

        mock_client_cls.assert_called_once_with(
            "mongodb://fake:27017", tlsCAFile="/fake/ca.pem"
        )


class TestMongodbOperationInsert:
    """Test insert methods delegate to pymongo correctly."""

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_insert_many(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        op = MongodbOperation(db_url="mongodb://fake:27017", db_name="testdb")
        records = [{"a": 1}, {"a": 2}]
        op.insert_many("my_collection", records)

        mock_client["testdb"]["my_collection"].insert_many.assert_called_once_with(records)

    @patch("src.database.mongodb.pymongo.MongoClient")
    @patch("src.database.mongodb.certifi.where", return_value="/fake/ca.pem")
    def test_insert_one(self, mock_certifi, mock_client_cls):
        from src.database.mongodb import MongodbOperation

        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        op = MongodbOperation(db_url="mongodb://fake:27017", db_name="testdb")
        record = {"a": 1}
        op.insert("my_collection", record)

        mock_client["testdb"]["my_collection"].insert_one.assert_called_once_with(record)
