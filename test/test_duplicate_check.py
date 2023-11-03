import pytest
import uuid
from unittest.mock import Mock, patch, MagicMock
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableEntity


# Assuming the original function is imported like this
from shared_code.duplicate_check import (
    ensure_table_exists,
    store_id,
    check_duplicate,
    get_table_service_client,
    store_serialized_object,
    retrieve_serialized_object,
)


class TestEnsureTableExists:
    @patch("shared_code.duplicate_check.TableServiceClient")
    def test_ensure_table_exists_creates_table(self, MockTableServiceClient):
        mock_instance = MockTableServiceClient.return_value
        result = ensure_table_exists("some_table", mock_instance)

        # Assert create_table was called once with "some_table"
        mock_instance.create_table_if_not_exists.assert_called_once_with("some_table")
        assert result is mock_instance.create_table_if_not_exists.return_value

    @patch("shared_code.duplicate_check.TableServiceClient")
    def test_ensure_table_exists_raises_exception(self, MockTableServiceClient):
        mock_instance = MockTableServiceClient.return_value
        mock_instance.create_table_if_not_exists.side_effect = Exception(
            "Random exception"
        )

        # Should raise the custom exception with message
        with pytest.raises(Exception) as e:
            ensure_table_exists("some_table", mock_instance)
        assert str(e.value) == "Failed to ensure table exists: Random exception"


class TestStoreID:
    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_store_id_success(self, MockEnsureTableExists, MockTableServiceClient):
        mock_table_client = Mock()
        MockEnsureTableExists.return_value = mock_table_client

        result = store_id(
            "some_id", "some_context", MockTableServiceClient.return_value
        )

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        mock_table_client.create_entity.assert_called_once_with(
            TableEntity(PartitionKey="messages", RowKey="some_id")
        )
        assert result is True

    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_store_id_exception(self, MockEnsureTableExists, MockTableServiceClient):
        mock_table_client = Mock()
        mock_table_client.create_entity.side_effect = Exception("Random exception")
        MockEnsureTableExists.return_value = mock_table_client

        with pytest.raises(Exception) as e:
            store_id("some_id", "some_context", MockTableServiceClient.return_value)

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        assert str(e.value) == "Failed to write to table: Random exception"

    @patch("shared_code.duplicate_check.TableServiceClient")
    def test_store_id_calls_ensure_table_exists(self, MockTableServiceClient):
        mock_table_client = Mock()
        MockTableServiceClient.return_value.get_table_client.return_value = (
            mock_table_client
        )

        with patch(
            "shared_code.duplicate_check.ensure_table_exists"
        ) as MockEnsureTableExists:
            store_id("some_id", "some_context", MockTableServiceClient.return_value)

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )

    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_get_table_client_fails(
        self, MockEnsureTableExists, MockTableServiceClient
    ):
        MockEnsureTableExists.side_effect = Exception("Get table client failed")

        with pytest.raises(Exception) as e:
            store_id("some_id", "some_context", MockTableServiceClient.return_value)

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        assert str(e.value) == "Get table client failed"

    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_store_id_idempotent(self, MockEnsureTableExists, MockTableServiceClient):
        mock_table_client = Mock()
        mock_table_client.create_entity.side_effect = ResourceExistsError(
            "Entity already exists"
        )
        MockEnsureTableExists.return_value = mock_table_client

        result = store_id(
            "some_id", "some_context", MockTableServiceClient.return_value
        )

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        mock_table_client.create_entity.assert_called_once_with(
            TableEntity(PartitionKey="messages", RowKey="some_id")
        )
        assert result is True  # Should return True even when entity already exists


class TestCheckDuplicate:
    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_check_duplicate_entity_exists(
        self, MockEnsureTableExists, MockTableServiceClient
    ):
        mock_table_client = Mock()
        MockEnsureTableExists.return_value = mock_table_client

        result = check_duplicate(
            "some_id", "some_context", MockTableServiceClient.return_value
        )

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        mock_table_client.get_entity.assert_called_once_with("messages", "some_id")
        assert result is True

    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_check_duplicate_entity_not_found(
        self, MockEnsureTableExists, MockTableServiceClient
    ):
        mock_table_client = Mock()
        mock_table_client.get_entity.side_effect = ResourceNotFoundError(
            "Entity not found"
        )
        MockEnsureTableExists.return_value = mock_table_client

        result = check_duplicate(
            "some_id", "some_context", MockTableServiceClient.return_value
        )

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        assert mock_table_client.get_entity.call_count == 1
        assert result is False

    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_check_duplicate_raises_exception(
        self, MockEnsureTableExists, MockTableServiceClient
    ):
        mock_table_client = Mock()
        mock_table_client.get_entity.side_effect = Exception("Random exception")
        MockEnsureTableExists.return_value = mock_table_client

        with pytest.raises(Exception) as e:
            check_duplicate(
                "some_id", "some_context", MockTableServiceClient.return_value
            )

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        assert str(e.value) == "Failed to check for duplicate: Random exception"
        assert mock_table_client.get_entity.call_count == 1


class TestGetTableServiceClient:
    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch.dict(
        "shared_code.duplicate_check.os.environ",
        {"AZURE_STORAGE_CONNECTION_STRING": "mock_connection_string"},
    )
    def test_initialize_table_service(self, MockTableServiceClient):
        MockTableServiceClient.from_connection_string.return_value = (
            MockTableServiceClient.return_value
        )

        result = get_table_service_client()

        MockTableServiceClient.from_connection_string.assert_called_once_with(
            "mock_connection_string"
        )
        assert result is MockTableServiceClient.return_value


class TestWithRealTableServiceCall:
    def test_end_to_end(self):
        tsc = get_table_service_client()
        context = "testdonotuseordelete"
        id = str(uuid.uuid4())
        assert check_duplicate(id, context, tsc) is False
        assert store_id(id, context, tsc) is True
        assert check_duplicate(id, context, tsc) is True

    def test_end_to_end_with_serialized_object(self):
        tsc = get_table_service_client()
        context = "testdonotuseordelete"
        id = str(uuid.uuid4())
        assert store_serialized_object(id, context, "test", tsc) is True
        assert retrieve_serialized_object(id, context, tsc) == "test"


class TestStoreSerializedObject:
    @pytest.fixture
    def mock_table_service_client(self):
        return MagicMock()

    @pytest.fixture
    def table_name(self):
        return "test_table"

    @pytest.fixture
    def key(self):
        return "test_key"

    @pytest.fixture
    def serialized_object(self):
        return "test_object"

    def test_store_serialized_object_success(
        self, mock_table_service_client, table_name, key, serialized_object
    ):
        with patch(
            "shared_code.duplicate_check.ensure_table_exists",
            return_value=mock_table_service_client,
        ):
            result = store_serialized_object(
                key,
                table_name,
                serialized_object,
                mock_table_service_client,
                overwrite=True,
            )
            assert result is True
            mock_table_service_client.upsert_entity.assert_called_once()

    def test_store_serialized_object_existing_key(
        self, mock_table_service_client, table_name, key, serialized_object
    ):
        with patch(
            "shared_code.duplicate_check.ensure_table_exists",
            return_value=mock_table_service_client,
        ):
            mock_table_service_client.get_entity.return_value = {
                "PartitionKey": table_name,
                "RowKey": key,
                "SerializedObject": serialized_object,
            }
            with pytest.raises(Exception) as exc_info:
                store_serialized_object(
                    key,
                    table_name,
                    serialized_object,
                    mock_table_service_client,
                    overwrite=False,
                )
            assert (
                str(exc_info.value)
                == f"Entity with key {key} already exists and overwrite is False"
            )

    def test_store_serialized_object_resource_not_found(
        self, mock_table_service_client, table_name, key, serialized_object
    ):
        with patch(
            "shared_code.duplicate_check.ensure_table_exists",
            return_value=mock_table_service_client,
        ):
            mock_table_service_client.get_entity.side_effect = ResourceNotFoundError(
                "Not Found"
            )
            result = store_serialized_object(
                key,
                table_name,
                serialized_object,
                mock_table_service_client,
                overwrite=False,
            )
            assert result is True

    def test_store_serialized_object_generic_exception(
        self, mock_table_service_client, table_name, key, serialized_object
    ):
        with patch(
            "shared_code.duplicate_check.ensure_table_exists",
            return_value=mock_table_service_client,
        ):
            mock_table_service_client.get_entity.side_effect = Exception(
                "Generic Exception"
            )
            with pytest.raises(Exception) as exc_info:
                store_serialized_object(
                    key,
                    table_name,
                    serialized_object,
                    mock_table_service_client,
                    overwrite=False,
                )
            assert str(exc_info.value) == "Generic Exception"

    def test_store_serialized_object_upsert_entity_raises(
        self, mock_table_service_client, table_name, key, serialized_object
    ):
        with patch(
            "shared_code.duplicate_check.ensure_table_exists",
            return_value=mock_table_service_client,
        ):
            mock_table_service_client.upsert_entity.side_effect = Exception(
                "Upsert failed"
            )
            with pytest.raises(Exception) as exc_info:
                store_serialized_object(
                    key,
                    table_name,
                    serialized_object,
                    mock_table_service_client,
                    overwrite=True,
                )
            assert str(exc_info.value) == "Upsert failed"


class TestRetrieveSerializedObject:
    @pytest.fixture
    def mock_table_service_client(self):
        return MagicMock()

    @pytest.fixture
    def table_name(self):
        return "test_table"

    @pytest.fixture
    def key(self):
        return "test_key"

    def test_retrieve_serialized_object_success(
        self, mock_table_service_client, table_name, key
    ):
        mock_entity = TableEntity(
            {"PartitionKey": 123, "RowKey": "abc", "body": '{"a": "b"}'}
        )
        mock_table_service_client.get_table_client.return_value.get_entity.return_value = (
            mock_entity
        )
        result = retrieve_serialized_object(key, table_name, mock_table_service_client)
        assert result == '{"a": "b"}'

    def test_retrieve_serialized_object_resource_not_found(
        self, mock_table_service_client, table_name, key
    ):
        mock_table_service_client.get_table_client.return_value.get_entity.side_effect = ResourceNotFoundError(
            "Not Found"
        )
        result = retrieve_serialized_object(key, table_name, mock_table_service_client)
        assert result is None

    def test_retrieve_serialized_object_generic_exception(
        self, mock_table_service_client, table_name, key
    ):
        mock_table_service_client.get_table_client.return_value.get_entity.side_effect = Exception(
            "Generic Exception"
        )
        with pytest.raises(Exception) as exc_info:
            retrieve_serialized_object(key, table_name, mock_table_service_client)
        assert str(exc_info.value) == "Generic Exception"
