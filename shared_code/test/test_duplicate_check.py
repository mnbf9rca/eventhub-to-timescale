import pytest
import uuid
from unittest.mock import Mock, patch
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables import TableEntity


# Assuming the original function is imported like this
from shared_code.duplicate_check import (
    ensure_table_exists,
    store_id,
    check_duplicate,
    get_table_service_client,
)


class TestEnsureTableExists:
    @patch("shared_code.duplicate_check.TableServiceClient")
    def test_ensure_table_exists_creates_table(self, MockTableServiceClient):
        mock_instance = MockTableServiceClient.return_value
        ensure_table_exists("some_table", mock_instance)

        # Assert create_table was called once with "some_table"
        mock_instance.create_table.assert_called_once_with("some_table")

    @patch("shared_code.duplicate_check.TableServiceClient")
    def test_ensure_table_exists_table_already_exists(self, MockTableServiceClient):
        mock_instance = MockTableServiceClient.return_value
        mock_instance.create_table.side_effect = ResourceExistsError("Table exists")

        # Should not raise any exception
        ensure_table_exists("some_table", mock_instance)

    @patch("shared_code.duplicate_check.TableServiceClient")
    def test_ensure_table_exists_raises_exception(self, MockTableServiceClient):
        mock_instance = MockTableServiceClient.return_value
        mock_instance.create_table.side_effect = Exception("Random exception")

        # Should raise the custom exception with message
        with pytest.raises(Exception) as e:
            ensure_table_exists("some_table", mock_instance)
        assert str(e.value) == "Failed to ensure table exists: Random exception"


class TestStoreID:
    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_store_id_success(self, MockEnsureTableExists, MockTableServiceClient):
        mock_table_client = Mock()
        MockTableServiceClient.return_value.get_table_client.return_value = (
            mock_table_client
        )

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
        MockTableServiceClient.return_value.get_table_client.return_value = (
            mock_table_client
        )

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
        MockTableServiceClient.return_value.get_table_client.side_effect = Exception(
            "Get table client failed"
        )

        with pytest.raises(Exception) as e:
            store_id("some_id", "some_context", MockTableServiceClient.return_value)

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        assert str(e.value) == "Get table client failed"


class TestCheckDuplicate:
    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_check_duplicate_entity_exists(
        self, MockEnsureTableExists, MockTableServiceClient
    ):
        mock_table_client = Mock()
        MockTableServiceClient.return_value.get_table_client.return_value = (
            mock_table_client
        )

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
        MockTableServiceClient.return_value.get_table_client.return_value = (
            mock_table_client
        )

        result = check_duplicate(
            "some_id", "some_context", MockTableServiceClient.return_value
        )

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        assert result is False

    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_check_duplicate_raises_exception(
        self, MockEnsureTableExists, MockTableServiceClient
    ):
        mock_table_client = Mock()
        mock_table_client.get_entity.side_effect = Exception("Random exception")
        MockTableServiceClient.return_value.get_table_client.return_value = (
            mock_table_client
        )

        with pytest.raises(Exception) as e:
            check_duplicate(
                "some_id", "some_context", MockTableServiceClient.return_value
            )

        MockEnsureTableExists.assert_called_once_with(
            "some_context", MockTableServiceClient.return_value
        )
        assert str(e.value) == "Failed to check for duplicate: Random exception"

    @patch("shared_code.duplicate_check.TableServiceClient")
    @patch("shared_code.duplicate_check.ensure_table_exists")
    def test_store_id_idempotent(self, MockEnsureTableExists, MockTableServiceClient):
        mock_table_client = Mock()
        mock_table_client.create_entity.side_effect = ResourceExistsError(
            "Entity already exists"
        )
        MockTableServiceClient.return_value.get_table_client.return_value = (
            mock_table_client
        )

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
