from shared_code.helpers import create_correlation_id
from uuid import UUID
from unittest.mock import patch
import pytest


class Test_create_correlation_id:
    @pytest.fixture
    def mock_uuid4(self):
        mock_uuid = "12345678-1234-5678-1234-567812345678"
        with patch("shared_code.helpers.uuid4", return_value=UUID(mock_uuid)):
            yield

    def test_create_correlation_id(self, mock_uuid4):
        correlation_id = create_correlation_id()
        assert isinstance(correlation_id, str)
        assert correlation_id == "12345678-1234-5678-1234-567812345678"
