from shared_code import helpers
from uuid import UUID
from unittest.mock import patch
import pytest
from dateutil import parser
from datetime import datetime


class Test_create_correlation_id:
    @pytest.fixture
    def mock_uuid4(self):
        mock_uuid = "12345678-1234-5678-1234-567812345678"
        with patch("shared_code.helpers.uuid4", return_value=UUID(mock_uuid)):
            yield

    def test_create_correlation_id(self, mock_uuid4):
        correlation_id = helpers.create_correlation_id()
        assert isinstance(correlation_id, str)
        assert correlation_id == "12345678-1234-5678-1234-567812345678"


class Test_is_topic_of_interest:
    def test_topic_of_interest(self):
        # Test case where the topic is of interest
        topic = "home/temperature/living_room"
        events_of_interest = ["living_room", "kitchen"]
        assert helpers.is_topic_of_interest(topic, events_of_interest) == "living_room"

    def test_topic_not_of_interest(self):
        # Test case where the topic is not of interest
        topic = "home/temperature/bathroom"
        events_of_interest = ["living_room", "kitchen"]
        assert helpers.is_topic_of_interest(topic, events_of_interest) is None

    def test_empty_topic(self):
        # Test case with an empty topic
        topic = ""
        events_of_interest = ["living_room", "kitchen"]
        assert helpers.is_topic_of_interest(topic, events_of_interest) is None

    def test_empty_events_of_interest(self):
        # Test case with an empty list of events of interest
        topic = "home/temperature/living_room"
        events_of_interest = []
        assert helpers.is_topic_of_interest(topic, events_of_interest) is None

    def test_topic_and_events_of_interest_empty(self):
        # Test case where both topic and events of interest are empty
        topic = ""
        events_of_interest = []
        assert helpers.is_topic_of_interest(topic, events_of_interest) is None


class Test_to_datetime:
    def test_to_datetime_with_valid_numeric_timestamp(self):
        # Test with valid numeric timestamp
        timestamp = 1699364497.0467954  # Example timestamp
        expected_datetime = datetime.fromtimestamp(float(timestamp)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        assert helpers.to_datetime(timestamp) == expected_datetime

    def test_to_datetime_with_valid_string_timestamp(self):
        # Test with valid string timestamp
        timestamp = "2023-01-01T00:00:00"
        expected_datetime = parser.parse(timestamp).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        assert helpers.to_datetime(timestamp) == expected_datetime

    def test_to_datetime_with_out_of_range_timestamp(self):
        # Test with out-of-range numeric timestamp
        timestamp = 253402300800  # Out of valid range
        with pytest.raises(ValueError) as exc_info:
            helpers.to_datetime(timestamp)
        assert f"Timestamp out of range: {timestamp}" in str(exc_info.value)

    def test_to_datetime_with_invalid_numeric_timestamp(self):
        # Test with invalid numeric format
        timestamp = "not_a_number"
        with pytest.raises(ValueError):
            helpers.to_datetime(timestamp)

    def test_to_datetime_with_invalid_string_timestamp(self):
        # Test with invalid string format
        timestamp = "invalid_date_string"
        with pytest.raises(ValueError):
            helpers.to_datetime(timestamp)

    # Optional: Test for empty/null input, if applicable
    def test_to_datetime_with_empty_input(self):
        # Test with empty string
        timestamp = ""
        with pytest.raises(ValueError):
            helpers.to_datetime(timestamp)
