from shared_code import helpers
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
