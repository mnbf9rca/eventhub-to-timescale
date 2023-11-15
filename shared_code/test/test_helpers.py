from shared_code import helpers
from uuid import UUID
from unittest.mock import patch
import pytest


class TestCreateCorrelationId:
    @pytest.fixture
    def mock_uuid4(self):
        mock_uuid = "12345678-1234-5678-1234-567812345678"
        with patch("shared_code.helpers.uuid4", return_value=UUID(mock_uuid)):
            yield

    def test_create_correlation_id(self, mock_uuid4):
        correlation_id = helpers.create_correlation_id()
        assert isinstance(correlation_id, str)
        assert correlation_id == "12345678-1234-5678-1234-567812345678"


class TestIsTopicOfInterest:
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


class TestValidateMessageBody:
    def test_with_valid_messagebody(self):
        result = helpers.validate_message_body_type_and_keys(
            {"payload": "test"}, "testing"
        )
        assert result is None

    def test_with_missing_service_name(self):
        with pytest.raises(
            ValueError, match=r".*validate_message_body: Invalid service_name: .*"
        ):
            helpers.validate_message_body_type_and_keys({"payload": "test"}, None)

    def test_with_missing_messagebody(self):
        with pytest.raises(
            ValueError, match=r".*Invalid messagebody: testing: messagebody is None.*"
        ):
            helpers.validate_message_body_type_and_keys(None, "testing")

    def test_with_missing_payload(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid messagebody: testing: 'payload', not in {'test': 'test'}.*",
        ):
            helpers.validate_message_body_type_and_keys({"test": "test"}, "testing")

    def test_with_non_dict(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid messagebody: testing processor only handles dict messages, not .*",
        ):
            helpers.validate_message_body_type_and_keys("test", "testing")

    def test_with_non_string_service_name(self):
        with pytest.raises(
            ValueError, match=r".*validate_message_body: Invalid service_name: .*"
        ):
            helpers.validate_message_body_type_and_keys({"payload": "test"}, 7)

    def test_with_empty_dict(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid messagebody: testing: 'payload', not in {}.*",
        ):
            helpers.validate_message_body_type_and_keys({}, "testing")

    def test_with_other_keys(self):
        result = helpers.validate_message_body_type_and_keys(
            {"payload": "test_val", "test_key": "test_val2"},
            "testing_service_name",
            ["test_key"],
        )
        assert result is None

    def test_with_missing_other_keys(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid messagebody: testing_service_name: 'test_key', not in .*",
        ):
            helpers.validate_message_body_type_and_keys(
                {"payload": "test_val"}, "testing_service_name", ["test_key"]
            )

    def test_with_missing_other_keys_bigger_dict(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid messagebody: testing_service_name: 'test_key', not in .*",
        ):
            helpers.validate_message_body_type_and_keys(
                {"payload": "test_val", "not_the_test_key": "test_val2"},
                "testing_service_name",
                ["test_key"],
            )


class TestToDatetime:
    @pytest.mark.parametrize(
        "test_data, expected_value",
        [
            ("2021-01-01T00:00:00", "2021-01-01T00:00:00.000000Z"),
            ("2021-01-01T00:00:00+00:00", "2021-01-01T00:00:00.000000Z"),
            ("2021-01-01T00:00:00.123", "2021-01-01T00:00:00.123000Z"),
            ("2021-01-01T00:00:00.123+00:00", "2021-01-01T00:00:00.123000Z"),
            (1609459200, "2021-01-01T00:00:00.000000Z"),
            (1609459200.123, "2021-01-01T00:00:00.123000Z"),
            ("2021-01-01", "2021-01-01T00:00:00.000000Z"),
            (1609459200.123456789, "2021-01-01T00:00:00.123457Z"),
        ],
    )
    def test_to_datetime_with_valid_timestamp(self, test_data, expected_value):
        actual_value = helpers.to_datetime_string(test_data)
        assert actual_value == expected_value
        assert isinstance(actual_value, str)

    def test_to_datetime_with_out_of_range_timestamp(self):
        # Test with out-of-range numeric timestamp
        timestamp = 253402300800  # Out of valid range
        with pytest.raises(ValueError) as exc_info:
            helpers.to_datetime_string(timestamp)
        assert f"Timestamp out of range: {timestamp}" in str(exc_info.value)

    def test_to_datetime_with_invalid_string_timestamp(self):
        # Test with invalid numeric format
        timestamp = "invalid_date_string"
        with pytest.raises(ValueError) as exc_info:
            helpers.to_datetime_string(timestamp)
        assert f"Invalid string timestamp format: {timestamp}" in str(exc_info.value)

    def test_to_datetime_with_invalid_type_timestamp(self):
        # Test with invalid string format
        timestamp = {"invalid": "type"}
        with pytest.raises(TypeError) as exc_info:
            helpers.to_datetime_string(timestamp)
        assert f"Unsupported type for timestamp: {type(timestamp).__name__}" in str(
            exc_info.value
        )

    def test_to_datetime_with_empty_input(self):
        # Test with empty string
        timestamp = ""
        with pytest.raises(ValueError) as exc_info:
            helpers.to_datetime_string(timestamp)
        assert f"Invalid string timestamp format: {timestamp}" in str(exc_info.value)


class TestValidatePublisher:
    def test_with_valid_publisher(self):
        result = helpers.validate_publisher("test_pub", "test_pub")
        assert result is None

    def test_with_invalid_publisher_type(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid publisher: test_pub processor only handles test_pub messages, not <class 'int'>.*",
        ):
            helpers.validate_publisher(7, "test_pub")

    def test_with_missing_expected_publisher_type(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid expected_publisher: expected str not.*",
        ):
            helpers.validate_publisher("test", None)

    def test_with_non_string_expected_publisher_type(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid expected_publisher: expected str not.*",
        ):
            helpers.validate_publisher("test", 7)

    def test_with_invalid_publisher(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid publisher: test_pub processor only handles test_pub messages, not incorrect_publisher.*",
        ):
            helpers.validate_publisher("incorrect_publisher", "test_pub")


class TestRecursivelyDeserialize:
    @pytest.mark.parametrize(
        "test_data, expected_value",
        [
            ('{"a": 1}', {"a": 1}),
            ('{"a": 1', '{"a": 1'),
            (None, None),
            ("", ""),
            ('{"a": {"b": 1}}', {"a": {"b": 1}}),
            ('{"a": [{"b": 1}]}', {"a": [{"b": 1}]}),
            ('{"a": [{"b": 1}, {"c": 2}]}', {"a": [{"b": 1}, {"c": 2}]}),
            ('{"a": [{"b": 1}, {"c": 2}, "d"]}', {"a": [{"b": 1}, {"c": 2}, "d"]}),
            (
                '{"homie_heartbeat": {"type": "EventHubEvent", "properties": {"body": "[{\\"a\\": \\"1\\", \\"b\\": \\"2\\"}, {\\"c\\": 3, \\"d\\": 4}]"}}}',  # noqa: E501
                {
                    "homie_heartbeat": {
                        "type": "EventHubEvent",
                        "properties": {"body": [{"a": 1, "b": 2}, {"c": 3, "d": 4}]},
                    }
                },
            ),
            ('{"a": 1, "b": 2}', {"a": 1, "b": 2}),
        ],
    )
    # @pytest.mark.parametrize(
    #     "test_data, expected_value",
    #     [
    #         ('{"a": {"b": 1}}', {"a": {"b": 1}}),
    #     ],
    # )
    def test_recursively_deserialize(self, test_data, expected_value):
        actual_value = helpers.recursively_deserialize(test_data)
        assert actual_value == expected_value
