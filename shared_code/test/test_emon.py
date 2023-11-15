from shared_code import emon
import pytest
from unittest.mock import patch


class TestExtractTimestamp:
    @patch("shared_code.emon.to_datetime_string")
    def test_extract_timestamp_valid(self, mock_to_datetime_string):
        # Test case with valid timestamp
        mock_to_datetime_string.return_value = "mock_return_value"
        message_payload = {"time": 1699364497.0467954}
        timestamp = emon.extract_timestamp(message_payload)
        assert timestamp == "mock_return_value"
        assert mock_to_datetime_string.call_count == 1

    def test_extract_timestamp_missing_time(self):
        # Test case where 'time' key is missing
        message_payload = {"other_key": "value"}
        with pytest.raises(ValueError) as exc_info:
            emon.extract_timestamp(message_payload)
        assert str(exc_info.value) == (
            f"Invalid message_payload: emon: missing time {message_payload}"
        )

    def test_extract_timestamp_invalid_object(self):
        # Test case with invalid time format
        message_payload = {"a_string"}
        # Assuming to_datetime raises a specific exception for invalid format
        with pytest.raises(ValueError) as exe:
            emon.extract_timestamp(message_payload)
        assert str(exe.value) == (
            f"Invalid message_payload: emon processor only handles dict message_payload, not {type(message_payload)}"
        )


class TestEmonToTimescale:
    @patch("shared_code.emon.validate_message_body_type_and_keys")
    @patch("shared_code.emon.validate_publisher")
    @patch("shared_code.emon.is_topic_of_interest")
    @patch("shared_code.emon.json.loads")
    @patch("shared_code.emon.extract_timestamp")
    @patch("shared_code.emon.create_correlation_id")
    @patch("shared_code.emon.create_record_recursive")
    def test_emon_to_timescale_subject_none(
        self,
        mock_create_record_recursive,
        mock_create_correlation_id,
        mock_extract_timestamp,
        mock_json_loads,
        mock_is_topic_of_interest,
        mock_validate_publisher,
        mock_validate_message_body,
    ):
        mock_is_topic_of_interest.return_value = None
        this_service = "emon"
        publisher = "test_publisher"
        topic = "abc/def"
        payload = {}

        result = emon.emon_to_timescale(payload, topic, publisher)

        assert result is None
        mock_validate_message_body.assert_called_once_with(payload, this_service)
        mock_validate_publisher.assert_called_once_with(publisher, this_service)
        mock_is_topic_of_interest.assert_called_once_with(topic, ["emonTx4"])
        mock_create_record_recursive.assert_not_called()

    @patch("shared_code.emon.validate_message_body_type_and_keys")
    @patch("shared_code.emon.validate_publisher")
    @patch("shared_code.emon.is_topic_of_interest")
    @patch("shared_code.emon.json.loads")
    @patch("shared_code.emon.extract_timestamp")
    @patch("shared_code.emon.create_correlation_id")
    @patch("shared_code.emon.create_record_recursive")
    def test_emon_to_timescale_valid(
        self,
        mock_create_record_recursive,
        mock_create_correlation_id,
        mock_extract_timestamp,
        mock_json_loads,
        mock_is_topic_of_interest,
        mock_validate_publisher,
        mock_validate_message_body,
    ):
        mock_is_topic_of_interest.return_value = "def"
        mock_json_loads.return_value = {"payload_data": "some_data"}
        mock_extract_timestamp.return_value = "2023-01-01T00:00:00"
        mock_create_correlation_id.return_value = "correlation_id"
        mock_create_record_recursive.return_value = "sample_record"
        this_service = "emon"
        publisher = "test_publisher"
        topic = "abc/def"
        payload = {"payload": '{"payload_data":"some_data"}'}

        result = emon.emon_to_timescale(payload, topic, publisher)

        mock_validate_message_body.assert_called_once_with(payload, this_service)
        mock_validate_publisher.assert_called_once_with(publisher, this_service)
        mock_is_topic_of_interest.assert_called_once_with(topic, ["emonTx4"])
        mock_json_loads.assert_called_once_with('{"payload_data":"some_data"}')
        mock_extract_timestamp.assert_called_once_with({"payload_data": "some_data"})
        mock_create_correlation_id.assert_called_once()
        mock_create_record_recursive.assert_called_once_with(
            payload=mock_json_loads.return_value,
            records=[],
            timestamp=mock_extract_timestamp.return_value,
            correlation_id=mock_create_correlation_id.return_value,
            measurement_publisher=publisher,
            measurement_subject=mock_is_topic_of_interest.return_value,
            ignore_keys=["time"],
        )
        assert result == mock_create_record_recursive.return_value
