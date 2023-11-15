import pytest
import json
import datetime
from typing import List
from unittest.mock import Mock, patch
import azure.functions as func

from shared_code import json_converter


def create_eventhub_event(body: str) -> func.EventHubEvent:
    return func.EventHubEvent(
        body=body.encode("UTF-8"),
        enqueued_time=datetime.datetime.now(),
        offset=0,
        sequence_number=0,
    )


class TestSendToConverter:
    @pytest.fixture()
    def mock_glow_to_timescale(self, mocker):
        return mocker.patch("shared_code.json_converter.glow_to_timescale")

    @pytest.fixture()
    def mock_homie_to_timescale(self, mocker):
        return mocker.patch("shared_code.json_converter.homie_to_timescale")

    @pytest.fixture()
    def mock_emon_to_timescale(self, mocker):
        return mocker.patch("shared_code.json_converter.emon_to_timescale")

    @pytest.fixture()
    def mock_logger(self, mocker):
        return mocker.patch("shared_code.json_converter.logging")

    @pytest.mark.parametrize(
        "publisher, expected_converter_name",
        [
            ("glow", "mock_glow_to_timescale"),
            ("homie", "mock_homie_to_timescale"),
            ("emon", "mock_emon_to_timescale"),
        ],
    )
    def test_send_to_converter(
        self,
        mock_glow_to_timescale,
        mock_homie_to_timescale,
        mock_emon_to_timescale,
        mock_logger,
        publisher,
        expected_converter_name,
        request: pytest.FixtureRequest,
    ):
        all_converters = [
            mock_glow_to_timescale,
            mock_homie_to_timescale,
            mock_emon_to_timescale,
        ]
        expected_converter = request.getfixturevalue(expected_converter_name)
        mock_messagebody = "mock_messagebody"
        mock_topic = "mock_topic"

        result = json_converter.send_to_converter(
            publisher, mock_messagebody, mock_topic
        )
        for converter in all_converters:
            if converter == expected_converter:
                converter.assert_called_once_with(
                    mock_messagebody, mock_topic, publisher
                )
            else:
                converter.assert_not_called()
        assert result == expected_converter.return_value

    def test_send_to_converter_invalid_converter(
        self,
        mock_glow_to_timescale,
        mock_homie_to_timescale,
        mock_emon_to_timescale,
        mock_logger,
    ):
        all_converters = [
            mock_glow_to_timescale,
            mock_homie_to_timescale,
            mock_emon_to_timescale,
        ]
        mock_publisher = "not_a_converter"
        mock_messagebody = "mock_messagebody"
        mock_topic = "mock_topic"

        with pytest.raises(ValueError) as e:
            json_converter.send_to_converter(
                mock_publisher, mock_messagebody, mock_topic
            )
        assert str(e.value) == f"Unknown publisher: {mock_publisher}"
        for converter in all_converters:
            converter.assert_not_called()
        assert mock_logger.error.call_count == 1


class TestGetEventAsStr:
    @pytest.mark.parametrize(
        "input_event, expected",
        [
            ("test string", "test string"),
            (create_eventhub_event("test event body"), "test event body"),
        ],
    )
    def test_get_event_as_str_success(self, input_event, expected):
        assert json_converter.get_event_as_str(input_event) == expected

    @pytest.mark.parametrize(
        "input_event, exception, expected_message",
        [
            (
                Mock(),
                json.JSONDecodeError("Could not serialize", "", 0),
                "<non-serializable object>",
            ),
            (Mock(), Exception("General exception"), "<unknown object>"),
        ],
    )
    def test_get_event_as_str_exceptions(
        self, mocker, input_event, exception, expected_message
    ):
        mocker.patch("json.dumps", side_effect=exception)
        with pytest.raises(TypeError) as exc_info:
            json_converter.get_event_as_str(input_event)
        assert expected_message in str(exc_info.value)

    @pytest.mark.parametrize(
        "input_event, expected_exception_message",
        [
            (12345, "Event 12345 is of type: <class 'int'> not str or EventHubEvent"),
            (
                [1, 2, 3],
                "Event [1, 2, 3] is of type: <class 'list'> not str or EventHubEvent",
            ),
            (
                {"key": "value"},
                'Event {"key": "value"} is of type: <class \'dict\'> not str or EventHubEvent',
            ),
        ],
    )
    def test_get_event_as_str_type_error(self, input_event, expected_exception_message):
        with pytest.raises(TypeError) as exc_info:
            json_converter.get_event_as_str(input_event)
        assert str(exc_info.value) == expected_exception_message

    @patch("shared_code.json_converter.logging.error")
    def test_get_event_as_str_event_body_error(self, mock_logging_error, mocker):
        # Create a mock EventHubEvent with a get_body method that raises an exception
        mock_event = Mock(spec=func.EventHubEvent)
        mock_event.get_body.side_effect = Exception("Error getting event body")

        with pytest.raises(Exception) as exc_info:
            json_converter.get_event_as_str(mock_event)

        # Assert that the specific error log is called
        mock_logging_error.assert_called_once_with(
            "Error getting event body: Error getting event body"
        )

        # Assert that the raised exception matches the expected message
        assert "Error getting event body" in str(exc_info.value)


class TestSendMessages:
    @pytest.fixture
    def mock_output_event_hub_message(self):
        return Mock(spec=func.Out)

    @pytest.mark.parametrize(
        "messages, expected_message_count, ids",
        [
            (["message1", "message2", "message3"], 3, []),
            (
                iter(["message1", "message2", "message3"]),
                3,
                [],
            ),  # testing with an iterator
            (
                [
                    {"name": "name1", "correlation_id": "id1"},
                    {"name3": "name4", "correlation_id": "id2"},
                ],
                2,
                ["id1", "id2"],
            ),
            (
                [
                    {"name": "name1", "correlation_id": "id1"},
                    {"name3": "name4", "correlation_id": "id1"},
                ],
                2,
                ["id1"],
            ),
        ],
    )
    @patch("shared_code.json_converter.logging")
    def test_send_messages_success(
        self,
        mock_logging,
        messages,
        expected_message_count,
        ids,
        mock_output_event_hub_message,
    ):
        json_converter.send_messages(messages, mock_output_event_hub_message)
        assert mock_output_event_hub_message.set.call_count == 1

        message_payload = mock_output_event_hub_message.set.call_args[0][0]
        assert len(message_payload) == expected_message_count
        assert isinstance(message_payload, List)
        assert all(isinstance(message, str) for message in message_payload)
        assert mock_logging.info.call_count == 1
        assert mock_logging.info.call_args[0][0] == (
            f"json_converter: Sent {expected_message_count} messages with correlation ids: {ids}"
        )
        assert mock_logging.error.call_count == 0

    @pytest.mark.parametrize(
        "message, exception",
        [
            ("non-serializable object", TypeError()),
            ("another non-serializable object", ValueError()),
        ],
    )
    def test_send_messages_json_dump_failure(
        self, message, exception, mock_output_event_hub_message
    ):
        with patch("json.dumps", side_effect=exception):
            with patch("logging.error") as mock_logging_error:
                json_converter.send_messages([message], mock_output_event_hub_message)
                mock_logging_error.assert_called_once()
                mock_output_event_hub_message.set.assert_not_called()

    def test_send_messages_unexpected_exception(self, mock_output_event_hub_message):
        with patch("json.dumps", side_effect=Exception("Unexpected error")):
            with patch("logging.error") as mock_logging_error:
                json_converter.send_messages(["message"], mock_output_event_hub_message)
                mock_logging_error.assert_called_once()
                mock_output_event_hub_message.set.assert_not_called()

    @patch("shared_code.json_converter.logging.error")
    @patch("shared_code.json_converter.json")
    def test_partial_failure_in_serialization(
        self, mock_json, mock_logging_error, mock_output_event_hub_message
    ):
        messages = ["message1", "message2", "message3"]

        # Define custom behavior for json.dumps
        def side_effect_for_json_dumps(message):
            if message == "message2":
                raise ValueError("Serialization Error")
            return json.dumps(message)

        # mock_json_dumps.side_effect = side_effect_for_json_dumps
        mock_json.JSONDecodeError = json.JSONDecodeError
        mock_json.dumps.side_effect = side_effect_for_json_dumps

        json_converter.send_messages(messages, mock_output_event_hub_message)

        # Check that set is called twice (for message1 and message3)
        assert mock_output_event_hub_message.set.call_count == 1
        assert mock_output_event_hub_message.set.call_args[0][0] == [
            '"message1"',
            '"message3"',
        ]

        # Check that the correct log message is recorded
        mock_logging_error.assert_called_once_with(
            "json_converter: Error serializing message: message2"
        )

    @patch("shared_code.json_converter.logging.error")
    def test_send_messages_output_event_hub_failure(
        self, mock_logging_error, mock_output_event_hub_message
    ):
        # Mock outputEventHubMessage.set to raise an exception
        mock_output_event_hub_message.set.side_effect = Exception(
            "Output Event Hub Failure"
        )

        # Test data
        messages = ["message"]

        # Call the function under test
        json_converter.send_messages(messages, mock_output_event_hub_message)

        # Assertions
        mock_logging_error.assert_called_once()
        assert "Output Event Hub Failure" in mock_logging_error.call_args[0][0]


class TestExtractTopic:
    @pytest.mark.parametrize(
        "messagebody, expected",
        [
            (
                {"topic": "publisher1/something/else"},
                ("publisher1/something/else", "publisher1"),
            ),
            (
                {"topic": "publisher2/anothershortertopic"},
                ("publisher2/anothershortertopic", "publisher2"),
            ),
            # Add more cases as needed
        ],
    )
    def test_extract_topic_success(self, messagebody, expected):
        assert json_converter.extract_topic(messagebody) == expected

    @pytest.mark.parametrize(
        "messagebody",
        [
            ({}),
            ({"not_topic": "value"}),
        ],
    )
    def test_extract_topic_no_topic_key(self, messagebody):
        with pytest.raises(ValueError):
            with patch("logging.error") as mock_logging_error:
                json_converter.extract_topic(messagebody)
                mock_logging_error.assert_called_once()


class TestToList:
    @pytest.mark.parametrize(
        "input, expected",
        [
            (
                ["event1", "event2", "event3"],
                ["event1", "event2", "event3"],
            ),  # list input
            ("single_event", ["single_event"]),  # single non-list input
            (123, [123]),  # numeric input
            (None, [None]),  # None input
            ([], []),  # empty list input
        ],
    )
    def test_to_list(self, input, expected):
        assert json_converter.to_list(input) == expected


class TestConvertJsonToTimeseries:
    @pytest.fixture
    def mock_output_event_hub_message(self):
        return Mock(spec=func.Out)

    @patch("shared_code.json_converter.get_event_as_str")
    @patch("shared_code.json_converter.convert_event")
    @patch("shared_code.json_converter.send_messages")
    def test_convert_json_to_timeseries(
        self,
        mock_send_messages,
        mock_convert_event,
        mock_get_event_as_str,
        mock_output_event_hub_message,
    ):
        # Define custom behavior for mock_get_event_as_str and mock_convert_event
        mock_get_event_as_str.side_effect = (
            lambda event: event
            if isinstance(event, str)
            else event.get_body().decode("utf-8")
        )

        mock_convert_event.side_effect = (
            lambda event: event if "valid" in event else None
        )

        # Create test data
        events = [
            create_eventhub_event("valid_event_1_eventhub_event"),
            "valid_event_2_str",
            create_eventhub_event(
                "missing_event_eventhub_event"
            ),  # This should be filtered out
            "valid_event_3_str",
        ]

        # Call the function under test
        json_converter.convert_json_to_timeseries(events, mock_output_event_hub_message)

        # Assert that send_messages is called correctly
        mock_send_messages.assert_called_once()
        # Extract the first argument passed to send_messages
        sent_messages = mock_send_messages.call_args[0][0]
        # check that we have list[Any] and not list[list[Any]] | None]
        assert isinstance(sent_messages, list)
        assert all(not isinstance(item, list) for item in sent_messages)
        assert all(item is not None for item in sent_messages)
        # check that the correct messages are sent
        assert list(sent_messages) == [
            "valid_event_1_eventhub_event",
            "valid_event_2_str",
            "valid_event_3_str",
        ]

        # Ensure the other functions are called as expected
        assert mock_get_event_as_str.call_count == len(events)
        assert mock_convert_event.call_count == len(events)


class TestConvertEvent:
    @pytest.mark.parametrize(
        "event_str, expected_payload",
        [
            ('{"key": "value"}', {"some": "payload"}),  # successful conversion
            # Add more cases for successful conversion with different inputs
        ],
    )
    @patch("shared_code.json_converter.extract_topic")
    @patch("shared_code.json_converter.send_to_converter")
    def test_convert_event_success(
        self, mock_send_to_converter, mock_extract_topic, event_str, expected_payload
    ):
        mock_extract_topic.return_value = ("topic", "publisher")
        mock_send_to_converter.return_value = expected_payload

        assert json_converter.convert_event(event_str) == expected_payload
        mock_extract_topic.assert_called_once()
        mock_send_to_converter.assert_called_once()

    @pytest.mark.parametrize(
        "event_str",
        [
            '{"invalid json"',  # malformed JSON
            # Add more cases for different types of invalid JSON
        ],
    )
    @patch("shared_code.json_converter.logging.error")
    def test_convert_event_json_error(self, mock_logging_error, event_str):
        assert json_converter.convert_event(event_str) is None
        assert mock_logging_error.call_count == 2

    @pytest.mark.parametrize(
        "event_str",
        [
            '{"key": "value"}',  # valid JSON string
        ],
    )
    @patch("shared_code.json_converter.logging.error")
    @patch("shared_code.json_converter.extract_topic")
    def test_convert_event_extraction_error(
        self, mock_extract_topic, mock_logging_error, event_str
    ):
        mock_extract_topic.side_effect = Exception("Mocked Extraction error")

        assert json_converter.convert_event(event_str) is None
        assert mock_logging_error.call_count == 2
        assert (
            "json_converter.convert_event: Error in event conversion: Mocked Extraction error"
            in mock_logging_error.call_args_list[0][0]
        )

    @pytest.mark.parametrize(
        "event_str",
        [
            '{"key": "value"}',  # valid JSON string
        ],
    )
    @patch("shared_code.json_converter.extract_topic")
    @patch("shared_code.json_converter.logging.error")
    @patch("shared_code.json_converter.send_to_converter")
    def test_convert_event_conversion_error(
        self, mock_send_to_converter, mock_logging_error, mock_extract_topic, event_str
    ):
        mock_send_to_converter.side_effect = Exception("Mock Conversion error")
        mock_extract_topic.return_value = ("topic", "publisher")

        assert json_converter.convert_event(event_str) is None
        assert mock_logging_error.call_count == 2

        assert (
            "json_converter.convert_event: Error in event conversion: Mock Conversion error"
            in mock_logging_error.call_args_list[0][0]
        )
