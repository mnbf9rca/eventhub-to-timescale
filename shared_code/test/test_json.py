import pytest
import json
import datetime
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
        mock = Mock(spec=func.Out)
        return mock

    @pytest.mark.parametrize(
        "messages, expected_message_count",
        [
            (["message1", "message2", "message3"], 3),
            (iter(["message1", "message2", "message3"]), 3),  # testing with an iterator
        ],
    )
    def test_send_messages_success(
        self, messages, expected_message_count, mock_output_event_hub_message
    ):
        json_converter.send_messages(messages, mock_output_event_hub_message)
        assert mock_output_event_hub_message.set.call_count == expected_message_count

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
        assert mock_output_event_hub_message.set.call_count == 2

        # Check that the correct log message is recorded
        mock_logging_error.assert_called_once_with(
            "json_converter: Error serializing message: message2"
        )


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
        sent_messages = mock_send_messages.call_args[0][
            0
        ]  # Extract the first argument passed to send_messages
        assert list(sent_messages) == [
            "valid_event_1_eventhub_event",
            "valid_event_2_str",
            "valid_event_3_str",
        ]

        # Ensure the other functions are called as expected
        assert mock_get_event_as_str.call_count == len(events)
        assert mock_convert_event.call_count == len(events)
