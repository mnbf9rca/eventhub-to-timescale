import pytest
import json
from shared_code import glow
from unittest.mock import patch


class TestParseMessagePayload:
    def test_valid_messagebody_and_subject(self):
        messagebody = {
            "payload": json.dumps(
                {"electricitymeter": {"timestamp": "2022-01-01T12:34:56Z"}}
            )
        }
        measurement_subject = "electricitymeter"
        expected_payload = {"electricitymeter": {"timestamp": "2022-01-01T12:34:56Z"}}
        expected_timestamp = "2022-01-01T12:34:56.000000Z"
        payload, timestamp = glow.parse_message_payload(
            messagebody, measurement_subject
        )
        assert payload == expected_payload
        assert timestamp == expected_timestamp

    def test_missing_payload(self):
        messagebody = {}
        measurement_subject = "electricitymeter"
        with pytest.raises(KeyError):
            glow.parse_message_payload(messagebody, measurement_subject)

    def test_missing_measurement_subject_in_payload(self):
        messagebody = {
            "payload": json.dumps({"gasmeter": {"timestamp": "2022-01-01T12:34:56Z"}})
        }
        measurement_subject = "electricitymeter"
        with pytest.raises(KeyError):
            glow.parse_message_payload(messagebody, measurement_subject)

    def test_missing_timestamp_in_subject(self):
        messagebody = {"payload": json.dumps({"electricitymeter": {}})}
        measurement_subject = "electricitymeter"
        with pytest.raises(KeyError):
            glow.parse_message_payload(messagebody, measurement_subject)


class TestGetIgnoreKeys:
    def test_get_ignore_keys(self):
        assert glow.get_ignore_keys() == [
            "units",
            "mpan",
            "mprn",
            "supplier",
            "dayweekmonthvolunits",
            "cumulativevolunits",
        ]


class TestProcessMeasurementSubject:
    @pytest.fixture
    def mock_create_record_recursive(self):
        with patch("shared_code.glow.create_record_recursive") as mock:
            # Simulate return values for create_record_recursive
            mock.side_effect = lambda payload, records, **kwargs: records + [
                "mocked_record"
            ]
            yield mock

    @pytest.mark.parametrize(
        "measurement_subject, expected_calls",
        [("electricitymeter", 2), ("gasmeter", 1), ("unknownmeter", 0)],
    )
    @patch("shared_code.glow.get_ignore_keys")
    def test_process_measurement_subject(
        self,
        mock_get_ignore_keys,
        mock_create_record_recursive,
        measurement_subject,
        expected_calls,
    ):
        mock_get_ignore_keys.return_value = ["some_keys"]
        message_payload = {
            "electricitymeter": {
                "energy": {"import": {"some": "data"}},
                "power": {"other": "data"},
            },
            "gasmeter": {"energy": {"import": {"some": "data"}}},
        }
        timestamp = "2023-01-01T00:00:00Z"
        correlation_id = "test_correlation_id"
        publisher = "glow"

        actual_result = glow.process_measurement_subject(
            message_payload, timestamp, correlation_id, publisher, measurement_subject
        )

        assert mock_create_record_recursive.call_count == expected_calls
        assert actual_result == ["mocked_record"] * expected_calls

        if measurement_subject in message_payload:
            energy_call_args = mock_create_record_recursive.call_args_list[0]
            assert energy_call_args == (
                {
                    "payload": message_payload[measurement_subject]["energy"]["import"],
                    "records": [],
                    "timestamp": timestamp,
                    "correlation_id": correlation_id,
                    "measurement_publisher": publisher,
                    "measurement_subject": measurement_subject,
                    "ignore_keys": mock_get_ignore_keys.return_value,
                    "measurement_of_prefix": "import",
                },
            )

            if measurement_subject == "electricitymeter":
                power_call_args = mock_create_record_recursive.call_args_list[1]
                assert power_call_args == (
                    {
                        "payload": message_payload[measurement_subject]["power"],
                        "records": ["mocked_record"]
                        * (expected_calls - 1),  # records from the first call
                        "timestamp": timestamp,
                        "correlation_id": correlation_id,
                        "measurement_publisher": publisher,
                        "measurement_subject": measurement_subject,
                        "ignore_keys": mock_get_ignore_keys.return_value,
                        "measurement_of_prefix": "power",
                    },
                )

    def test_invalid_measurement_subject(self, mock_create_record_recursive):
        message_payload = {"valid_subject": {"some": "data"}}
        invalid_subject = "invalid_subject"
        records = glow.process_measurement_subject(
            message_payload,
            "2023-01-01T00:00:00Z",
            "test_correlation_id",
            "glow",
            invalid_subject,
        )

        assert records == []
        mock_create_record_recursive.assert_not_called()


class TestGlowToTimescale:
    @patch("shared_code.glow.process_measurement_subject")
    @patch("shared_code.glow.create_correlation_id")
    @patch("shared_code.glow.parse_message_payload")
    @patch("shared_code.glow.is_topic_of_interest")
    @patch("shared_code.glow.validate_message_body_type_and_keys")
    @patch("shared_code.glow.validate_publisher")
    def test_glow_to_timescale_valid_subject(
        self,
        mock_validate_publisher,
        mock_validate_message_body_type_and_keys,
        mock_is_topic_of_interest,
        mock_parse_message_payload,
        mock_create_correlation_id,
        mock_process_measurement_subject,
    ):
        # Set up return values for the mocks
        mock_is_topic_of_interest.return_value = "electricitymeter"
        mock_parse_message_payload.return_value = ("mocked_payload", "mocked_timestamp")
        mock_create_correlation_id.return_value = "mocked_correlation_id"
        mock_process_measurement_subject.return_value = ["mocked_record"]

        messagebody = {"some_key": "some_value"}
        topic = "some/topic"
        publisher = "glow"

        # Call the function under test
        records = glow.glow_to_timescale(messagebody, topic, publisher)

        # Assertions to verify the correct calls were made to the mocks
        mock_validate_publisher.assert_called_once_with(publisher, "glow")
        mock_validate_message_body_type_and_keys.assert_called_once_with(
            messagebody, "glow"
        )
        mock_is_topic_of_interest.assert_called_once_with(
            topic, ["electricitymeter", "gasmeter"]
        )
        mock_parse_message_payload.assert_called_once_with(
            messagebody, "electricitymeter"
        )
        mock_create_correlation_id.assert_called_once()
        mock_process_measurement_subject.assert_called_once_with(
            "mocked_payload",
            "mocked_timestamp",
            "mocked_correlation_id",
            publisher,
            "electricitymeter",
        )

        # Verify the return value
        assert records == ["mocked_record"]

    @patch("shared_code.glow.process_measurement_subject")
    @patch("shared_code.glow.create_correlation_id")
    @patch("shared_code.glow.parse_message_payload")
    @patch("shared_code.glow.is_topic_of_interest")
    @patch("shared_code.glow.validate_message_body_type_and_keys")
    @patch("shared_code.glow.validate_publisher")
    def test_glow_to_timescale_invalid_subject(
        self,
        mock_validate_publisher,
        mock_validate_message_body_type_and_keys,
        mock_is_topic_of_interest,
        mock_parse_message_payload,
        mock_create_correlation_id,
        mock_process_measurement_subject,
    ):
        # Set up mocks for invalid subject case
        mock_is_topic_of_interest.return_value = None

        messagebody = {"some_key": "some_value"}
        topic = "some/topic"
        publisher = "glow"

        # Call the function under test
        result = glow.glow_to_timescale(messagebody, topic, publisher)

        # Validate that the function returned early
        assert result is None or result == []

        # Validate that no further processing was done
        mock_parse_message_payload.assert_not_called()
        mock_create_correlation_id.assert_not_called()
        mock_process_measurement_subject.assert_not_called()
