import pytest
import json
from shared_code.glow import (
    validate_publisher_and_topic,
    parse_message_payload,
    create_records_for_subject,
)


class TestValidatePublisherAndTopic:
    # For Valid Tests
    @pytest.mark.parametrize(
        "publisher, topic, expected",
        [
            ("Glow", "some/valid/electricitymeter", "electricitymeter"),
            ("gLoW", "some/valid/gasmeter", "gasmeter"),
            ("Glow", "some/invalid/invalidsubject", None),
            ("Glow", "some/invalid/", None),
            ("Glow", "some/valid/notofinterest", None),
        ],
    )
    def test_validate_publisher_and_topic_valid(self, publisher, topic, expected):
        assert validate_publisher_and_topic(publisher, topic) == expected

    # For Invalid Tests
    @pytest.mark.parametrize(
        "publisher, topic, expected_error, expected_message",
        [
            (
                "NotGlow",
                "some/valid/electricitymeter",
                ValueError,
                "Invalid publisher: Glow processor only handles Glow messages, not NotGlow",
            ),
        ],
    )
    def test_validate_publisher_and_topic_invalid(
        self, publisher, topic, expected_error, expected_message
    ):
        with pytest.raises(expected_error) as e:
            validate_publisher_and_topic(publisher, topic)
        assert str(e.value) == expected_message


class TestParseMessagePayload:
    def test_valid_messagebody_and_subject(self):
        messagebody = {
            "payload": json.dumps(
                {"electricitymeter": {"timestamp": "2022-01-01T12:34:56Z"}}
            )
        }
        measurement_subject = "electricitymeter"
        expected_payload = {"electricitymeter": {"timestamp": "2022-01-01T12:34:56Z"}}
        expected_timestamp = "2022-01-01T12:34:56.000000Z"  # Replace with the actual datetime object if to_datetime converts it
        payload, timestamp = parse_message_payload(messagebody, measurement_subject)
        assert payload == expected_payload
        assert timestamp == expected_timestamp

    def test_missing_payload(self):
        messagebody = {}
        measurement_subject = "electricitymeter"
        with pytest.raises(KeyError):
            parse_message_payload(messagebody, measurement_subject)

    def test_missing_measurement_subject_in_payload(self):
        messagebody = {
            "payload": json.dumps({"gasmeter": {"timestamp": "2022-01-01T12:34:56Z"}})
        }
        measurement_subject = "electricitymeter"
        with pytest.raises(KeyError):
            parse_message_payload(messagebody, measurement_subject)

    def test_missing_timestamp_in_subject(self):
        messagebody = {"payload": json.dumps({"electricitymeter": {}})}
        measurement_subject = "electricitymeter"
        with pytest.raises(KeyError):
            parse_message_payload(messagebody, measurement_subject)


class TestCreateRecordsForSubject:
    
    @pytest.mark.parametrize(
        "message_payload, timestamp, correlation_id, publisher, measurement_subject, records, expected_records",
        [
            (
                {
                    "electricitymeter": {
                        "energy": {"import": {"value": 100}},
                        "power": {"value": 200},
                    }
                },
                "2022-01-01T12:34:56Z",
                "some_id",
                "Glow",
                "electricitymeter",
                [],
                [
                    {
                        "timestamp": "2022-01-01T12:34:56Z",
                        "correlation_id": "some_id",
                        "measurement_publisher": "Glow",
                        "measurement_subject": "electricitymeter",
                        "measurement_of": "import_value",
                        "measurement_value": 100,
                        "measurement_data_type": "number",
                    },
                    {
                        "timestamp": "2022-01-01T12:34:56Z",
                        "correlation_id": "some_id",
                        "measurement_publisher": "Glow",
                        "measurement_subject": "electricitymeter",
                        "measurement_of": "power_value",
                        "measurement_value": 200,
                        "measurement_data_type": "number",
                    },
                ],
            ),
            (
                {},  # Empty payload
                "2022-01-01T12:34:56Z",
                "some_id",
                "Glow",
                "electricitymeter",
                [],
                [],
            ),
            (
                {
                    "electricitymeter": {
                        "energy": {"import": {"value": 100, "units": "kW"}},
                        "power": {"value": 200},
                    }
                },
                "2022-01-01T12:34:56Z",
                "some_id",
                "Glow",
                "electricitymeter",
                [],
                [
                    {
                        "timestamp": "2022-01-01T12:34:56Z",
                        "correlation_id": "some_id",
                        "measurement_publisher": "Glow",
                        "measurement_subject": "electricitymeter",
                        "measurement_of": "import_value",
                        "measurement_value": 100,
                        "measurement_data_type": "number",
                    },
                    {
                        "timestamp": "2022-01-01T12:34:56Z",
                        "correlation_id": "some_id",
                        "measurement_publisher": "Glow",
                        "measurement_subject": "electricitymeter",
                        "measurement_of": "power_value",
                        "measurement_value": 200,
                        "measurement_data_type": "number",
                    },
                ],
            ),
        ],
    )
    def test_create_records_for_subject(
        self, 
        message_payload, 
        timestamp, 
        correlation_id, 
        publisher, 
        measurement_subject, 
        records, 
        expected_records
    ):
        output_records = create_records_for_subject(
            message_payload,
            timestamp,
            correlation_id,
            publisher,
            measurement_subject,
            records,
        )
        assert output_records == expected_records
