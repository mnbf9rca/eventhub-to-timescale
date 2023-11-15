import pytest
from unittest.mock import patch
from shared_code import homie


class TestGetPayloadType:
    def test_get_payload_type_returns_string(self):
        # Test cases where it should return PayloadType.STRING
        for measurement in ["state", "mode"]:
            assert homie.get_payload_type(measurement) == homie.PayloadType.STRING

    def test_get_payload_type_returns_number(self):
        # Test cases where it should return PayloadType.NUMBER
        for measurement in ["temp", "humidity", "other"]:
            assert homie.get_payload_type(measurement) == homie.PayloadType.NUMBER


class TestGetMeasurementOfAndSubject:
    def test_get_measurement_of_and_subject_normal(self):
        # Test with a normal topic structure
        topic = "home/living_room/temperature"
        measurement_of, measurement_subject = homie.get_measurement_of_and_subject(
            topic
        )
        assert measurement_of == "temperature"
        assert measurement_subject == "living_room"

    def test_get_measurement_of_and_subject_single_part(self):
        # Test with a topic that has only one part
        topic = "temperature"
        with pytest.raises(IndexError):
            homie.get_measurement_of_and_subject(topic)

    def test_get_measurement_of_and_subject_two_parts(self):
        # Test with a topic that has only one part
        topic = "temperature/value"
        measurement_of, measurement_subject = homie.get_measurement_of_and_subject(
            topic
        )
        assert measurement_of == "value"
        assert measurement_subject == "temperature"

    def test_get_measurement_of_and_subject_empty(self):
        # Test with an empty topic
        topic = ""
        with pytest.raises(IndexError):
            homie.get_measurement_of_and_subject(topic)

    def test_get_measurement_of_and_subject_non_string(self):
        # Test with an empty topic
        topic = 7
        with pytest.raises(
            ValueError,
            match=r".*Invalid topic: .*",
        ):
            homie.get_measurement_of_and_subject(topic)

    def test_get_measurement_of_and_subject_none(self):
        # Test with an empty topic
        with pytest.raises(
            ValueError,
            match=r".*Invalid topic: .*",
        ):
            homie.get_measurement_of_and_subject(None)


class TestHomieToTimescale:
    @patch("shared_code.homie.get_events_of_interest")
    @patch("shared_code.homie.get_measurement_of_and_subject")
    @patch("shared_code.homie.validate_publisher")
    @patch("shared_code.homie.validate_message_body_type_and_keys")
    @patch("shared_code.homie.create_atomic_record")
    @patch("shared_code.homie.to_datetime_string")
    @patch("shared_code.homie.get_payload_type")
    @patch("shared_code.homie.create_correlation_id")
    def test_homie_to_timescale_interest(
        self,
        mock_create_correlation_id,
        mock_get_payload_type,
        mock_to_datetime_string,
        mock_create_atomic_record,
        mock_validate_message_body_type_and_keys,
        mock_validate_publisher,
        mock_get_measurement_of_and_subject,
        mock_get_events_of_interest,
    ):
        # Setup
        topic = "topic-root/topic-subject"
        payload = {"timestamp": "123456", "payload": "22.5"}
        publisher = "homie"
        mock_get_events_of_interest.return_value = ["test_measurement_of"]
        mock_get_measurement_of_and_subject.return_value = (
            "test_measurement_of",
            "measurement_subject_return",
        )

        # Call the function
        homie.homie_to_timescale(payload, topic, publisher)

        # Asserts
        mock_validate_publisher.assert_called_once_with(publisher, "homie")
        mock_validate_message_body_type_and_keys.assert_called_once_with(
            payload, publisher, ["timestamp"]
        )
        mock_get_measurement_of_and_subject.assert_called_once_with(topic)
        mock_to_datetime_string.assert_called_once_with("123456")
        mock_get_payload_type.assert_called_once_with("test_measurement_of")
        mock_create_correlation_id.assert_called_once()
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp=mock_to_datetime_string.return_value,
            measurement_subject="measurement_subject_return",
            measurement_publisher=publisher,
            measurement_of="test_measurement_of",
            measurement_value="22.5",
            measurement_data_type=mock_get_payload_type.return_value,
            correlation_id=mock_create_correlation_id.return_value,
        )

    @patch("shared_code.homie.get_events_of_interest")
    @patch("shared_code.homie.get_measurement_of_and_subject")
    @patch("shared_code.homie.validate_publisher")
    @patch("shared_code.homie.validate_message_body_type_and_keys")
    @patch("shared_code.homie.create_atomic_record")
    def test_homie_to_timescale_not_interest(
        self,
        mock_create_atomic_record,
        mock_validate_message_body_type_and_keys,
        mock_validate_publisher,
        mock_get_measurement_of_and_subject,
        mock_get_events_of_interest,
    ):
        # Setup
        topic = "unknown-root/unknown-measure"
        mock_get_events_of_interest.return_value = ["test_measurement_of"]
        mock_get_measurement_of_and_subject.return_value = (
            "unknown-measure",
            "unknown-root",
        )
        payload = {"timestamp": "123456", "payload": "22.5"}
        publisher = "homie"
        # Call the function and assert
        result = homie.homie_to_timescale(payload, topic, publisher)
        assert result is None
        mock_validate_publisher.assert_called_once_with(publisher, "homie")
        mock_validate_message_body_type_and_keys.assert_called_once_with(
            payload, publisher, ["timestamp"]
        )
        mock_get_measurement_of_and_subject.assert_called_once_with(topic)
        mock_create_atomic_record.assert_not_called()


class TestGetEventsOfInterest:
    def test_get_events_of_interest(self):
        # Test that the events of interest are as expected
        assert homie.get_events_of_interest() == [
            "measure-temperature",
            "heating-setpoint",
            "state",
            "mode",
            "thermostat-setpoint",
        ]
