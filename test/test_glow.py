import pytest
import json
from shared_code.glow import validate_publisher_and_topic, parse_message_payload, create_records_for_subject


class TestValidatePublisherAndTopic:
    def test_valid_publisher_and_topic(self):
        publisher = 'Glow'
        topic = 'some/valid/electricitymeter'
        assert validate_publisher_and_topic(publisher, topic) == 'electricitymeter'

    def test_valid_publisher_case_insensitive_and_topic(self):
        publisher = 'gLoW'
        topic = 'some/valid/gasmeter'
        assert validate_publisher_and_topic(publisher, topic) == 'gasmeter'

    def test_invalid_publisher(self):
        publisher = 'NotGlow'
        topic = 'some/valid/electricitymeter'
        with pytest.raises(ValueError) as e:
            validate_publisher_and_topic(publisher, topic)
        assert str(e.value) == 'Invalid publisher: Glow processor only handles Glow messages, not NotGlow'

    def test_invalid_topic(self):
        publisher = 'Glow'
        topic = 'some/invalid/invalidsubject'
        assert validate_publisher_and_topic(publisher, topic) is None

    def test_missing_measurement_subject(self):
        publisher = 'Glow'
        topic = 'some/invalid/'
        assert validate_publisher_and_topic(publisher, topic) is None

    def test_valid_publisher_and_topic_but_not_of_interest(self):
        publisher = 'Glow'
        topic = 'some/valid/notofinterest'
        assert validate_publisher_and_topic(publisher, topic) is None


class TestParseMessagePayload:

    def test_valid_messagebody_and_subject(self):
        messagebody = {'payload': json.dumps({'electricitymeter': {'timestamp': '2022-01-01T12:34:56Z'}})}
        measurement_subject = 'electricitymeter'
        expected_payload = {'electricitymeter': {'timestamp': '2022-01-01T12:34:56Z'}}
        expected_timestamp = '2022-01-01T12:34:56.000000Z'  # Replace with the actual datetime object if to_datetime converts it
        payload, timestamp = parse_message_payload(messagebody, measurement_subject)
        assert payload == expected_payload
        assert timestamp == expected_timestamp

    def test_missing_payload(self):
        messagebody = {}
        measurement_subject = 'electricitymeter'
        with pytest.raises(KeyError):
            parse_message_payload(messagebody, measurement_subject)

    def test_missing_measurement_subject_in_payload(self):
        messagebody = {'payload': json.dumps({'gasmeter': {'timestamp': '2022-01-01T12:34:56Z'}})}
        measurement_subject = 'electricitymeter'
        with pytest.raises(KeyError):
            parse_message_payload(messagebody, measurement_subject)

    def test_missing_timestamp_in_subject(self):
        messagebody = {'payload': json.dumps({'electricitymeter': {}})}
        measurement_subject = 'electricitymeter'
        with pytest.raises(KeyError):
            parse_message_payload(messagebody, measurement_subject)

class TestCreateRecordsForSubject:

    def test_all_valid_parameters(self):
        message_payload = {
            'electricitymeter': {
                'energy': {'import': {'value': 100}},
                'power': {'value': 200}
            }
        }
        timestamp = '2022-01-01T12:34:56Z'
        correlation_id = 'some_id'
        publisher = 'Glow'
        measurement_subject = 'electricitymeter'
        records = []
        expected_records = [
            {'timestamp': timestamp, 'correlation_id': correlation_id, 'measurement_publisher': publisher,
            'measurement_subject': measurement_subject, 'measurement_of': 'import_value', 'measurement_value': 100, 
            'measurement_data_type': 'number'},
            {'timestamp': timestamp, 'correlation_id': correlation_id, 'measurement_publisher': publisher,
            'measurement_subject': measurement_subject, 'measurement_of': 'power_value', 'measurement_value': 200, 
            'measurement_data_type': 'number'}
        ]

        output_records = create_records_for_subject(
            message_payload, timestamp, correlation_id, publisher, measurement_subject, records)
        assert output_records == expected_records

    def test_missing_measurement_subject_in_payload(self):
        message_payload = {}
        timestamp = '2022-01-01T12:34:56Z'
        correlation_id = 'some_id'
        publisher = 'Glow'
        measurement_subject = 'electricitymeter'
        records = []
        expected_records = []
        output_records = create_records_for_subject(
            message_payload, timestamp, correlation_id, publisher, measurement_subject, records)
        assert output_records == expected_records

    def test_ignore_keys(self):
        message_payload = {
            'electricitymeter': {
                'energy': {'import': {'value': 100, 'units': 'kW'}},
                'power': {'value': 200}
            }
        }
        timestamp = '2022-01-01T12:34:56Z'
        correlation_id = 'some_id'
        publisher = 'Glow'
        measurement_subject = 'electricitymeter'
        records = []
        expected_records = [
            {'timestamp': '2022-01-01T12:34:56Z', 'correlation_id': 'some_id', 
            'measurement_publisher': 'Glow', 'measurement_subject': 'electricitymeter',
            'measurement_of': 'import_value', 'measurement_value': 100, 'measurement_data_type': 'number'},
            {'timestamp': '2022-01-01T12:34:56Z', 'correlation_id': 'some_id', 
            'measurement_publisher': 'Glow', 'measurement_subject': 'electricitymeter',
            'measurement_of': 'power_value', 'measurement_value': 200, 'measurement_data_type': 'number'}
        ]

        output_records = create_records_for_subject(
            message_payload, timestamp, correlation_id, publisher, measurement_subject, records)
        assert output_records == expected_records

    def test_electricitymeter_with_both_import_and_power(self):
        message_payload = {
            'electricitymeter': {
                'energy': {'import': {'value': 100}},
                'power': {'value': 200}
            }
        }
        timestamp = '2022-01-01T12:34:56Z'
        correlation_id = 'some_id'
        publisher = 'Glow'
        measurement_subject = 'electricitymeter'
        records = []
        expected_records = [
            {'timestamp': '2022-01-01T12:34:56Z', 'correlation_id': 'some_id', 
            'measurement_publisher': 'Glow', 'measurement_subject': 'electricitymeter',
            'measurement_of': 'import_value', 'measurement_value': 100, 'measurement_data_type': 'number'},
            {'timestamp': '2022-01-01T12:34:56Z', 'correlation_id': 'some_id', 
            'measurement_publisher': 'Glow', 'measurement_subject': 'electricitymeter',
            'measurement_of': 'power_value', 'measurement_value': 200, 'measurement_data_type': 'number'}
        ]

        output_records = create_records_for_subject(
            message_payload, timestamp, correlation_id, publisher, measurement_subject, records)
        assert output_records == expected_records