from unittest import TestCase
import pytest
import pytest_mock
import datetime
import json
from azure.functions import EventHubEvent

from get_test_data import create_event_hub_event, load_test_data, recursive_json_parser


class Test_recursive_json_parser:
    def test_recursive_json_parser_with_valid_json(self):
        actual_value = recursive_json_parser('{"a": 1}')
        assert actual_value == {"a": 1}

    def test_recursive_json_parser_with_invalid_json(self):
        actual_value = recursive_json_parser('{"a": 1')
        assert actual_value == '{"a": 1'

    def test_recursive_json_parser_with_none(self):
        actual_value = recursive_json_parser(None)
        assert actual_value is None

    def test_recursive_json_parser_with_empty_string(self):
        actual_value = recursive_json_parser("")
        assert actual_value == ""

    def test_recursive_json_parser_with_nested_object(self):
        actual_value = recursive_json_parser('{"a": {"b": 1}}')
        assert actual_value == {"a": {"b": 1}}

    def test_recursive_json_parser_with_nested_array(self):
        actual_value = recursive_json_parser('{"a": [{"b": 1}]}')
        assert actual_value == {"a": [{"b": 1}]}

    def test_recursive_json_parser_with_nested_array_and_object(self):
        actual_value = recursive_json_parser('{"a": [{"b": 1}, {"c": 2}]}')
        assert actual_value == {"a": [{"b": 1}, {"c": 2}]}

    def test_recursive_json_parser_with_nested_array_and_object_and_string(self):
        actual_value = recursive_json_parser('{"a": [{"b": 1}, {"c": 2}, "d"]}')
        assert actual_value == {"a": [{"b": 1}, {"c": 2}, "d"]}


class Test_create_event_hub_event:
    def test_create_event_hub_event_with_valid_dict(self):
        sample_event = {
            "body": '{"c": 3}',
            "trigger_metadata": {"a": "1", "b": "2"},
            "enqueued_time": "2020-01-01T00:00:00.000Z",
            "partition_key": "partition_key_123",
            "sequence_number": "789",
            "offset": "456",
            "iothub_metadata": {"d": "4", "e": "5"},
        }
        expected_value = EventHubEvent(
            body=sample_event["body"].encode("utf-8"),
            trigger_metadata=sample_event["trigger_metadata"],
            enqueued_time=datetime.datetime(
                2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            partition_key=sample_event["partition_key"],
            sequence_number=sample_event["sequence_number"],
            offset=sample_event["offset"],
            iothub_metadata=sample_event["iothub_metadata"],
        )
        actual_value = create_event_hub_event(sample_event)

        assert actual_value.partition_key == expected_value.partition_key
        assert actual_value.enqueued_time == expected_value.enqueued_time
        assert actual_value.offset == expected_value.offset
        assert actual_value.sequence_number == expected_value.sequence_number
        TestCase().assertDictEqual(
            actual_value.iothub_metadata, expected_value.iothub_metadata
        )
        TestCase().assertDictEqual(
            json.loads(actual_value.get_body().decode("UTF-8")),
            json.loads(actual_value.get_body().decode("UTF-8")),
        )

    def test_create_event_hub_event_with_empty_body(self):
        sample_event = {
            "trigger_metadata": {"a": "1", "b": "2"},
            "enqueued_time": "2020-01-01T00:00:00.000Z",
            "partition_key": "partition_key_123",
            "sequence_number": "789",
            "offset": "456",
            "iothub_metadata": {"d": "4", "e": "5"},
        }
        expected_value = EventHubEvent(
            body=None,
            trigger_metadata=sample_event["trigger_metadata"],
            enqueued_time=datetime.datetime(
                2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            partition_key=sample_event["partition_key"],
            sequence_number=sample_event["sequence_number"],
            offset=sample_event["offset"],
            iothub_metadata=sample_event["iothub_metadata"],
        )
        actual_value = create_event_hub_event(sample_event)

        assert actual_value.partition_key == expected_value.partition_key
        assert actual_value.enqueued_time == expected_value.enqueued_time
        assert actual_value.offset == expected_value.offset
        assert actual_value.sequence_number == expected_value.sequence_number
        TestCase().assertDictEqual(
            actual_value.iothub_metadata, expected_value.iothub_metadata
        )
        assert actual_value.get_body() == expected_value.get_body()

    def test_create_event_hub_event_with_empty_dict_as_body(self):
        sample_event = {
            "body": {},
            "trigger_metadata": {"a": "1", "b": "2"},
            "enqueued_time": "2020-01-01T00:00:00.000Z",
            "partition_key": "partition_key_123",
            "sequence_number": "789",
            "offset": "456",
            "iothub_metadata": {"d": "4", "e": "5"},
        }
        expected_value = EventHubEvent(
            body={},
            trigger_metadata=sample_event["trigger_metadata"],
            enqueued_time=datetime.datetime(
                2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc
            ),
            partition_key=sample_event["partition_key"],
            sequence_number=sample_event["sequence_number"],
            offset=sample_event["offset"],
            iothub_metadata=sample_event["iothub_metadata"],
        )
        actual_value = create_event_hub_event(sample_event)

        assert actual_value.partition_key == expected_value.partition_key
        assert actual_value.enqueued_time == expected_value.enqueued_time
        assert actual_value.offset == expected_value.offset
        assert actual_value.sequence_number == expected_value.sequence_number
        TestCase().assertDictEqual(
            actual_value.iothub_metadata, expected_value.iothub_metadata
        )
        TestCase().assertDictEqual(actual_value.get_body(), expected_value.get_body())

    def test_create_event_hub_event_with_missing_timestamp(self):
        sample_event = {
            "body": '{"c": 3}',
            "trigger_metadata": {"a": "1", "b": "2"},
            "partition_key": "partition_key_123",
            "sequence_number": "789",
            "offset": "456",
            "iothub_metadata": {"d": "4", "e": "5"},
        }
        expected_value = EventHubEvent(
            body='{"c": 3}'.encode("utf-8"),
            trigger_metadata=sample_event["trigger_metadata"],
            enqueued_time=datetime.datetime.now(),
            partition_key=sample_event["partition_key"],
            sequence_number=sample_event["sequence_number"],
            offset=sample_event["offset"],
            iothub_metadata=sample_event["iothub_metadata"],
        )
        actual_value = create_event_hub_event(sample_event)

        assert actual_value.partition_key == expected_value.partition_key
        # Allow for a 500 microsecond difference in the timestamp
        assert (
            abs(actual_value.enqueued_time - expected_value.enqueued_time)
        ) < datetime.timedelta(microseconds=500)
        assert actual_value.offset == expected_value.offset
        assert actual_value.sequence_number == expected_value.sequence_number
        TestCase().assertDictEqual(
            actual_value.iothub_metadata, expected_value.iothub_metadata
        )
        TestCase().assertDictEqual(
            json.loads(actual_value.get_body().decode("UTF-8")),
            json.loads(actual_value.get_body().decode("UTF-8")),
        )

    def test_create_event_hub_event_with_missing_sequence_throws_error(self):
        sample_event = {
            "body": '{"c": 3}',
            "trigger_metadata": {"a": "1", "b": "2"},
            "enqueued_time": "2020-01-01T00:00:00.000Z",
            "partition_key": "partition_key_123",
            "offset": "456",
            "iothub_metadata": {"d": "4", "e": "5"},
        }
        with pytest.raises(KeyError):
            create_event_hub_event(sample_event)

    def test_create_event_hub_event_with_missing_offset_throws_error(self):
        sample_event = {
            "body": '{"c": 3}',
            "trigger_metadata": {"a": "1", "b": "2"},
            "enqueued_time": "2020-01-01T00:00:00.000Z",
            "partition_key": "partition_key_123",
            "sequence_number": "789",
            "iothub_metadata": {"d": "4", "e": "5"},
        }
        with pytest.raises(KeyError):
            create_event_hub_event(sample_event)

    def test_create_event_hub_event_with_only_body_offset_sequence_number(self):
        sample_event = {
            "body": '{"c": 3}',
            "offset": "456",
            "sequence_number": "789",
        }
        expected_value = EventHubEvent(
            body='{"c": 3}'.encode("utf-8"),
            trigger_metadata={},
            enqueued_time=datetime.datetime.now(),
            partition_key=None,
            sequence_number=sample_event["sequence_number"],
            offset=sample_event["offset"],
            iothub_metadata={},
        )
        actual_value = create_event_hub_event(sample_event)

        # Allow for a 500 microsecond difference in the timestamp
        assert (
            abs(actual_value.enqueued_time - expected_value.enqueued_time)
        ) < datetime.timedelta(microseconds=500)
        assert actual_value.offset == expected_value.offset
        assert actual_value.sequence_number == expected_value.sequence_number
        TestCase().assertDictEqual(
            actual_value.iothub_metadata, expected_value.iothub_metadata
        )
        TestCase().assertDictEqual(
            json.loads(actual_value.get_body().decode("UTF-8")),
            json.loads(actual_value.get_body().decode("UTF-8")),
        )


class Test_load_test_data:
    def test_load_test_data_returns_dict(self, mocker: pytest_mock.MockerFixture):
        test_data = {"a": 1, "b": 2}
        mocked_open = mocker.mock_open(read_data=json.dumps(test_data))
        builtin_open = "builtins.open"
        mocker.patch(builtin_open, mocked_open)
        actual_value = load_test_data()
        assert isinstance(actual_value, dict)
        TestCase().assertDictEqual(actual_value, test_data)

    def test_load_test_data_throws_error_if_file_not_found(
        self, mocker: pytest_mock.MockerFixture
    ):
        mocked_open = mocker.mock_open()
        builtin_open = "builtins.open"
        mocker.patch(builtin_open, mocked_open)
        mocked_open.side_effect = FileNotFoundError
        with pytest.raises(FileNotFoundError):
            load_test_data()

    def test_load_test_data_returns_dict_for_doubly_dumped_json(
        self, mocker: pytest_mock.MockerFixture
    ):
        sub_test_data = {"d": 3, "e": 4}
        test_data = {"a": 1, "b": 2, "c": json.dumps(sub_test_data)}

        mocked_open = mocker.mock_open(read_data=json.dumps(test_data))
        builtin_open = "builtins.open"
        mocker.patch(builtin_open, mocked_open)
        expected_value = {"a": 1, "b": 2, "c": {"d": 3, "e": 4}}
        actual_value = load_test_data()
        assert isinstance(actual_value, dict)
        TestCase().assertDictEqual(actual_value, expected_value)
