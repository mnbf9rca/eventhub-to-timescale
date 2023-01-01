import os
import sys
from typing import Any, List
from unittest import TestCase
import pytest
from get_test_data import create_event_hub_event, load_test_data
from azure.functions import EventHubEvent
from datetime import datetime, timezone

import json
from jsonschema import validate


# add the shared_code directory to the path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shared_code import (  # noqa E402
    emon_to_timescale,
    is_topic_of_interest,
    glow_to_timescale,
    homie_to_timescale,
    get_record_type,
    PayloadType,
    create_record_recursive,
    to_datetime,
    create_correlation_id,
    recursively_deserialize,
)

# load the schema
schema_path = os.sep.join([SCRIPT_DIR, "timeseries.json"])
with open(schema_path) as f:
    schema = json.load(f)

# import test data
test_data = load_test_data()


def sc_test_extract_topic(messagebody: dict) -> tuple[str, str]:
    """(in test_shared_code)
    Extract the topic and publisher from the message body
    @param messagebody: the message body
    @return: the topic and publisher
    """
    topic: str = messagebody["topic"]
    # the publisher is the first characters to the left of the first /
    publisher = topic.split("/")[0]
    return topic, publisher


def call_converter(
    converter: str,
    test_data_item: dict[str, Any],
    override_publisher: str = None
) -> List[dict[str, Any]]:
    """Call the converter with the test data item
    @param converter: the converter to call - homie, emon, glow
    @param test_data_item: the test data item to use
    @return: the result of the converter
    """
    test_event = create_event_hub_event(test_data_item["properties"])
    messagebody = test_data_item["properties"]["body"]
    # o_messagebody: dict = json.loads(messagebody)
    topic, publisher = sc_test_extract_topic(messagebody)
    publisher_to_send = override_publisher or publisher
    if converter == "emon":
        return emon_to_timescale(test_event, messagebody, topic, publisher_to_send)
    elif converter == "glow":
        return glow_to_timescale(test_event, messagebody, topic, publisher_to_send)
    elif converter == "homie":
        return homie_to_timescale(test_event, messagebody, topic, publisher_to_send)
    else:
        raise ValueError(f"Unknown converter {converter}")


def assert_valid_schema(data: dict, schema: dict):
    """Checks whether the given data matches the schema"""

    return validate(data, schema)


class Test_Converter_Methods:
    class Test_Emon:
        def test_with_ignored_key(self):
            actual_value = call_converter("emon", test_data["emon_ignored"])
            expected_value = test_data["emon_ignored"]["expected"]
            assert expected_value is None
            assert actual_value is None

        def test_with_emonTx4_key(self):
            test_object: dict = test_data["emontx4_json"]
            actual_value = call_converter("emon", test_object)
            expected_value = test_object["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)
            assert_valid_schema(actual_value, schema)

        def test_ignored_publisher(self):
            test_object: dict = test_data["emontx4_json"]
            with pytest.raises(ValueError) as e:
                call_converter("emon", test_object, "incorrect_publisher")
            assert "incorrect_publisher" in str(e.value)
            assert "emon processor only handles emon messages" in str(e.value).lower()



    class Test_Glow:
        def test_glow_to_timescale_with_valid_json_for_electricity_meter(self):
            actual_value = call_converter("glow", test_data["glow_electricitymeter"])
            expected_value = test_data["glow_electricitymeter"]["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)
            assert_valid_schema(actual_value, schema)

        def test_glow_to_timescale_with_valid_json_for_gas_meter(self):
            actual_value = call_converter("glow", test_data["glow_gasmeter"])
            expected_value = test_data["glow_gasmeter"]["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)
            assert_valid_schema(actual_value, schema)

        def test_ignored_publisher(self):
            test_object: dict = test_data["glow_gasmeter"]
            with pytest.raises(ValueError) as e:
                call_converter("glow", test_object, "incorrect_publisher")
            assert "incorrect_publisher" in str(e.value)
            assert "glow processor only handles glow messages" in str(e.value).lower()

        def test_glow_to_timescale_with_item_to_ignored_measurement(self):
            actual_value = call_converter("glow", test_data["glow_ignored"])
            expected_value = test_data["homie_heartbeat"]["expected"]  # None
            assert expected_value is None
            assert actual_value is None

    class Test_Homie:
        def test_homie_to_timescale_with_valid_json_for_mode(self):
            actual_value = call_converter("homie", test_data["homie_mode"])
            expected_value = test_data["homie_mode"]["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)
            assert_valid_schema(actual_value, schema)

        def test_homie_to_timescale_with_ignored_json_for_heartbeat(self):
            actual_value = call_converter("homie", test_data["homie_heartbeat"])
            expected_value = test_data["homie_heartbeat"]["expected"]
            assert expected_value is None
            assert actual_value is None

        def test_homie_to_timescale_with_valid_json_for_measure_temperature(self):
            actual_value = call_converter(
                "homie", test_data["homie_measure_temperature"]
            )
            expected_value = test_data["homie_measure_temperature"]["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)
            assert_valid_schema(actual_value, schema)

        def test_ignored_publisher(self):
            test_object: dict = test_data["homie_mode"]
            with pytest.raises(ValueError) as e:
                call_converter("homie", test_object, "incorrect_publisher")
            assert "incorrect_publisher" in str(e.value)
            assert "homie processor only handles homie messages" in str(e.value).lower()


    class Test_Timeseries:
        class Test_get_record_type:
            def test_get_record_type_with_string(self):
                actual_value = get_record_type("a string")
                assert actual_value == PayloadType.STRING

            def test_get_record_type_with_int(self):
                actual_value = get_record_type(1)
                assert actual_value == PayloadType.NUMBER

            def test_get_record_type_with_float(self):
                actual_value = get_record_type(1.1)
                assert actual_value == PayloadType.NUMBER

            def test_get_record_type_with_none(self):
                with pytest.raises(Exception):
                    get_record_type(None)

            def test_get_record_type_with_empty_string(self):
                actual_value = get_record_type("")
                assert actual_value == PayloadType.STRING

            def test_get_record_type_with_boolean(self):
                actual_value = get_record_type(True)
                assert actual_value == PayloadType.BOOLEAN

            def test_get_record_type_with_dict(self):
                with pytest.raises(Exception):
                    get_record_type({"a": 1})

            def test_get_record_type_with_list(self):
                with pytest.raises(Exception):
                    get_record_type(["a", 1])

        class Test_create_record_recursive:
            #    payload: dict,
            #    records: List[dict[str, Any]],
            #    timestamp: str,
            #    correlation_id: str,
            #    measurement_subject: str,
            #    ignore_keys: list = None,
            #    measurement_of_prefix: str = None,
            def test_create_record_recursive_with_single_payload(self):
                records = []
                test_data = {
                    "payload": {"a": 1},
                    "timestamp": "2021-01-01T00:00:00",
                    "correlation_id": "123",
                    "measurement_subject": "emonTx4",
                    "measurement_publisher": "emon",
                    "ignore_keys": None,
                    "measurement_of_prefix": None,
                }
                expected_value = [
                    {
                        "timestamp": test_data["timestamp"],
                        "measurement_subject": test_data["measurement_subject"],
                        "measurement_publisher": test_data["measurement_publisher"],
                        "measurement_of": "a",
                        "measurement_value": 1,
                        "measurement_data_type": PayloadType.NUMBER.value,
                        "correlation_id": "123",
                    }
                ]
                actual_value = create_record_recursive(
                    test_data["payload"],
                    records,
                    test_data["timestamp"],
                    test_data["correlation_id"],
                    test_data["measurement_publisher"],
                    test_data["measurement_subject"],
                    test_data["ignore_keys"],
                    test_data["measurement_of_prefix"],
                )
                for actual, expected in zip(actual_value, expected_value):
                    TestCase().assertDictEqual(actual, expected)
                assert_valid_schema(actual_value, schema)

            def test_create_record_recursive_with_empty_payload(self):
                records = []
                test_data = {
                    "payload": None,
                    "timestamp": "2021-01-01T00:00:00",
                    "correlation_id": "123",
                    "measurement_subject": "emonTx4",
                    "measurement_publisher": "emon",
                    "ignore_keys": None,
                    "measurement_of_prefix": None,
                }
                expected_value = []
                actual_value = create_record_recursive(
                    test_data["payload"],
                    records,
                    test_data["timestamp"],
                    test_data["correlation_id"],
                    test_data["measurement_publisher"],
                    test_data["measurement_subject"],
                    test_data["ignore_keys"],
                    test_data["measurement_of_prefix"],
                )
                for actual, expected in zip(actual_value, expected_value):
                    TestCase().assertDictEqual(actual, expected)
                assert_valid_schema(actual_value, schema)

            def test_create_record_recursive_with_dict_of_payloads(self):
                records = []
                test_data = {
                    "payload": {"a": 1, "b": 2},
                    "timestamp": "2021-01-01T00:00:00",
                    "correlation_id": "123",
                    "measurement_subject": "emonTx4",
                    "measurement_publisher": "emon",
                    "ignore_keys": None,
                    "measurement_of_prefix": None,
                }
                expected_value = [
                    {
                        "timestamp": test_data["timestamp"],
                        "measurement_subject": test_data["measurement_subject"],
                        "measurement_publisher": test_data["measurement_publisher"],
                        "measurement_of": "a",
                        "measurement_value": 1,
                        "measurement_data_type": PayloadType.NUMBER.value,
                        "correlation_id": "123",
                    },
                    {
                        "timestamp": test_data["timestamp"],
                        "measurement_subject": test_data["measurement_subject"],
                        "measurement_publisher": test_data["measurement_publisher"],
                        "measurement_of": "b",
                        "measurement_value": 2,
                        "measurement_data_type": PayloadType.NUMBER.value,
                        "correlation_id": "123",
                    },
                ]
                actual_value = create_record_recursive(
                    test_data["payload"],
                    records,
                    test_data["timestamp"],
                    test_data["correlation_id"],
                    test_data["measurement_publisher"],
                    test_data["measurement_subject"],
                    test_data["ignore_keys"],
                    test_data["measurement_of_prefix"],
                )
                for actual, expected in zip(actual_value, expected_value):
                    TestCase().assertDictEqual(actual, expected)
                assert_valid_schema(actual_value, schema)

            def test_create_record_recursive_with_dict_of_payloads_ignoring_one(self):
                records = []
                test_data = {
                    "payload": {"a": 1, "b": 2},
                    "timestamp": "2021-01-01T00:00:00",
                    "correlation_id": "123",
                    "measurement_subject": "emonTx4",
                    "measurement_publisher": "emon",
                    "ignore_keys": ["a"],
                    "measurement_of_prefix": None,
                }
                expected_value = [
                    {
                        "timestamp": test_data["timestamp"],
                        "measurement_subject": test_data["measurement_subject"],
                        "measurement_publisher": test_data["measurement_publisher"],
                        "measurement_of": "b",
                        "measurement_value": 2,
                        "measurement_data_type": PayloadType.NUMBER.value,
                        "correlation_id": "123",
                    }
                ]
                actual_value = create_record_recursive(
                    test_data["payload"],
                    records,
                    test_data["timestamp"],
                    test_data["correlation_id"],
                    test_data["measurement_publisher"],
                    test_data["measurement_subject"],
                    test_data["ignore_keys"],
                    test_data["measurement_of_prefix"],
                )
                for actual, expected in zip(actual_value, expected_value):
                    TestCase().assertDictEqual(actual, expected)
                assert_valid_schema(actual_value, schema)

            def test_create_record_recursive_with_dict_of_payloads_and_measurement_prefix(
                self,
            ):
                records = []
                test_data = {
                    "payload": {"a": 1, "b": 2},
                    "timestamp": "2021-01-01T00:00:00",
                    "correlation_id": "123",
                    "measurement_subject": "emonTx4",
                    "measurement_publisher": "emon",
                    "ignore_keys": None,
                    "measurement_of_prefix": "prefix",
                }
                expected_value = [
                    {
                        "timestamp": test_data["timestamp"],
                        "measurement_subject": test_data["measurement_subject"],
                        "measurement_publisher": test_data["measurement_publisher"],
                        "measurement_of": "prefix_a",
                        "measurement_value": 1,
                        "measurement_data_type": PayloadType.NUMBER.value,
                        "correlation_id": "123",
                    },
                    {
                        "timestamp": test_data["timestamp"],
                        "measurement_subject": test_data["measurement_subject"],
                        "measurement_publisher": test_data["measurement_publisher"],
                        "measurement_of": "prefix_b",
                        "measurement_value": 2,
                        "measurement_data_type": PayloadType.NUMBER.value,
                        "correlation_id": "123",
                    },
                ]
                actual_value = create_record_recursive(
                    test_data["payload"],
                    records,
                    test_data["timestamp"],
                    test_data["correlation_id"],
                    test_data["measurement_publisher"],
                    test_data["measurement_subject"],
                    test_data["ignore_keys"],
                    test_data["measurement_of_prefix"],
                )
                for actual, expected in zip(actual_value, expected_value):
                    TestCase().assertDictEqual(actual, expected)
                assert_valid_schema(actual_value, schema)

            def test_create_record_recursive_with_dict_of_payloads_and_measurement_prefix_ignoring_one(
                self,
            ):
                records = []
                test_data = {
                    "payload": {"a": 1, "b": 2},
                    "timestamp": "2021-01-01T00:00:00",
                    "correlation_id": "123",
                    "measurement_subject": "emonTx4",
                    "measurement_publisher": "emon",
                    "ignore_keys": ["a"],
                    "measurement_of_prefix": "prefix",
                }
                expected_value = [
                    {
                        "timestamp": test_data["timestamp"],
                        "measurement_subject": test_data["measurement_subject"],
                        "measurement_publisher": test_data["measurement_publisher"],
                        "measurement_of": "prefix_b",
                        "measurement_value": 2,
                        "measurement_data_type": PayloadType.NUMBER.value,
                        "correlation_id": "123",
                    }
                ]
                actual_value = create_record_recursive(
                    test_data["payload"],
                    records,
                    test_data["timestamp"],
                    test_data["correlation_id"],
                    test_data["measurement_publisher"],
                    test_data["measurement_subject"],
                    test_data["ignore_keys"],
                    test_data["measurement_of_prefix"],
                )
                for actual, expected in zip(actual_value, expected_value):
                    TestCase().assertDictEqual(actual, expected)
                assert_valid_schema(actual_value, schema)


class Test_Helpers:
    class Test_is_topic_of_interest:
        def test_with_ignored_key(self):
            actual_value = is_topic_of_interest("homie/heartbeat", ["emonTx4"])
            assert actual_value is None

        def test_with_emonTx4_key(self):
            actual_value = is_topic_of_interest("emon/emonTx4", ["emonTx4"])
            assert actual_value == "emonTx4"

        def test_with_emonTx4_key_and_no_events_of_interest(self):
            actual_value = is_topic_of_interest("emon/emonTx4", [])
            assert actual_value is None

        def test_with_emonTx4_key_and_no_slashes(self):
            actual_value = is_topic_of_interest("emon_emonTx4", ["emonTx4"])
            assert actual_value is None

    class Test_to_datetime:
        def test_to_datetime_with_string_no_ms_no_tz(self):
            test_data = "2021-01-01T00:00:00"
            expected_value = "2021-01-01T00:00:00.000000Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_string_no_ms_with_tz(self):
            test_data = "2021-01-01T00:00:00+00:00"
            expected_value = "2021-01-01T00:00:00.000000Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_string_with_ms_no_tz(self):
            test_data = "2021-01-01T00:00:00.123"
            expected_value = "2021-01-01T00:00:00.123000Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_string_with_ms_with_tz(self):
            test_data = "2021-01-01T00:00:00.123+00:00"
            expected_value = "2021-01-01T00:00:00.123000Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_timestamp_no_ms(self):
            test_data = 1609459200
            expected_value = "2021-01-01T00:00:00.000000Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_timestamp_with_ms(self):
            test_data = 1609459200.123
            expected_value = "2021-01-01T00:00:00.123000Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_just_a_date(self):
            test_data = "2021-01-01"
            expected_value = "2021-01-01T00:00:00.000000Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_an_very_precise_timestamp(self):
            test_data = 1609459200.123456789
            expected_value = "2021-01-01T00:00:00.123457Z"
            actual_value = to_datetime(test_data)
            TestCase().assertEqual(actual_value, expected_value)
            TestCase().assertIs(type(actual_value), str)

        def test_to_datetime_with_an_incompatible_string(self):
            test_data = "lemon"
            with pytest.raises(Exception):
                to_datetime(test_data)

        def test_to_datetime_with_an_incompatible_dict(self):
            test_data = {"a": 1}
            with pytest.raises(Exception):
                to_datetime(test_data)

        def test_to_datetime_with_an_incompatible_int(self):
            test_data = -1
            with pytest.raises(Exception):
                to_datetime(test_data)

        def test_to_datetime_check_type(self):
            test_data = "2021-01-01T00:00:00"
            actual_value = to_datetime(test_data)
            TestCase().assertIs(type(actual_value), str)

    class Test_create_correlation_id:
        def test_create_correlation_id(self):
            sample_event = EventHubEvent(
                body=b"{}",
                sequence_number=1,
                enqueued_time=datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
            )
            expected_value = "2020-01-01T00:00:00.000000-1"
            actual_value = create_correlation_id(sample_event)
            TestCase().assertIs(type(actual_value), str)
            TestCase().assertEqual(actual_value, expected_value)

        def test_create_correlation_id_with_no_event(self):
            with pytest.raises(Exception):
                create_correlation_id(None)

        def test_create_correlation_id_with_no_enqueued_time(self):
            sample_event = EventHubEvent(
                body=b"{}",
                sequence_number=1,
                enqueued_time=None,
            )
            with pytest.raises(Exception):
                create_correlation_id(sample_event)

        def test_create_correlation_id_with_no_sequence_number(self):
            sample_event = EventHubEvent(
                body=b"{}",
                sequence_number=None,
                enqueued_time=datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc),
            )
            with pytest.raises(Exception):
                create_correlation_id(sample_event)

    class Test_recursively_deserialize:
        def test_recursively_deserialize_with_valid_json(self):
            actual_value = recursively_deserialize('{"a": 1}')
            assert actual_value == {"a": 1}

        def test_recursively_deserialize_with_invalid_json(self):
            actual_value = recursively_deserialize('{"a": 1')
            assert actual_value == '{"a": 1'

        def test_recursively_deserialize_with_none(self):
            actual_value = recursively_deserialize(None)
            assert actual_value is None

        def test_recursively_deserialize_with_empty_string(self):
            actual_value = recursively_deserialize("")
            assert actual_value == ""

        def test_recursively_deserialize_with_nested_object(self):
            actual_value = recursively_deserialize('{"a": {"b": 1}}')
            assert actual_value == {"a": {"b": 1}}

        def test_recursively_deserialize_with_nested_array(self):
            actual_value = recursively_deserialize('{"a": [{"b": 1}]}')
            assert actual_value == {"a": [{"b": 1}]}

        def test_recursively_deserialize_with_nested_array_and_object(self):
            actual_value = recursively_deserialize('{"a": [{"b": 1}, {"c": 2}]}')
            assert actual_value == {"a": [{"b": 1}, {"c": 2}]}

        def test_recursively_deserialize_with_nested_array_and_object_and_string(self):
            actual_value = recursively_deserialize('{"a": [{"b": 1}, {"c": 2}, "d"]}')
            assert actual_value == {"a": [{"b": 1}, {"c": 2}, "d"]}

        def test_recursively_deserialize_with_data_from_test_data_json(self):
            test_data = '{"homie_heartbeat": {"type": "EventHubEvent", "properties": {"body": "[{\\"a\\": \\"1\\", \\"b\\": \\"2\\"}, {\\"c\\": 3, \\"d\\": 4}]"}}}'  # noqa: E501
            expected_value = {
                "homie_heartbeat": {
                    "type": "EventHubEvent",
                    "properties": {"body": [{"a": "1", "b": "2"}, {"c": 3, "d": 4}]},
                }
            }
            actual_value = recursively_deserialize(test_data)
            assert actual_value == expected_value

        def test_recursively_deserialize_simple_dict_string(self):
            test_data = '{"a": 1, "b": 2}'
            expected_value = {"a": 1, "b": 2}
            actual_value = recursively_deserialize(test_data)
            assert actual_value == expected_value
    


if __name__ == "__main__":
    pytest.main()
