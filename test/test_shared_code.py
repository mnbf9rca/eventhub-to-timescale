import os
import sys
from typing import Any, List
from unittest import TestCase
import pytest
from test_data import create_event_hub_event, load_test_data, recursive_json_parser

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
)

# import test data
test_data = load_test_data()


def extract_topic(messagebody: dict) -> tuple[str, str]:
    """Extract the topic and publisher from the message body
    @param messagebody: the message body
    @return: the topic and publisher
    """
    topic: str = messagebody["topic"]
    # the publisher is the first characters to the left of the first /
    publisher = topic.split("/")[0]
    return topic, publisher


def call_converter(
    converter: str, test_data_item: dict[str, Any]
) -> List[dict[str, Any]]:
    """Call the converter with the test data item
    @param converter: the converter to call - homie, emon, glow
    @param test_data_item: the test data item to use
    @return: the result of the converter
    """
    test_event = create_event_hub_event(test_data_item["properties"])
    messagebody = test_data_item["properties"]["body"]
    # o_messagebody: dict = json.loads(messagebody)
    topic, publisher = extract_topic(messagebody)
    if converter == "emon":
        return emon_to_timescale(test_event, messagebody, topic, publisher)
    elif converter == "glow":
        return glow_to_timescale(test_event, messagebody, topic, publisher)
    elif converter == "homie":
        return homie_to_timescale(test_event, messagebody, topic, publisher)
    else:
        raise ValueError(f"Unknown converter {converter}")


class Test_Converter_Methods:
    class Test_Emon:
        def test_with_ignored_key(self):
            actual_value = call_converter("emon", test_data["homie_heartbeat"])
            expected_value = test_data["homie_heartbeat"]["expected"]
            assert expected_value is None
            assert actual_value is None

        def test_with_emonTx4_key(self):
            test_object: dict = test_data["emontx4_json"]
            actual_value = call_converter("emon", test_object)
            expected_value = test_object["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)

    class Test_Glow:
        def test_glow_to_timescale_with_valid_json_for_electricity_meter(self):
            actual_value = call_converter("glow", test_data["glow_electricitymeter"])
            expected_value = test_data["glow_electricitymeter"]["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)

        def test_glow_to_timescale_with_valid_json_for_gas_meter(self):
            actual_value = call_converter("glow", test_data["glow_gasmeter"])
            expected_value = test_data["glow_gasmeter"]["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)

        def test_glow_to_timescale_with_item_to_ignore(self):
            actual_value = call_converter("glow", test_data["homie_heartbeat"])
            expected_value = test_data["homie_heartbeat"]["expected"]
            assert actual_value == expected_value

    class Test_Homie:
        def test_homie_to_timescale_with_valid_json_for_mode(self):
            actual_value = call_converter("homie", test_data["homie_mode"])
            expected_value = test_data["homie_mode"]["expected"]
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)

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
                "ignore_keys": None,
                "measurement_of_prefix": None,
            }
            expected_value = [{
                "timestamp": test_data["timestamp"],
                "measurement_subject": test_data["measurement_subject"],
                "measurement_of": "a",
                "measurement_value": 1,
                "measurement_data_type": PayloadType.NUMBER.value,
                "correlation_id": "123",
            }]
            # payload: dict, records: List[dict[str, Any]], timestamp: str, correlation_id: str, measurement_subject: str, ignore_keys: list = None, measurement_of_prefix: str = None
            actual_value = create_record_recursive(
                test_data["payload"],
                records,
                test_data["timestamp"],
                test_data["correlation_id"],
                test_data["measurement_subject"],
                test_data["ignore_keys"],
                test_data["measurement_of_prefix"],
            )
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)
        
        def test_create_record_recursive_with_empty_payload(self):
            records = []
            test_data = {
                "payload": None,
                "timestamp": "2021-01-01T00:00:00",
                "correlation_id": "123",
                "measurement_subject": "emonTx4",
                "ignore_keys": None,
                "measurement_of_prefix": None,
            }
            expected_value = []
            actual_value = create_record_recursive(
                test_data["payload"],
                records,
                test_data["timestamp"],
                test_data["correlation_id"],
                test_data["measurement_subject"],
                test_data["ignore_keys"],
                test_data["measurement_of_prefix"],
            )
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)

        def test_create_record_recursive_with_dict_of_payloads(self):
            records = []
            test_data = {
                "payload": {"a": 1, "b": 2},
                "timestamp": "2021-01-01T00:00:00",
                "correlation_id": "123",
                "measurement_subject": "emonTx4",
                "ignore_keys": None,
                "measurement_of_prefix": None,
            }
            expected_value = [{
                "timestamp": test_data["timestamp"],
                "measurement_subject": test_data["measurement_subject"],
                "measurement_of": "a",
                "measurement_value": 1,
                "measurement_data_type": PayloadType.NUMBER.value,
                "correlation_id": "123",
            },
            {
                "timestamp": test_data["timestamp"],
                "measurement_subject": test_data["measurement_subject"],
                "measurement_of": "b",
                "measurement_value": 2,
                "measurement_data_type": PayloadType.NUMBER.value,
                "correlation_id": "123",
            }]
            # payload: dict, records: List[dict[str, Any]], timestamp: str, correlation_id: str, measurement_subject: str, ignore_keys: list = None, measurement_of_prefix: str = None
            actual_value = create_record_recursive(
                test_data["payload"],
                records,
                test_data["timestamp"],
                test_data["correlation_id"],
                test_data["measurement_subject"],
                test_data["ignore_keys"],
                test_data["measurement_of_prefix"],
            )
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)      

        def test_create_record_recursive_with_dict_of_payloads_ignoring_one(self):
            records = []
            test_data = {
                "payload": {"a": 1, "b": 2},
                "timestamp": "2021-01-01T00:00:00",
                "correlation_id": "123",
                "measurement_subject": "emonTx4",
                "ignore_keys": ["a"],
                "measurement_of_prefix": None,
            }
            expected_value = [{
                "timestamp": test_data["timestamp"],
                "measurement_subject": test_data["measurement_subject"],
                "measurement_of": "b",
                "measurement_value": 2,
                "measurement_data_type": PayloadType.NUMBER.value,
                "correlation_id": "123",
            }]
            # payload: dict, records: List[dict[str, Any]], timestamp: str, correlation_id: str, measurement_subject: str, ignore_keys: list = None, measurement_of_prefix: str = None
            actual_value = create_record_recursive(
                test_data["payload"],
                records,
                test_data["timestamp"],
                test_data["correlation_id"],
                test_data["measurement_subject"],
                test_data["ignore_keys"],
                test_data["measurement_of_prefix"],
            )
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)    

        def test_create_record_recursive_with_dict_of_payloads_and_measurement_prefix(self):
            records = []
            test_data = {
                "payload": {"a": 1, "b": 2},
                "timestamp": "2021-01-01T00:00:00",
                "correlation_id": "123",
                "measurement_subject": "emonTx4",
                "ignore_keys": None,
                "measurement_of_prefix": "prefix",
            }
            expected_value = [{
                "timestamp": test_data["timestamp"],
                "measurement_subject": test_data["measurement_subject"],
                "measurement_of": "prefix_a",
                "measurement_value": 1,
                "measurement_data_type": PayloadType.NUMBER.value,
                "correlation_id": "123",
            },
            {
                "timestamp": test_data["timestamp"],
                "measurement_subject": test_data["measurement_subject"],
                "measurement_of": "prefix_b",
                "measurement_value": 2,
                "measurement_data_type": PayloadType.NUMBER.value,
                "correlation_id": "123",
            }]
            # payload: dict, records: List[dict[str, Any]], timestamp: str, correlation_id: str, measurement_subject: str, ignore_keys: list = None, measurement_of_prefix: str = None
            actual_value = create_record_recursive(
                test_data["payload"],
                records,
                test_data["timestamp"],
                test_data["correlation_id"],
                test_data["measurement_subject"],
                test_data["ignore_keys"],
                test_data["measurement_of_prefix"],
            )
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)

if __name__ == "__main__":
    pytest.main()
