import os
import sys
from typing import Any, List
from unittest import TestCase

import pytest
from test_data import create_event_hub_event, load_test_data, recursive_json_parser


# add the shared_code directory to the path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shared_code import ( # noqa E402
    emon_to_timescale,
    is_topic_of_interest,
    glow_to_timescale,
    homie_to_timescale,
)  

# import test data

test_data = load_test_data()


def extract_topic(messagebody: dict) -> tuple[str, str]:
    topic: str = messagebody["topic"]
    # the publisher is the first characters to the left of the first /
    publisher = topic.split("/")[0]
    return topic, publisher


def call_emon_to_timescale(test_data_item: dict[str, Any]) -> List[dict[str, Any]]:
    test_event = create_event_hub_event(test_data_item["properties"])
    messagebody = test_data_item["properties"]["body"]
    # o_messagebody: dict = json.loads(messagebody)
    topic, publisher = extract_topic(messagebody)
    return emon_to_timescale(test_event, messagebody, topic, publisher)


class TestEmonToTimescale:
    def test_with_ignored_key(self):
        actual_value = call_emon_to_timescale(test_data["homie_heartbeat"])
        expected_value = test_data["homie_heartbeat"]["expected"]
        assert expected_value is None
        assert actual_value is None

    def test_with_emonTx4_key(self):
        test_object: dict = test_data["emontx4_json"]
        actual_value = call_emon_to_timescale(test_object)
        expected_value = test_object["expected"]
        for actual, expected in zip(actual_value, expected_value):
            TestCase().assertDictEqual(actual, expected)


def call_glow_to_timescale(test_data_item: dict[str, Any]) -> List[dict[str, Any]]:
    test_event = create_event_hub_event(test_data_item["properties"])
    messagebody = test_data_item["properties"]["body"]
    # o_messagebody: dict = json.loads(messagebody)
    topic, publisher = extract_topic(messagebody)
    return glow_to_timescale(test_event, messagebody, topic, publisher)


class Test_Glow:
    def test_glow_to_timescale_with_valid_json_for_electricity_meter(self):
        actual_value = call_glow_to_timescale(test_data["glow_electricitymeter"])
        expected_value = test_data["glow_electricitymeter"]["expected"]
        for actual, expected in zip(actual_value, expected_value):
            TestCase().assertDictEqual(actual, expected)

    def test_glow_to_timescale_with_valid_json_for_gas_meter(self):
        actual_value = call_glow_to_timescale(test_data["glow_gasmeter"])
        expected_value = test_data["glow_gasmeter"]["expected"]
        for actual, expected in zip(actual_value, expected_value):
            TestCase().assertDictEqual(actual, expected)

    def test_glow_to_timescale_with_item_to_ignore(self):
        actual_value = call_glow_to_timescale(test_data["homie_heartbeat"])
        expected_value = test_data["homie_heartbeat"]["expected"]
        assert actual_value == expected_value


def call_homie_to_timescale(test_data_item: dict[str, Any]) -> List[dict[str, Any]]:
    test_event = create_event_hub_event(test_data_item["properties"])
    messagebody = test_data_item["properties"]["body"]
    # o_messagebody: dict = json.loads(messagebody)
    topic, publisher = extract_topic(messagebody)
    return homie_to_timescale(test_event, messagebody, topic, publisher)


class Test_Homie:
    def test_homie_to_timescale_with_valid_json_for_mode(self):
        actual_value = call_homie_to_timescale(test_data["homie_mode"])
        expected_value = test_data["homie_mode"]["expected"]
        for actual, expected in zip(actual_value, expected_value):
            TestCase().assertDictEqual(actual, expected)

    def test_homie_to_timescale_with_ignored_json_for_heartbeat(self):
        actual_value = call_homie_to_timescale(test_data["homie_heartbeat"])
        expected_value = test_data["homie_heartbeat"]["expected"]
        assert expected_value is None
        assert actual_value is None

    def test_homie_to_timescale_with_valid_json_for_measure_temperature(self):
        actual_value = call_homie_to_timescale(test_data["homie_measure_temperature"])
        expected_value = test_data["homie_measure_temperature"]["expected"]
        for actual, expected in zip(actual_value, expected_value):
            TestCase().assertDictEqual(actual, expected)


class Test_Helpers:
    class TestIsTopicOfInterest:
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

    class TestJsonParser:
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


if __name__ == "__main__":
    pytest.main()
