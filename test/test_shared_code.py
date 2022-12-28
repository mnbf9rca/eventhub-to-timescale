import json
import os
import sys
from typing import Any, List
from unittest import TestCase

import pytest
from test_data import create_event_hub_event, load_test_data

# add the shared_code directory to the path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shared_code import emon_to_timescale  # noqa: E402

# import test data

test_data = load_test_data()


class TestEmonToTimescale:
    def test_with_ignored_key(self):
        actual_value = call_emon_to_timescale(test_data["homie_heartbeat"])
        expected_value = test_data["homie_heartbeat"]["expected"]
        if expected_value is None:
            assert actual_value is None
        else:
            assert actual_value == expected_value["value"]
    
    def test_with_emonTx4_key(self):
        test_object: dict = test_data["emontx4_json"]
        actual_value = call_emon_to_timescale(test_object)
        expected_value = test_object["expected"]
        if expected_value is None:
            assert actual_value is None
        else:
            expected_value: List[dict] = json.loads(expected_value)
            # use TestCase.AssertDictEqual to compare the dictionaries
            for actual, expected in zip(actual_value, expected_value):
                TestCase().assertDictEqual(actual, expected)
            # TestCase().assertDictEqual(actual_value, expected_value)



def call_emon_to_timescale(test_data_item: dict[str, Any]) -> List[dict[str, Any]]:
    test_event = create_event_hub_event(test_data_item["properties"])
    messagebody = test_data_item["properties"]["body"]
    o_messagebody: dict = json.loads(messagebody)
    topic, publisher = extract_topic(o_messagebody)
    return_value = emon_to_timescale(test_event, o_messagebody, topic, publisher)
    return return_value


def extract_topic(messagebody: dict) -> tuple[str, str]:
    topic: str = messagebody["topic"]
    # the publisher is the first characters to the left of the first /
    publisher = topic.split("/")[0]
    return topic, publisher


if __name__ == "__main__":
    pytest.main()
