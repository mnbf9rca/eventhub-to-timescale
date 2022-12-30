from typing import Any
import pytest
import mock
import json
# from unittest import mock
import pytest_mock
import os
import sys
import logging

import azure.functions as func


from azure.functions import EventHubEvent
from test_data import create_event_hub_event, load_test_data, recursive_json_parser


# add the shared_code directory to the path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import json_to_timeseries
from shared_code import glow, homie, emon
from json_to_timeseries import parse_message, send_to_converter, extract_topic as jts_extract_topic

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

def get_spies(mocker: pytest_mock.MockFixture):
    """Get the spies for the functions that are called by send_to_converter
    @param mocker: the mocker
    @return: the spies
    """
    spy_glow_to_timescale = mocker.spy(json_to_timeseries, "glow_to_timescale")
    spy_homie_to_timescale = mocker.spy(json_to_timeseries, "homie_to_timescale")
    spy_emon_to_timescale = mocker.spy(json_to_timeseries, "emon_to_timescale")
    return spy_glow_to_timescale, spy_homie_to_timescale, spy_emon_to_timescale

def get_test_data(test_name: str):
    """Get the test data for a test
    @param test_name: the name of the test
    @return: the test data
    """
    test_data_item = test_data[test_name]
    test_event = create_event_hub_event(test_data_item["properties"])
    messagebody = test_data_item["properties"]["body"]
    topic, publisher = extract_topic(messagebody)
    return test_event, messagebody, topic, publisher


class Test_send_to_converter:
    def test_parse_message_glow_electricitymeter(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("glow_electricitymeter")
        spy_glow_to_timescale, spy_homie_to_timescale, spy_emon_to_timescale = get_spies(mocker)
        send_to_converter(publisher, test_event, messagebody, topic)
        assert spy_glow_to_timescale.call_count == 1
        assert spy_homie_to_timescale.call_count == 0
        assert spy_emon_to_timescale.call_count == 0

    def test_parse_message_glow_gasmeter(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("glow_gasmeter")
        spy_glow_to_timescale, spy_homie_to_timescale, spy_emon_to_timescale = get_spies(mocker)
        send_to_converter(publisher, test_event, messagebody, topic)
        assert spy_glow_to_timescale.call_count == 1
        assert spy_homie_to_timescale.call_count == 0
        assert spy_emon_to_timescale.call_count == 0


    def test_parse_message_homie_mode(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("homie_mode")
        spy_glow_to_timescale, spy_homie_to_timescale, spy_emon_to_timescale = get_spies(mocker)
        send_to_converter(publisher, test_event, messagebody, topic)
        assert spy_glow_to_timescale.call_count == 0
        assert spy_homie_to_timescale.call_count == 1
        assert spy_emon_to_timescale.call_count == 0

    def test_parse_message_homie_measure_temperature(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("homie_measure_temperature")
        spy_glow_to_timescale, spy_homie_to_timescale, spy_emon_to_timescale = get_spies(mocker)
        send_to_converter(publisher, test_event, messagebody, topic)
        assert spy_glow_to_timescale.call_count == 0
        assert spy_homie_to_timescale.call_count == 1
        assert spy_emon_to_timescale.call_count == 0

    def test_parse_message_emontx4_json(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("emontx4_json")
        spy_glow_to_timescale, spy_homie_to_timescale, spy_emon_to_timescale = get_spies(mocker)
        send_to_converter(publisher, test_event, messagebody, topic)
        assert spy_glow_to_timescale.call_count == 0
        assert spy_homie_to_timescale.call_count == 0
        assert spy_emon_to_timescale.call_count == 1

    def test_parse_message_unknown_publisher(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("glow_electricitymeter")
        publisher = "abc"
        spy_glow_to_timescale, spy_homie_to_timescale, spy_emon_to_timescale = get_spies(mocker)
        with pytest.raises(Exception):
            send_to_converter(publisher, test_event, messagebody, topic)
        assert spy_glow_to_timescale.call_count == 0
        assert spy_homie_to_timescale.call_count == 0
        assert spy_emon_to_timescale.call_count == 0
        assert "Unknown publisher: abc" in caplog.text

class Test_parse_message:
    def test_parse_message_calls_send_to_converter(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("glow_electricitymeter")
        mocked_spy_send_to_converter = mocker.patch("json_to_timeseries.send_to_converter",  autospec=True)
        return_value = [{"measurement": "test", "tags": {"tag1": "value1"}, "fields": {"field1": 1}}]
        mocked_spy_send_to_converter.return_value = return_value
        expected_value = [json.dumps(p) for p in return_value]
        actual_value = parse_message(test_event)
        assert actual_value == expected_value
        assert mocked_spy_send_to_converter.call_count == 1

    def test_parse_message_calls_send_to_converter_which_returns_two_items(self, mocker: pytest_mock.MockFixture, caplog: pytest.LogCaptureFixture):
        caplog.set_level(logging.DEBUG)
        test_event, messagebody, topic, publisher = get_test_data("glow_electricitymeter")
        mocked_spy_send_to_converter = mocker.patch("json_to_timeseries.send_to_converter",  autospec=True)
        return_value = [{"measurement": "test", "tags": {"tag1": "value1"}, "fields": {"field1": 1}}, {"measurement": "test2", "tags": {"tag2": "value2"}, "fields": {"field2": 2}}]
        mocked_spy_send_to_converter.return_value = return_value
        expected_value = [json.dumps(p) for p in return_value]
        actual_value = parse_message(test_event)
        assert actual_value == expected_value
        assert mocked_spy_send_to_converter.call_count == 1
        