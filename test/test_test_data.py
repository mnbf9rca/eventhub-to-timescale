from unittest import TestCase
import pytest

from test_data import create_event_hub_event, load_test_data, recursive_json_parser


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
