# from test_utils.get_test_data import create_event_hub_event, load_test_data
from shared_code import emon
import pytest
from datetime import datetime


# test_data = load_test_data()


class TestValidateThisIsAnEmonMessage:
    def test_with_valid_publisher(self):
        result = emon.validate_this_is_an_emon_message("emon")
        assert result is None

    def test_with_invalid_publisher(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid publisher: emon processor only handles emon messages, not incorrect_publisher.*",
        ):
            emon.validate_this_is_an_emon_message("incorrect_publisher")


class TestValidateMessageBody:
    def test_with_valid_messagebody(self):
        result = emon.validate_message_body({"payload": "test"})
        assert result is None

    def test_with_missing_payload(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid messagebody: emon: missing payload, not {'test': 'test'}.*",
        ):
            emon.validate_message_body({"test": "test"})

    def test_with_non_dict(self):
        with pytest.raises(
            ValueError,
            match=r".*Invalid messagebody: emon processor only handles dict messages, not .*",
        ):
            emon.validate_message_body("test")


class TestExtractTimestamp:
    def test_extract_timestamp_valid(self):
        # Test case with valid timestamp
        message_payload = {"time": 1699364497.0467954}
        timestamp = emon.extract_timestamp(message_payload)
        assert isinstance(timestamp, str)
        assert timestamp == "2023-11-07T13:41:37.046795Z"

    def test_extract_timestamp_missing_time(self):
        # Test case where 'time' key is missing
        message_payload = {"other_key": "value"}
        with pytest.raises(ValueError) as exc_info:
            emon.extract_timestamp(message_payload)
        assert str(exc_info.value) == (
            f"Invalid messagebody: emon: missing time, not {message_payload}"
        )

    # Optional: Test for invalid time format, depends on to_datetime's behavior
    def test_extract_timestamp_invalid_time_format(self):
        # Test case with invalid time format
        message_payload = {"time": "invalid_time_format"}
        # Assuming to_datetime raises a specific exception for invalid format
        with pytest.raises(
            Exception
        ):  # Replace YourSpecificException with the actual exception type
            emon.extract_timestamp(message_payload)


# class Test_emon_to_timescale:
#         def test_with_ignored_key(self):
#             actual_value = call_converter("emon", test_data["emon_ignored"])
#             expected_value = test_data["emon_ignored"]["expected"]
#             assert expected_value is None
#             assert actual_value is None

#         def test_with_valid_emonTx4_data(self):
#             test_object: dict = test_data["emontx4_json"]
#             actual_value = call_converter("emon", test_object)
#             expected_value = test_object["expected"]
#             for actual, expected in zip(actual_value, expected_value):
#                 TestCase().assertDictEqual(actual, expected)
#             assert_valid_schema(actual_value, schema)

#         def test_ignored_publisher(self):
#             test_object: dict = test_data["emontx4_json"]
#             with pytest.raises(
#                 ValueError,
#                 match=r".*Invalid publisher: [eE]mon processor only handles [eE]mon messages, not incorrect_publisher.*",  # noqa E501
#             ):
#                 call_converter("emon", test_object, "incorrect_publisher")

#     class Test_glow_to_timescale:
#         def test_with_valid_json_for_electricity_meter(self):
#             actual_value = call_converter("glow", test_data["glow_electricitymeter"])
#             expected_value = test_data["glow_electricitymeter"]["expected"]
#             for actual, expected in zip(actual_value, expected_value):
#                 TestCase().assertDictEqual(actual, expected)
#             assert_valid_schema(actual_value, schema)

#         def test_with_valid_json_for_gas_meter(self):
#             actual_value = call_converter("glow", test_data["glow_gasmeter"])
#             expected_value = test_data["glow_gasmeter"]["expected"]
#             for actual, expected in zip(actual_value, expected_value):
#                 TestCase().assertDictEqual(actual, expected)
#             assert_valid_schema(actual_value, schema)

#         def test_ignored_publisher(self):
#             test_object: dict = test_data["glow_gasmeter"]
#             with pytest.raises(
#                 ValueError,
#                 match=r".*Invalid publisher: [gG]low processor only handles [gG]low messages, not incorrect_publisher.*",  # noqa E501
#             ):
#                 call_converter("glow", test_object, "incorrect_publisher")

#         def test_with_item_to_ignored_measurement(self):
#             actual_value = call_converter("glow", test_data["glow_ignored"])
#             expected_value = test_data["homie_heartbeat"]["expected"]  # None
#             assert expected_value is None
#             assert actual_value is None
