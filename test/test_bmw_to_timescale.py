import pytest
import os
import uuid

from copy import copy
import json
from unittest.mock import patch, Mock, call, MagicMock
from shared_code.bmw_to_timescale import (
    convert_bmw_to_timescale,
    get_event_body,
    construct_messages,
    get_electric_charging_state_from_message,
    get_current_mileage_from_message,
    get_coordinates_from_message,
    validate_lat_long,
    create_records_from_fields,
    get_vin_from_message,
    get_last_updated_at_from_message,
)
from shared_code import bmw_to_timescale as btc
import shared_code as sc
from shared_code import PayloadType
from azure.functions import EventHubEvent, Out
from shared_code.duplicate_check import check_duplicate, store_id


def generate_alpha_uuid():
    raw_uuid = str(uuid.uuid4()).replace("-", "")  # Remove hyphens
    if raw_uuid[0].isdigit():
        # Replace the first character with a letter if it's a digit
        return "a" + raw_uuid[1:]
    return raw_uuid


class TestConvertBmwToTimescaleEndToEnd:
    @patch("shared_code.bmw_to_timescale.get_vin_from_message")
    def test_convert_bmw_to_timescale_with_real_data(
        self, mock_get_vin_from_message, mocker
    ):
        # Mock get_vin_from_message to return a GUID
        mock_get_vin_from_message.return_value = generate_alpha_uuid()

        # Load the real data from the json file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_file = os.path.join(current_dir, "bmw_topic_messages.json")
        with open(data_file, "r") as f:
            event_data = json.load(f)

        # Create EventHubEvent objects
        events = [
            EventHubEvent(body=json.dumps(data).encode("utf-8")) for data in event_data
        ]

        # Create an outputEventHubMessage mock
        mock_outputEventHubMessage = mocker.MagicMock(spec=Out)

        # Spy on other external functions
        spy_get_last_updated_at_from_message = mocker.spy(
            btc, "get_last_updated_at_from_message"
        )
        spy_check_duplicate = mocker.spy(sc, "check_duplicate")
        spy_construct_messages = mocker.spy(btc, "construct_messages")
        spy_store_id = mocker.spy(sc, "store_id")

        # Call the function
        convert_bmw_to_timescale(events, mock_outputEventHubMessage)

        # Validate that the external functions were called the expected number of times
        assert mock_get_vin_from_message.call_count == 3
        assert spy_get_last_updated_at_from_message.call_count == 3
        assert spy_check_duplicate.call_count == 3
        assert spy_construct_messages.call_count == 2  # One duplicate, one not

        assert spy_store_id.call_count == 2  # One duplicate, one not

        # Validate that outputEventHubMessage.set was called the expected number of times
        expected_published_messages = (
            spy_construct_messages.call_count * 6
        )  # 6 records per message
        # chargingLevelPercent, range, isChargerConnected, chargingStatus, currentMileage, coordinates

        assert mock_outputEventHubMessage.set.call_count == expected_published_messages


class TestConvertBmwToTimescale:
    @pytest.mark.parametrize("duplicate_status", [True, False])
    @patch("shared_code.bmw_to_timescale.construct_messages")
    @patch("shared_code.bmw_to_timescale.sc.store_id")
    @patch("shared_code.bmw_to_timescale.sc.check_duplicate")
    @patch("shared_code.bmw_to_timescale.get_last_updated_at_from_message")
    @patch("shared_code.bmw_to_timescale.get_vin_from_message")
    @patch("shared_code.bmw_to_timescale.get_event_body")
    @patch("shared_code.bmw_to_timescale.sc.get_table_service_client")
    def test_convert_bmw_to_timescale_no_exception(
        self,
        mock_get_table_service_client,
        mock_get_event_body,
        mock_get_vin_from_message,
        mock_get_last_updated_at_from_message,
        mock_check_duplicate,
        mock_store_id,
        mock_construct_messages,
        duplicate_status,
    ):
        # Mock external functions
        mock_get_table_service_client.return_value = MagicMock()
        mock_get_event_body.return_value = {}
        mock_get_vin_from_message.return_value = "VIN123"
        mock_get_last_updated_at_from_message.return_value = "timestamp123"
        mock_check_duplicate.return_value = duplicate_status
        mock_construct_messages.return_value = [[{"some_messages": "some_value"}]]
        mock_outputEventHubMessage = MagicMock()

        # Call function
        convert_bmw_to_timescale([MagicMock()], mock_outputEventHubMessage)

        # Assertions
        mock_check_duplicate.assert_called_once_with(
            mock_get_last_updated_at_from_message.return_value,
            mock_get_vin_from_message.return_value,
            mock_get_table_service_client.return_value,
        )
        if duplicate_status:
            mock_construct_messages.assert_not_called()
            mock_outputEventHubMessage.set.assert_not_called()
            mock_store_id.assert_not_called()
        else:
            mock_construct_messages.assert_called_with(
                mock_get_vin_from_message.return_value,
                mock_get_last_updated_at_from_message.return_value,
                mock_get_event_body.return_value,
            )
            mock_outputEventHubMessage.set.assert_called_with(
                json.dumps(mock_construct_messages.return_value[0])
            )
            mock_store_id.assert_called_with(
                mock_get_last_updated_at_from_message.return_value,
                mock_get_vin_from_message.return_value,
                mock_get_table_service_client.return_value,
            )

    @patch("shared_code.bmw_to_timescale.get_event_body")
    def test_convert_bmw_to_timescale_exception(self, mock_get_event_body):
        # Mock external function to raise exception
        mock_get_event_body.side_effect = Exception("An error occurred")

        # Call function and assert that it raises an exception
        with pytest.raises(Exception):
            convert_bmw_to_timescale([MagicMock()], MagicMock())

    @patch("shared_code.bmw_to_timescale.construct_messages")
    @patch("shared_code.bmw_to_timescale.sc.store_id")
    @patch("shared_code.bmw_to_timescale.sc.check_duplicate")
    @patch("shared_code.bmw_to_timescale.get_last_updated_at_from_message")
    @patch("shared_code.bmw_to_timescale.get_vin_from_message")
    @patch("shared_code.bmw_to_timescale.get_event_body")
    @patch("shared_code.bmw_to_timescale.sc.get_table_service_client")
    def test_convert_bmw_to_timescale_outputEventHubMessage_exception(
        self,
        mock_get_table_service_client,
        mock_get_event_body,
        mock_get_vin_from_message,
        mock_get_last_updated_at_from_message,
        mock_check_duplicate,
        mock_store_id,
        mock_construct_messages,
    ):
        # Mock external functions
        mock_get_table_service_client.return_value = MagicMock()
        mock_get_event_body.return_value = {}
        mock_get_vin_from_message.return_value = "VIN123"
        mock_get_last_updated_at_from_message.return_value = "timestamp123"
        mock_check_duplicate.return_value = False
        mock_construct_messages.return_value = [[{"some_messages": "some_value"}]]

        # Mock outputEventHubMessage to raise an exception
        mock_outputEventHubMessage = MagicMock()
        mock_outputEventHubMessage.set.side_effect = Exception(
            "An error occurred while sending message"
        )

        # Call function and assert that it raises the expected exception
        with pytest.raises(Exception) as excinfo:
            convert_bmw_to_timescale([MagicMock()], mock_outputEventHubMessage)

        # Assert the exception message
        assert str(excinfo.value) == "An error occurred while sending message"


class TestGetEventBody:
    def test_valid_event(self):
        mock_event = Mock()
        mock_event.get_body.return_value = b'{"key": "value"}'
        assert get_event_body(mock_event) == {"key": "value"}

    def test_invalid_json(self):
        mock_event = Mock()
        mock_event.get_body.return_value = b"invalid_json"
        with pytest.raises(json.JSONDecodeError):
            get_event_body(mock_event)

    def test_non_utf8_encoding(self):
        mock_event = Mock()
        mock_event.get_body.return_value = b"\x80abc"
        with pytest.raises(UnicodeDecodeError):
            get_event_body(mock_event)

    def test_empty_event(self):
        mock_event = Mock()
        mock_event.get_body.return_value = b""
        with pytest.raises(json.JSONDecodeError):
            get_event_body(mock_event)

    def test_none_event(self):
        mock_event = Mock()
        mock_event.get_body.return_value = None
        with pytest.raises(AttributeError):
            get_event_body(mock_event)


class TestConstructMessages:
    @patch("shared_code.bmw_to_timescale.validate_lat_long")
    @patch("shared_code.bmw_to_timescale.get_coordinates_from_message")
    @patch("shared_code.bmw_to_timescale.get_current_mileage_from_message")
    @patch("shared_code.bmw_to_timescale.get_electric_charging_state_from_message")
    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_construct_messages(
        self,
        mock_sc_create_atomic_record,
        mock_get_electric_charging_state_from_message,
        mock_get_current_mileage_from_message,
        mock_get_coordinates_from_message,
        mock_validate_lat_long,
    ):
        # Setup
        mock_get_electric_charging_state_from_message.return_value = {
            "chargingLevelPercent": 80,
            "range": 200,
            "isChargerConnected": 1,
            "chargingStatus": "Charging",
        }
        mock_get_current_mileage_from_message.return_value = {"currentMileage": 120}
        mock_get_coordinates_from_message.return_value = {
            "coordinates": {"latitude": 12.3456, "longitude": 0.1234}
        }
        mock_sc_create_atomic_record.return_value = "atomic_record"
        mock_validate_lat_long.return_value = [12.3456, 0.1234]

        vin = "some_vin"
        last_updated_at = "some_timestamp"
        event_object = {"some_key": "some_value"}

        all_fields = {
            **mock_get_current_mileage_from_message.return_value,
            **mock_get_electric_charging_state_from_message.return_value,
            **mock_get_coordinates_from_message.return_value,
        }

        # Fields and types as they are in the function
        # one for each type of PayloadType
        # including a complex type (GEOGRAPHY)
        fields_to_record = [
            (
                "chargingLevelPercent",
                PayloadType.NUMBER,
                all_fields["chargingLevelPercent"],
            ),
            ("range", PayloadType.NUMBER, all_fields["range"]),
            (
                "isChargerConnected",
                PayloadType.BOOLEAN,
                bool(all_fields["isChargerConnected"]),
            ),
            (
                "chargingStatus",
                PayloadType.STRING,
                all_fields["chargingStatus"],
            ),
            (
                "currentMileage",
                PayloadType.NUMBER,
                all_fields["currentMileage"],
            ),
            (
                "coordinates",
                PayloadType.GEOGRAPHY,
                tuple(
                    validate_lat_long(
                        all_fields["coordinates"]["latitude"],
                        all_fields["coordinates"]["longitude"],
                    )
                ),
            ),
        ]

        # Extract keys from the list of tuples
        fields_to_record_keys = [field[0] for field in fields_to_record]

        # Now find the intersection
        common_fields_count = len(
            set(fields_to_record_keys).intersection(set(all_fields.keys()))
        )

        # Exercise
        result = construct_messages(vin, last_updated_at, event_object)

        # Debug prints
        print("all_fields: ", all_fields)
        print("fields_to_record: ", fields_to_record)
        print("common_fields_count: ", common_fields_count)

        # Verify
        mock_get_electric_charging_state_from_message.assert_called_once_with(
            event_object
        )
        mock_get_current_mileage_from_message.assert_called_once_with(event_object)
        mock_get_coordinates_from_message.assert_called_once_with(event_object)

        # Check if mock_sc_create_atomic_record is called with the expected arguments
        mock_sc_create_atomic_record.assert_has_calls(
            [
                call(
                    vin,
                    last_updated_at,
                    field,
                    fields_to_record[field],
                    all_fields.get(field, None),
                )
                for field in fields_to_record
                if field in all_fields
            ],
            any_order=True,
        )

        # Check the resulting messages
        assert result == ["atomic_record"] * common_fields_count


class TestCreateRecordsFromFields:
    @pytest.fixture(scope="class")
    def common_data(self):
        all_fields = {"speed": 70, "battery": 80}
        fields_to_record = [
            ("speed", PayloadType.NUMBER, all_fields["speed"]),
            ("battery", PayloadType.NUMBER, all_fields["battery"]),
        ]
        return all_fields, fields_to_record

    @pytest.mark.usefixtures("common_data")
    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_missing_field_in_all_fields(
        self, mock_create_atomic_record: MagicMock, common_data
    ):
        all_fields, fields_to_record = common_data
        vin = "some_vin"
        last_updated_at = "2023-01-01T12:34:56Z"

        # Create a shallow copy of all_fields and remove 'battery'
        mutated_all_fields = copy(all_fields)
        mutated_all_fields.pop("battery", None)

        expected = [
            {"return_1"}
        ]  # should only be called once because battery is missing

        mock_create_atomic_record.side_effect = [
            {"return_1"},
            {"return_2"},
        ]

        records = create_records_from_fields(
            vin, last_updated_at, mutated_all_fields, fields_to_record
        )

        # Verify that the mock was called with the expected arguments
        mock_create_atomic_record.assert_has_calls(
            [
                call(
                    source_timestamp=last_updated_at,
                    measurement_subject=vin,
                    measurement_publisher="bmw",
                    measurement_of=field,
                    measurement_data_type=ptype,
                    correlation_id=last_updated_at,
                    measurement_value=value,
                )
                for field, ptype, value in fields_to_record
                if field in mutated_all_fields
            ],
            any_order=True,
        )

        assert records == expected
        # sanity check - did we get this right?
        expected_call_count = len(
            [field for field, _, _ in fields_to_record if field in mutated_all_fields]
        )
        assert expected_call_count == 1
        assert mock_create_atomic_record.call_count == expected_call_count

    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_valid_input_speed_and_battery(
        self,
        mock_create_atomic_record: MagicMock,
    ):
        all_fields = {"speed": 70, "battery": 80}
        fields_to_record = [
            ("speed", PayloadType.NUMBER, all_fields["speed"]),
            ("battery", PayloadType.NUMBER, all_fields["battery"]),
        ]
        vin = "some_vin"
        last_updated_at = "2023-01-01T12:34:56Z"

        expected = [{"return_1"}, {"return_2"}]

        mock_create_atomic_record.side_effect = [
            {"return_1"},
            {"return_2"},
        ]

        records = create_records_from_fields(
            vin, last_updated_at, all_fields, fields_to_record
        )

        mock_create_atomic_record.assert_has_calls(
            [
                call(
                    source_timestamp=last_updated_at,
                    measurement_subject=vin,
                    measurement_publisher="bmw",
                    measurement_of=field,
                    measurement_data_type=ptype,
                    correlation_id=last_updated_at,
                    measurement_value=value,
                )
                for field, ptype, value in fields_to_record
                if field in all_fields
            ],
            any_order=True,
        )

        assert records == expected
        # sanity check - did we get this right?
        expected_call_count = len(
            [field for field, _, _ in fields_to_record if field in all_fields]
        )
        assert expected_call_count == 2
        assert mock_create_atomic_record.call_count == expected_call_count

    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_valid_input_bool_output(
        self,
        mock_create_atomic_record: MagicMock,
    ):
        all_fields = {"bool_item": 1, "other_bool_item": 0}
        fields_to_record = [
            ("bool_item", PayloadType.BOOLEAN, bool(all_fields["bool_item"])),
            (
                "other_bool_item",
                PayloadType.NUMBER,
                bool(all_fields["other_bool_item"]),
            ),
        ]
        vin = "some_vin"
        last_updated_at = "2023-01-01T12:34:56Z"

        expected = [{"return_1"}, {"return_2"}]

        mock_create_atomic_record.side_effect = [
            {"return_1"},
            {"return_2"},
        ]

        records = create_records_from_fields(
            vin, last_updated_at, all_fields, fields_to_record
        )

        mock_create_atomic_record.assert_has_calls(
            [
                call(
                    source_timestamp=last_updated_at,
                    measurement_subject=vin,
                    measurement_publisher="bmw",
                    measurement_of=field,
                    measurement_data_type=ptype,
                    correlation_id=last_updated_at,
                    measurement_value=value,
                )
                for field, ptype, value in fields_to_record
                if field in all_fields
            ],
            any_order=True,
        )

        assert records == expected
        # sanity check - did we get this right?
        expected_call_count = len(
            [field for field, _, _ in fields_to_record if field in all_fields]
        )
        assert expected_call_count == 2
        assert mock_create_atomic_record.call_count == expected_call_count

    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_extra_field_in_all_fields(self, mock_create_atomic_record: MagicMock):
        vin = "some_vin"
        last_updated_at = "2023-01-01T12:34:56Z"
        all_fields = {"speed": 70, "battery": 80, "extra_field": 100}
        fields_to_record = [
            ("speed", PayloadType.NUMBER, all_fields["speed"]),
            ("battery", PayloadType.NUMBER, all_fields["battery"]),
        ]
        expected = [{"return_1"}, {"return_2"}]

        mock_create_atomic_record.side_effect = [
            {"return_1"},
            {"return_2"},
            {"return_3"},  # shouldnt be used, so shoudlnt show up in return
        ]

        records = create_records_from_fields(
            vin, last_updated_at, all_fields, fields_to_record
        )

        mock_create_atomic_record.assert_has_calls(
            [
                call(
                    source_timestamp=last_updated_at,
                    measurement_subject=vin,
                    measurement_publisher="bmw",
                    measurement_of=field,
                    measurement_data_type=ptype,
                    correlation_id=last_updated_at,
                    measurement_value=value,
                )
                for field, ptype, value in fields_to_record
                if field in all_fields
            ],
            any_order=True,
        )

        assert records == expected
        # sanity check - did we get this right?
        expected_call_count = len(
            [field for field, _, _ in fields_to_record if field in all_fields]
        )
        assert expected_call_count == 2
        assert mock_create_atomic_record.call_count == expected_call_count

    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_exception_in_create_atomic_record(
        self, mock_create_atomic_record: MagicMock
    ):
        vin = "some_vin"
        last_updated_at = "2023-01-01T12:34:56Z"
        all_fields = {"speed": 70}
        fields_to_record = [("speed", PayloadType.NUMBER, 70)]

        # Setting side_effect to raise an exception
        mock_create_atomic_record.side_effect = Exception("An error occurred")

        # Since your original function doesn't raise the exception but prints it,
        # we're capturing stdout here to check if the exception was handled.
        with patch("builtins.print") as mocked_print:
            records = create_records_from_fields(
                vin, last_updated_at, all_fields, fields_to_record
            )

            # Assert that print was called with the exception message
            mocked_print.assert_called_with(
                "Failed to create atomic record for field speed: An error occurred"
            )

        # Assert that the returned records list is empty
        assert records == []


class TestGetVinFromMessage:
    @pytest.mark.parametrize(
        "messagebody, expected_vin",
        [
            ({"vin": "1234567890"}, "1234567890"),
            ({"vin": "ABCDE"}, "ABCDE"),
        ],
    )
    def test_valid_messagebody(self, messagebody, expected_vin):
        # When
        result = get_vin_from_message(messagebody)

        # Then
        assert result == expected_vin

    def test_missing_vin_key(self):
        # Given
        messagebody = {"not_vin": "1234567890"}

        # When & Then
        with pytest.raises(KeyError):
            get_vin_from_message(messagebody)


class TestGetLastUpdatedAtFromMessage:
    @pytest.mark.parametrize(
        "messagebody, expected_last_updated_at",
        [
            (
                {"state": {"lastUpdatedAt": "2021-01-01T12:34:56Z"}},
                "2021-01-01T12:34:56Z",
            ),
            (
                {"state": {"lastUpdatedAt": "2022-12-31T23:59:59Z"}},
                "2022-12-31T23:59:59Z",
            ),
        ],
    )
    def test_valid_messagebody(self, messagebody, expected_last_updated_at):
        # When
        result = get_last_updated_at_from_message(messagebody)

        # Then
        assert result == expected_last_updated_at

    def test_missing_state_key(self):
        # Given
        messagebody = {"not_state": {"lastUpdatedAt": "2021-01-01T12:34:56Z"}}

        # When & Then
        with pytest.raises(KeyError):
            get_last_updated_at_from_message(messagebody)

    def test_missing_last_updated_at_key(self):
        # Given
        messagebody = {"state": {"not_lastUpdatedAt": "2021-01-01T12:34:56Z"}}

        # When & Then
        with pytest.raises(KeyError):
            get_last_updated_at_from_message(messagebody)


class TestGetElectricChargingStateFromMessage:
    @pytest.mark.parametrize(
        "message, expected_output",
        [
            (
                {
                    "state": {
                        "electricChargingState": {
                            "chargingLevelPercent": 80,
                            "range": 120,
                            "isChargerConnected": 1,
                            "chargingStatus": "Charging",
                        }
                    }
                },
                {
                    "chargingLevelPercent": 80,
                    "range": 120,
                    "isChargerConnected": 1,
                    "chargingStatus": "Charging",
                },
            ),
            (
                {
                    "state": {
                        "electricChargingState": {
                            "chargingLevelPercent": 80,
                            "range": 120,
                        }
                    }
                },
                {"chargingLevelPercent": 80, "range": 120},
            ),
            ({"state": {"electricChargingState": {}}}, {}),
            ({"state": {"electricChargingState": None}}, {}),
            ({"state": {}}, {}),
            ({"state": {"other_item": "some_value"}}, {}),
            ({}, {}),
            ("Invalid", {}),
        ],
    )
    def test_get_electric_charging_state_from_message(self, message, expected_output):
        assert get_electric_charging_state_from_message(message) == expected_output


class TestGetCurrentMileageFromMessage:
    @pytest.mark.parametrize(
        "message, expected_output",
        [
            ({"state": {"currentMileage": 1000}}, {"currentMileage": 1000}),
            ({"state": {}}, None),
            ({}, None),
            ({"state": {"currentMileage": None}}, None),
            ("Invalid", None),
        ],
    )
    def test_get_current_mileage_from_message(self, message, expected_output):
        assert get_current_mileage_from_message(message) == expected_output

    def test_mileage_not_integer(self):
        message = {"state": {"currentMileage": "1000"}}
        with pytest.raises(
            TypeError, match="Invalid type for currentMileage: <class 'str'>"
        ):
            get_current_mileage_from_message(message)


class TestGetLocationFromMessage:
    def test_valid_location(self):
        message = {
            "state": {
                "location": {"coordinates": {"latitude": 12.3456, "longitude": 0.1234}}
            }
        }
        assert get_coordinates_from_message(message) == {
            "coordinates": {
                "latitude": 12.3456,
                "longitude": 0.1234,
            }
        }

    def test_missing_coordinates(self):
        message = {"state": {"location": {}}}
        assert get_coordinates_from_message(message) is None

    def test_missing_location(self):
        message = {"state": {}}
        assert get_coordinates_from_message(message) is None

    def test_missing_state(self):
        message = {}
        assert get_coordinates_from_message(message) is None


class TestValidateLatLong:
    def test_valid_lat_lon_with_floats(self):
        lat = 12.3456
        lng = 0.12345
        assert validate_lat_long(lat, lng) == [float(lat), float(lng)]

    def test_valid_lat_lon_with_ints(self):
        lat = 12
        lng = 1
        assert validate_lat_long(int(lat), int(lng)) == [float(lat), float(lng)]

    def test_invalid_latitude_type(self):
        lat = "invalid"
        lng = 0.1234
        with pytest.raises(
            TypeError, match="Invalid types for latitude and/or longitude:"
        ):
            validate_lat_long(lat, lng)

    def test_none_latitude_type(self):
        lat = None
        lng = 0.1234
        with pytest.raises(
            TypeError, match="Invalid types for latitude and/or longitude:"
        ):
            validate_lat_long(lat, lng)

    def test_invalid_longitude_type(self):
        lat = 12.3456
        lng = "invalid"
        with pytest.raises(
            TypeError, match="Invalid types for latitude and/or longitude:"
        ):
            validate_lat_long(lat, lng)

    def test_latitude_out_of_range(self):
        lat = 91.0
        lng = 0.1234
        with pytest.raises(ValueError, match="Invalid latitude value:"):
            validate_lat_long(lat, lng)

    def test_longitude_out_of_range(self):
        lat = 12.3456
        lng = 181.0
        with pytest.raises(ValueError, match="Invalid longitude value:"):
            validate_lat_long(lat, lng)
