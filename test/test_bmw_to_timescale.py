import pytest
from copy import copy
import json
from unittest.mock import patch, Mock, call, MagicMock
from shared_code.bmw_to_timescale import (
    get_event_body,
    construct_messages,
    get_electric_charging_state_from_message,
    get_current_mileage_from_message,
    get_coordinates_from_message,
    validate_lat_long,
    create_records_from_fields,
)
from shared_code import PayloadType


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
        mock_get_current_mileage_from_message.return_value = {"current_mileage": 120}
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
                "current_mileage",
                PayloadType.NUMBER,
                all_fields["current_mileage"],
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


class TestGetElectricChargingStateFromMessage:
    def test_all_fields_present(self):
        message = {
            "state": {
                "electricChargingState": {
                    "chargingLevelPercent": 80,
                    "range": 120,
                    "isChargerConnected": 1,
                    "chargingStatus": "Charging",
                }
            }
        }
        expected = {
            "chargingLevelPercent": 80,
            "range": 120,
            "isChargerConnected": 1,
            "chargingStatus": "Charging",
        }
        assert get_electric_charging_state_from_message(message) == expected

    def test_some_fields_missing(self):
        message = {
            "state": {
                "electricChargingState": {"chargingLevelPercent": 80, "range": 120}
            }
        }
        expected = {"chargingLevelPercent": 80, "range": 120}
        assert get_electric_charging_state_from_message(message) == expected

    def test_all_fields_missing(self):
        message = {"state": {"electricChargingState": {}}}
        expected = {}
        assert get_electric_charging_state_from_message(message) == expected

    def test_none_value(self):
        message = {"state": {"electricChargingState"}}
        expected = {}
        assert get_electric_charging_state_from_message(message) == expected

    def test_none_value_2(self):
        message = {"state": {"electricChargingState": None}}
        expected = {}
        assert get_electric_charging_state_from_message(message) == expected

    def test_no_electricChargingState(self):
        message = {"state": {}}
        expected = {}
        assert get_electric_charging_state_from_message(message) == expected

    def test_no_electricChargingStateButOtherItems(self):
        message = {"state": {"other_item": "some_value"}}
        expected = {}
        assert get_electric_charging_state_from_message(message) == expected

    def test_no_state(self):
        message = {}
        expected = {}
        assert get_electric_charging_state_from_message(message) == expected

    def test_invalid_message_structure(self):
        message = "Invalid"
        expected = {}
        assert get_electric_charging_state_from_message(message) == expected


class TestGetCurrentMileageFromMessage:
    def test_valid_mileage(self):
        message = {"state": {"currentMileage": 1000}}
        assert get_current_mileage_from_message(message) == {"current_mileage": 1000}

    def test_missing_mileage_field(self):
        message = {"state": {}}
        assert get_current_mileage_from_message(message) is None

    def test_missing_state_field(self):
        message = {}
        assert get_current_mileage_from_message(message) is None

    def test_mileage_not_integer(self):
        message = {"state": {"currentMileage": "1000"}}
        with pytest.raises(
            TypeError, match="Invalid type for currentMileage: <class 'str'>"
        ):
            get_current_mileage_from_message(message)

    def test_none_value(self):
        message = {"state": {"currentMileage": None}}
        assert get_current_mileage_from_message(message) is None

    def test_invalid_message_structure(self):
        message = "Invalid"
        assert get_current_mileage_from_message(message) is None


class TestGetLocationFromMessage:
    def test_valid_location(self):
        message = {
            "state": {
                "location": {"coordinates": {"latitude": 12.3456, "longitude": 0.1234}}
            }
        }
        assert get_coordinates_from_message(message) == {"coordinates" : {
            "latitude": 12.3456,
            "longitude": 0.1234,
        }}

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
        lat = int(12)
        lng = int(1)
        assert validate_lat_long(lat, lng) == [float(lat), float(lng)]

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
