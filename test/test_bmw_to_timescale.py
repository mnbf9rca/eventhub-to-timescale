import pytest
import json
from unittest.mock import patch, Mock, call
from shared_code.bmw_to_timescale import (
    get_event_body,
    construct_messages,
    generate_atomic_record,
    get_electric_charging_state_from_message,
    get_current_mileage_from_message,
    get_location_from_message,
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
    @patch("shared_code.bmw_to_timescale.construct_location_message")
    @patch("shared_code.bmw_to_timescale.get_current_mileage_from_message")
    @patch("shared_code.bmw_to_timescale.get_electric_charging_state_from_message")
    @patch("shared_code.bmw_to_timescale.generate_atomic_record")
    def test_construct_messages(
        self,
        mock_generate_atomic_record,
        mock_get_electric_charging_state_from_message,
        mock_get_current_mileage_from_message,
        mock_construct_location_message,
    ):
        # Setup
        mock_get_electric_charging_state_from_message.return_value = {
            "chargingLevelPercent": 80,
            "range": 200,
            "isChargerConnected": True,
            "chargingStatus": "Charging",
        }
        mock_get_current_mileage_from_message.return_value = {"current_mileage": 120}
        mock_construct_location_message.return_value = "location_message"
        mock_generate_atomic_record.return_value = "atomic_record"

        vin = "some_vin"
        last_updated_at = "some_timestamp"
        event_object = {"some_key": "some_value"}

        # Fields and types as they are in the function
        fields_to_record = {
            "chargingLevelPercent": PayloadType.NUMBER,
            "range": PayloadType.NUMBER,
            "isChargerConnected": PayloadType.BOOLEAN,
            "chargingStatus": PayloadType.STRING,
            "current_mileage": PayloadType.NUMBER,
        }

        all_fields = {
            **mock_get_current_mileage_from_message.return_value,
            **mock_get_electric_charging_state_from_message.return_value,
        }
        common_fields_count = len(
            set(fields_to_record.keys()).intersection(set(all_fields.keys()))
        )

        # Exercise
        result = construct_messages(vin, last_updated_at, event_object)

        # Debug prints
        print("all_fields: ", all_fields)
        print("fields_to_record: ", fields_to_record.keys())
        print("common_fields_count: ", common_fields_count)

        # Verify
        mock_get_electric_charging_state_from_message.assert_called_once_with(
            event_object
        )
        mock_get_current_mileage_from_message.assert_called_once_with(event_object)
        mock_construct_location_message.assert_called_once_with(
            vin, last_updated_at, event_object
        )

        # Check if generate_atomic_record is called with the expected arguments
        mock_generate_atomic_record.assert_has_calls(
            [
                call(
                    vin,
                    last_updated_at,
                    field,
                    fields_to_record[field],
                    all_fields.get(field, None),
                )
                for field in fields_to_record.keys()
                if field in all_fields
            ],
            any_order=True,
        )

        # Check the resulting messages
        assert result == ["location_message"] + ["atomic_record"] * common_fields_count


    @patch("shared_code.bmw_to_timescale.construct_location_message")
    @patch("shared_code.bmw_to_timescale.get_current_mileage_from_message")
    @patch("shared_code.bmw_to_timescale.get_electric_charging_state_from_message")
    @patch("shared_code.bmw_to_timescale.generate_atomic_record")
    def test_construct_messages_missing_return_from_function(
        self,
        mock_generate_atomic_record,
        mock_get_electric_charging_state_from_message,
        mock_get_current_mileage_from_message,
        mock_construct_location_message,
    ):
        # Setup
        mock_get_electric_charging_state_from_message.return_value = {
            "chargingLevelPercent": 80,
            "range": 200,
            "isChargerConnected": True,
        }
        mock_get_current_mileage_from_message.return_value = {"current_mileage": 120}
        mock_construct_location_message.return_value = "location_message"
        mock_generate_atomic_record.return_value = "atomic_record"

        vin = "some_vin"
        last_updated_at = "some_timestamp"
        event_object = {"some_key": "some_value"}

        # Fields and types as they are in the function
        fields_to_record = {
            "chargingLevelPercent": PayloadType.NUMBER,
            "range": PayloadType.NUMBER,
            "isChargerConnected": PayloadType.BOOLEAN,
            "chargingStatus": PayloadType.STRING,
            "current_mileage": PayloadType.NUMBER,
        }

        all_fields = {
            **mock_get_current_mileage_from_message.return_value,
            **mock_get_electric_charging_state_from_message.return_value,
        }
        common_fields_count = len(
            set(fields_to_record.keys()).intersection(set(all_fields.keys()))
        )

        # Exercise
        result = construct_messages(vin, last_updated_at, event_object)

        # Debug prints
        print("all_fields: ", all_fields)
        print("fields_to_record: ", fields_to_record.keys())
        print("common_fields_count: ", common_fields_count)

        # Verify
        mock_get_electric_charging_state_from_message.assert_called_once_with(
            event_object
        )
        mock_get_current_mileage_from_message.assert_called_once_with(event_object)
        mock_construct_location_message.assert_called_once_with(
            vin, last_updated_at, event_object
        )

        # Check if generate_atomic_record is called with the expected arguments
        mock_generate_atomic_record.assert_has_calls(
            [
                call(
                    vin,
                    last_updated_at,
                    field,
                    fields_to_record[field],
                    all_fields.get(field, None),
                )
                for field in fields_to_record.keys()
                if field in all_fields
            ],
            any_order=True,
        )

        # Check the resulting messages
        assert result == ["location_message"] + ["atomic_record"] * common_fields_count

    @patch("shared_code.bmw_to_timescale.construct_location_message")
    @patch("shared_code.bmw_to_timescale.get_current_mileage_from_message")
    @patch("shared_code.bmw_to_timescale.get_electric_charging_state_from_message")
    @patch("shared_code.bmw_to_timescale.generate_atomic_record")
    def test_construct_messages_extra_return_from_function(
        self,
        mock_generate_atomic_record,
        mock_get_electric_charging_state_from_message,
        mock_get_current_mileage_from_message,
        mock_construct_location_message,
    ):
        # Setup
        mock_get_electric_charging_state_from_message.return_value = {
            "chargingLevelPercent": 80,
            "range": 200,
            "isChargerConnected": True,
            "some_new_field": "some_new_value",
        }
        mock_get_current_mileage_from_message.return_value = {"current_mileage": 120}
        mock_construct_location_message.return_value = "location_message"
        mock_generate_atomic_record.return_value = "atomic_record"

        vin = "some_vin"
        last_updated_at = "some_timestamp"
        event_object = {"some_key": "some_value"}

        # Fields and types as they are in the function
        fields_to_record = {
            "chargingLevelPercent": PayloadType.NUMBER,
            "range": PayloadType.NUMBER,
            "isChargerConnected": PayloadType.BOOLEAN,
            "chargingStatus": PayloadType.STRING,
            "current_mileage": PayloadType.NUMBER,
        }

        # all fields as they are in teh function
        all_fields = {
            **mock_get_current_mileage_from_message.return_value,
            **mock_get_electric_charging_state_from_message.return_value,
        }
        common_fields_count = len(
            set(fields_to_record.keys()).intersection(set(all_fields.keys()))
        )

        # Exercise
        result = construct_messages(vin, last_updated_at, event_object)

        # Debug prints
        print("all_fields: ", all_fields)
        print("fields_to_record: ", fields_to_record.keys())
        print("common_fields_count: ", common_fields_count)

        # Verify
        mock_get_electric_charging_state_from_message.assert_called_once_with(
            event_object
        )
        mock_get_current_mileage_from_message.assert_called_once_with(event_object)
        mock_construct_location_message.assert_called_once_with(
            vin, last_updated_at, event_object
        )

        # Check if generate_atomic_record is called with the expected arguments
        mock_generate_atomic_record.assert_has_calls(
            [
                call(
                    vin,
                    last_updated_at,
                    field,
                    fields_to_record[field],
                    all_fields.get(field, None),
                )
                for field in fields_to_record.keys()
                if field in all_fields
            ],
            any_order=True,
        )

        # Check the resulting messages
        assert result == ["location_message"] + ["atomic_record"] * common_fields_count

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

    def test_no_electricChargingState(self):
        message = {"state": {}}
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
        assert get_current_mileage_from_message(message) == None

    def test_missing_state_field(self):
        message = {}
        assert get_current_mileage_from_message(message) == None

    def test_mileage_not_integer(self):
        message = {"state": {"currentMileage": "1000"}}
        with pytest.raises(
            TypeError, match="Invalid type for currentMileage: <class 'str'>"
        ):
            get_current_mileage_from_message(message)

    def test_none_value(self):
        message = {"state": {"currentMileage": None}}
        assert get_current_mileage_from_message(message) == None

    def test_invalid_message_structure(self):
        message = "Invalid"
        assert get_current_mileage_from_message(message) is None


class TestGenerateAtomicRecord:
    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_generate_atomic_record_default_case(self, mock_create_atomic_record):
        # Setup
        mock_create_atomic_record.return_value = "mocked_atomic_record"
        vin = "some_vin"
        last_updated_at = "some_timestamp"
        field = "some_field"
        payload_type = "some_type"
        value = "some_value"

        # Exercise
        result = generate_atomic_record(
            vin, last_updated_at, field, payload_type, value
        )

        # Verify
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp=last_updated_at,
            measurement_subject=vin,
            measurement_publisher="bmw",
            measurement_of=field,
            measurement_data_type=payload_type,
            correlation_id=last_updated_at,
            measurement_value=value,
        )
        assert result == "mocked_atomic_record"

    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_generate_atomic_record_with_none_values(self, mock_create_atomic_record):
        # Setup
        mock_create_atomic_record.return_value = "mocked_atomic_record"

        # Exercise
        result = generate_atomic_record(None, None, None, None, None)

        # Verify
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp=None,
            measurement_subject=None,
            measurement_publisher="bmw",
            measurement_of=None,
            measurement_data_type=None,
            correlation_id=None,
            measurement_value=None,
        )
        assert result == "mocked_atomic_record"

    @patch("shared_code.bmw_to_timescale.sc.create_atomic_record")
    def test_generate_atomic_record_with_boolean_payload(
        self, mock_create_atomic_record
    ):
        # Setup
        mock_create_atomic_record.return_value = "mocked_atomic_record"
        payload_type = PayloadType.BOOLEAN  # This should match sc.PayloadType.BOOLEAN
        value = "1"

        # Exercise
        result = generate_atomic_record(
            "vin", "timestamp", "field", payload_type, value
        )

        # Verify
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp="timestamp",
            measurement_subject="vin",
            measurement_publisher="bmw",
            measurement_of="field",
            measurement_data_type=payload_type,
            correlation_id="timestamp",
            measurement_value=True,  # Value should be converted to boolean
        )
        assert result == "mocked_atomic_record"


class TestGetLocationFromMessage:
    def test_valid_location(self):
        message = {
            "state": {
                "location": {"coordinates": {"latitude": 51.6269, "longitude": -0.1385}}
            }
        }
        assert get_location_from_message(message) == {"lat": 51.6269, "lon": -0.1385}

    def test_missing_coordinates(self):
        message = {"state": {"location": {}}}
        assert get_location_from_message(message) is None

    def test_missing_location(self):
        message = {"state": {}}
        assert get_location_from_message(message) is None

    def test_missing_state(self):
        message = {}
        assert get_location_from_message(message) is None

    def test_invalid_latitude_type(self):
        message = {
            "state": {
                "location": {
                    "coordinates": {"latitude": "invalid", "longitude": -0.1385}
                }
            }
        }
        with pytest.raises(
            TypeError, match="Invalid types for latitude and/or longitude:"
        ):
            get_location_from_message(message)

    def test_invalid_longitude_type(self):
        message = {
            "state": {
                "location": {
                    "coordinates": {"latitude": 51.6269, "longitude": "invalid"}
                }
            }
        }
        with pytest.raises(
            TypeError, match="Invalid types for latitude and/or longitude:"
        ):
            get_location_from_message(message)

    def test_latitude_out_of_range(self):
        message = {
            "state": {
                "location": {"coordinates": {"latitude": 91.0, "longitude": -0.1385}}
            }
        }
        with pytest.raises(ValueError, match="Invalid latitude value:"):
            get_location_from_message(message)

    def test_longitude_out_of_range(self):
        message = {
            "state": {
                "location": {"coordinates": {"latitude": 51.6269, "longitude": 181.0}}
            }
        }
        with pytest.raises(ValueError, match="Invalid longitude value:"):
            get_location_from_message(message)
