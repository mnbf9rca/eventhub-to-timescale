import os
import sys
import importlib
from psycopg2.extensions import connection

import pytest

# add the shared_code directory to the path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shared_code import (  # noqa E402
    create_single_timescale_record,
    parse_measurement_value,
    TimescaleConnection,

)

# when developing locally, use .env file to set environment variables
# will move this to dotenv-vault in future
dotenv_spec = importlib.util.find_spec("dotenv")
if dotenv_spec is not None:
    print(f"loading dotenv from {os.getcwd()}")
    from dotenv import load_dotenv
    load_dotenv(verbose=True)

db = TimescaleConnection()
timescale_connection_string = os.environ["timescale_connection_string"]

def connect_to_database() -> connection:
    db.set_connection_string(timescale_connection_string)
    conn: connection = db.get_connection()
    return conn



class Test_create_single_timescale_record:
    def test_create_single_timescale_record(self):
        sample_record = {
            "timestamp": "2021-09-01T00:00:00.000Z",
            "measurement_subject": "test",
            "correlation_id": "test",
            "measurement_name": "test",
            "measurement_data_type": "number",
            "measurement_value": "1",
        }
        create_single_timescale_record(connect_to_database(), sample_record)

class Test_parse_measurement_value:
    def test_parse_measurement_value_with_string_and_string(self):
        test_data_type = "string"
        test_value = "test"
        expected_value = "test"
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == str

    def test_parse_measurement_value_with_string_and_number(self):
        test_data_type = "string"
        test_value = "1"
        expected_value = "1"
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == str

    def test_parse_measurement_value_with_number_and_string(self):
        test_data_type = "number"
        test_value = "test"
        with pytest.raises(ValueError):
            parse_measurement_value(test_data_type, test_value)


    def test_parse_measurement_value_with_number_and_number(self):
        test_data_type = "number"
        test_value = "1"
        expected_value = 1
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == float

    def test_parse_measurement_value_with_boolean_and_string(self):
        test_data_type = "boolean"
        test_value = "test"
        with pytest.raises(ValueError):
            parse_measurement_value(test_data_type, test_value)

    def test_parse_measurement_value_with_boolean_and_boolean(self):
        test_data_type = "boolean"
        test_value = "true"
        expected_value = True
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == bool

    def test_parse_measurement_value_with_boolean_and_boolean(self):
        test_data_type = "boolean"
        test_value = "false"
        expected_value = False
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == bool

    def test_parse_measurement_value_with_number_and_float(self):
        test_data_type = "number"
        test_value = "1.1"
        expected_value = 1.1
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == float

    def test_parse_measurement_value_with_number_and_negative_float(self):
        test_data_type = "number"
        test_value = "-1.1"
        expected_value = -1.1
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == float


