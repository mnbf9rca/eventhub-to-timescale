import datetime
from unittest.mock import MagicMock, patch
from typing import Any, Tuple
from dateutil import parser
from dotenv import load_dotenv

import os
import sys
import uuid
import psycopg
import pytest_mock
import json
import pytest


# import test data
from get_test_data import load_test_data

test_data = load_test_data()


# add the shared_code directory to the path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shared_code import (  # noqa E402
    create_single_timescale_record,
    parse_measurement_value,
    identify_data_column,
    # create_timescale_records_from_batch_of_events,
    validate_all_fields_in_record,
    get_table_name,
    get_connection_string,
    parse_to_geopoint,
)

# when developing locally, use .env file to set environment variables

load_dotenv(verbose=True)
# dotenv_spec = importlib.util.find_spec("dotenv")
# if dotenv_spec is not None:
#     print(f"loading dotenv from {os.getcwd()}")


class db_helpers:
    """Helper functions for the database"""

    test_table_name = os.environ["TABLE_NAME"]

    @staticmethod
    def get_connection_string_for_test():
        required_env_vars = [
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "POSTGRES_HOST",
            "POSTGRES_PORT",
        ]
        if missing_env_vars := [
            env_var for env_var in required_env_vars if env_var not in os.environ
        ]:
            raise ValueError(
                f"Missing required environment variables: {missing_env_vars}"
            )
        connstring = f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']} port={os.environ['POSTGRES_PORT']}"  # noqa: E501
        print("connstring:", connstring)
        return connstring

    @staticmethod
    def field_names():
        """return the field names for the conditions table"""
        return (
            "timestamp, "
            + "measurement_subject, "
            + "measurement_number, "
            + "measurement_of, "
            + "measurement_string, "
            + "correlation_id, "
            + "measurement_bool, "
            + "measurement_publisher, "
            + "measurement_unique_id, "
            + "ST_AsText(measurement_location) as measurement_location"
        )

    @staticmethod
    def check_record(record: Tuple[Any, ...], expected_record: dict[str, Any]):
        """Check that the record matches the expected record
        @param record: the record to check
        @param expected_record: the expected record
        """
        if expected_record["measurement_data_type"] == "number":
            assert record[2] == float(expected_record["measurement_value"])
            none_fields = [4, 6]
        elif expected_record["measurement_data_type"] == "string":
            assert record[4] == expected_record["measurement_value"]
            none_fields = [2, 6]
        elif expected_record["measurement_data_type"] == "boolean":
            assert record[6] is (expected_record["measurement_value"].lower() == "true")
            none_fields = [2, 4]
        elif expected_record["measurement_data_type"] == "geography":
            assert record[9] == expected_record["measurement_value"]
            none_fields = [4, 6]
        else:
            raise ValueError("invalid measurement_data_type")

        for field in none_fields:
            assert record[field] is None
        assert record[0] == parser.parse(expected_record["timestamp"])
        assert record[1] == expected_record["measurement_subject"]
        assert record[3] == expected_record["measurement_of"]
        assert record[5] == expected_record["correlation_id"]
        assert record[7] == expected_record["measurement_publisher"]

    @staticmethod
    def check_single_record_exists(
        conn: psycopg.Connection, expected_record: dict[str, Any], table_name: str
    ):
        """Check that the record exists in the database
        @param conn: the database connection
        @param expected_record: the expected record
        """
        # check that the connection is still open - psycopg will close it
        # if its used in a with block inside the method or test
        # https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html#with-connection
        # which is itself a failure as we want to reuse the connection
        assert (
            conn.closed is False
        ), "The connection is closed. Check that you are not using a with conn block inside the method or test"
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {db_helpers.field_names()} FROM {table_name} WHERE correlation_id = %s",
                (expected_record["correlation_id"],),
            )
            actual_record = cur.fetchall()
            assert cur.rowcount == 1
            assert actual_record is not None
            db_helpers.check_record(actual_record[0], expected_record)


class Test_get_table_name:
    def test_get_table_name_success(self):
        with patch.dict(os.environ, {"TABLE_NAME": "test_table"}):
            assert get_table_name() == "test_table"

    def test_get_table_name_failure(self):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as excinfo:
                get_table_name()
            assert (
                str(excinfo.value)
                == "Missing required environment variable: TABLE_NAME"
            )


class Test_get_connection_string:
    def test_get_connection_string_from_components(self):
        mock_env_vars = {
            "POSTGRES_DB": "test_db",
            "POSTGRES_USER": "test_user",
            "POSTGRES_PASSWORD": "test_password",
            "POSTGRES_HOST": "test_host",
            "POSTGRES_PORT": "test_port",
        }
        with patch.dict(os.environ, mock_env_vars, clear=True):
            expected_conn_string = "dbname=test_db user=test_user password=test_password host=test_host port=test_port"
            assert get_connection_string() == expected_conn_string

    def test_get_connection_string_missing_env_vars(self):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as excinfo:
                get_connection_string()
            assert str(excinfo.value).startswith(
                "Missing required environment variables:"
            )


class Test_create_single_timescale_record_against_actual_database:
    conn: psycopg.Connection = None
    list_of_test_correlation_ids = []

    def generate_correlation_id(self) -> str:
        # do this here so that we can store the value and use it in the teardown method
        correlation_id = f"test_{str(uuid.uuid4())}"
        self.list_of_test_correlation_ids.append(correlation_id)
        return correlation_id

    def setup_method(self):
        # create a connection to the database just for this test class, reuse it for all tests
        self.conn = psycopg.connect(db_helpers.get_connection_string_for_test())

    def teardown_method(self):
        # delete all records from the DB
        with self.conn as conn:  # will close the connection after the blocks
            with conn.cursor() as cur:
                for correlation_id in self.list_of_test_correlation_ids:
                    cur.execute(
                        f"DELETE FROM {db_helpers.test_table_name} WHERE correlation_id = '{correlation_id}'"
                    )

    @pytest.mark.parametrize(
        "measurement_value, data_type",
        [
            ("1", "number"),
            ("1.1", "number"),
            ("test", "string"),
            ("true", "boolean"),
            ("false", "boolean"),
            ("40.7128,-74.0060", "geography"),
        ],
    )
    def test_create_single_timescale_record(self, measurement_value, data_type):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_publisher": "testpublisher",
            "measurement_of": "testname",
            "measurement_data_type": data_type,
            "measurement_value": measurement_value,
        }

        # Convert latitude and longitude to POINT format for geography data type
        if data_type == "geography":
            latlon_values = measurement_value.split(",")
            latitude, longitude = map(float, latlon_values)
            expected_value = f"POINT({longitude} {latitude})"
        else:
            expected_value = measurement_value

        expected_record = {
            **sample_record,
            "measurement_value": expected_value,
        }

        create_single_timescale_record(
            self.conn, json.dumps(sample_record), db_helpers.test_table_name
        )
        db_helpers.check_single_record_exists(
            self.conn, expected_record, db_helpers.test_table_name
        )

    @pytest.mark.parametrize(
        "measurement_value, data_type, expected_error, expected_message",
        [
            ("invalid", "boolean", ValueError, r".*Invalid boolean value.*"),
            ("invalid", "number", ValueError, r".*Invalid number value: invalid.*"),
        ],
    )
    def test_create_single_timescale_record_with_invalid_value(
        self, measurement_value, data_type, expected_error, expected_message
    ):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_publisher": "testpublisher",
            "measurement_of": "testname",
            "measurement_data_type": data_type,
            "measurement_value": measurement_value,
        }
        with pytest.raises(expected_error, match=expected_message):
            create_single_timescale_record(
                self.conn, json.dumps(sample_record), db_helpers.test_table_name
            )


class Test_create_single_timescale_record_with_mock:
    sample_record = {
        "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "measurement_subject": "testsubject",
        "correlation_id": "mocked_correlation_id",
        "measurement_of": "testname",
        "measurement_data_type": "number",
        "measurement_publisher": "testpublisher",
        "measurement_value": "1",
        "measurement_publisher": "testpublisher",
    }

    def test_where_cursor_raises_exception(self, mocker):
        mock_conn, mock_cursor = get_mock_conn_cursor(mocker)
        mock_execute = mock_cursor.__enter__()
        mock_execute.execute.side_effect = Exception("test exception")
        with pytest.raises(Exception, match=r".*test exception*"):
            create_single_timescale_record(
                mock_conn, json.dumps(self.sample_record), db_helpers.test_table_name
            )

    def test_where_no_records_returned(self, mocker):
        mock_conn, _ = get_mock_conn_cursor(mocker)
        mock_result: MagicMock = mock_conn.cursor().__enter__().execute()
        with mock_result(new=mocker.PropertyMock):
            mock_result.rowcount = 0
        with pytest.raises(ValueError, match=r".*Failed to insert record*"):
            create_single_timescale_record(
                mock_conn, json.dumps(self.sample_record), db_helpers.test_table_name
            )

    def test_where_more_than_one_records_returned(self, mocker):
        mock_conn, _ = get_mock_conn_cursor(mocker)
        mock_result: MagicMock = mock_conn.cursor().__enter__().execute()
        with mock_result(new=mocker.PropertyMock):
            mock_result.rowcount = 3
        with pytest.raises(ValueError, match=r".*Inserted too many records.*"):
            create_single_timescale_record(
                mock_conn, json.dumps(self.sample_record), db_helpers.test_table_name
            )


def stringify_test_data(test_dataset_name: str) -> str:
    """loads test data from json file and returns it as a string"""
    return json.dumps(list(map(json.dumps, test_data[test_dataset_name]["body"])))


def get_mock_conn_cursor(
    mocker: pytest_mock.MockFixture,
) -> Tuple[MagicMock, MagicMock]:
    """creates a mock connection and cursor and returns them"""
    mock_conn_o = mocker.patch("psycopg.connect", autospec=True)
    mock_conn = mock_conn_o.return_value
    return mock_conn, mock_conn.cursor()


# class Test_create_timescale_records_from_batch_of_events:
#     @pytest.mark.parametrize(
#         "timeseries_emon_electricitymeter",
#         [
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_cumulative", "measurement_value": 5100.748, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_day", "measurement_value": 16.138, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_week", "measurement_value": 43.873, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_month", "measurement_value": 649.307, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_unitrate", "measurement_value": 0.3788, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_standingcharge", "measurement_value": 0.4458, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "power_value", "measurement_value": 0.679, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#         ],
#     )
#     def test_from_batch_of_events(
#         self, timeseries_emon_electricitymeter, mocker: pytest_mock.MockFixture
#     ):
#         # Mocking create_single_timescale_record
#         mocked_create_single_timescale_record = mocker.patch(
#             "shared_code.timescale.create_single_timescale_record", autospec=True
#         )

#         # Getting mock connection and cursor
#         mock_conn, _ = get_mock_conn_cursor(mocker)

#         # Stringified test data
#         test_value = (
#             timeseries_emon_electricitymeter  # Use the parameterized input here
#         )
#         patch_value = None
#         mocked_create_single_timescale_record.return_value = patch_value

#         # Function under test
#         actual_value = create_timescale_records_from_batch_of_events(
#             mock_conn, test_value, db_helpers.test_table_name
#         )

#         # Assertion
#         assert actual_value is None

#     @pytest.mark.parametrize(
#         "timeseries_emon_electricitymeter",
#         [
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_cumulative", "measurement_value": 5100.748, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_day", "measurement_value": 16.138, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_week", "measurement_value": 43.873, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_month", "measurement_value": 649.307, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_unitrate", "measurement_value": 0.3788, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_standingcharge", "measurement_value": 0.4458, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#             '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "power_value", "measurement_value": 0.679, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501
#         ],
#     )
#     def test_from_batch_of_events_with_single_error(
#         self, timeseries_emon_electricitymeter, mocker: pytest_mock.MockFixture
#     ):
#         # Mocking create_single_timescale_record
#         mocked_create_single_timescale_record = mocker.patch(
#             "shared_code.timescale.create_single_timescale_record", autospec=True
#         )

#         # Getting mock connection and cursor
#         mock_conn, _ = get_mock_conn_cursor(mocker)

#         # Stringified test data
#         test_value = (
#             timeseries_emon_electricitymeter  # Use the parameterized input here
#         )

#         # Patched return values, including one exception
#         patch_value = [None, None, None, Exception("test exception"), None, None, None]
#         mocked_create_single_timescale_record.side_effect = patch_value

#         # Function under test
#         actual_value = create_timescale_records_from_batch_of_events(
#             mock_conn, test_value, db_helpers.test_table_name
#         )

#         # Assertions
#         assert len(actual_value) == 1
#         assert actual_value[0] == patch_value[3]

#     def test_from_batch_of_events_with_schema_error(
#         self, mocker: pytest_mock.MockFixture
#     ):
#         # Mocking and setup
#         mock_conn, _ = get_mock_conn_cursor(mocker)

#         # Use the parameterized input here
#         test_value = '{"measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_cumulative", "measurement_value": 5100.748, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}'  # noqa: E501

#         # Function under test
#         actual_value = create_timescale_records_from_batch_of_events(
#             mock_conn, test_value, db_helpers.test_table_name
#         )

#         # Assertions
#         assert len(actual_value) == 1
#         assert isinstance(actual_value[0], ValidationError)


#     def test_from_batch_of_events_where_create_single_timescale_record_errors(
#         self, mocker: pytest_mock.MockFixture
#     ):
#         mocked_create_single_timescale_record = mocker.patch(
#             "shared_code.timescale.create_single_timescale_record", autospec=True
#         )
#         mock_conn, _ = get_mock_conn_cursor(mocker)
#         test_value = '{"timestamp": "2022-12-27T15:23:10Z", "measurement_subject": "electricitymeter", "measurement_publisher": "emon", "measurement_of": "import_cumulative", "measurement_value": 5100.748, "measurement_data_type": "number", "correlation_id": "2022-12-27T15:23:18.282000-132527"}',  # noqa: E501

#         side_effect = [
#             Exception("test exception 1"),
#         ]
#         mocked_create_single_timescale_record.side_effect = side_effect
#         with pytest.raises(Exception) as exc_info:
#             create_timescale_records_from_batch_of_events(
#                 mock_conn, test_value, db_helpers.test_table_name
#             )
#         assert str(exc_info.value) == "test exception 1"


class Test_parse_measurement_value:
    # Tests for valid measurement types and values
    @pytest.mark.parametrize(
        "test_data_type, test_value, expected_value, expected_type",
        [
            ("string", "test", "test", str),
            ("string", "1", "1", str),
            ("number", "1", 1, float),
            ("boolean", "true", True, bool),
            ("boolean", "false", False, bool),
            ("boolean", True, True, bool),
            ("boolean", False, False, bool),
            ("number", "1.1", 1.1, float),
            ("number", "-1.1", -1.1, float),
        ],
    )
    def test_with_valid_measurement_types(
        self, test_data_type, test_value, expected_value, expected_type
    ):
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert isinstance(actual_value, expected_type)

    # Tests for invalid measurement types and values
    @pytest.mark.parametrize(
        "test_data_type, test_value, expected_error, expected_message",
        [
            ("invalid", "test", ValueError, r".*Unknown measurement type: invalid*"),
            ("number", "test", ValueError, r".*Invalid number value: test.*"),
            ("boolean", "test", ValueError, r".*Invalid boolean value: test.*"),
            ("boolean", 7, ValueError, r".*Invalid boolean value: 7.*"),
        ],
    )
    def test_with_invalid_measurement_types(
        self, test_data_type, test_value, expected_error, expected_message
    ):
        with pytest.raises(expected_error, match=expected_message):
            parse_measurement_value(test_data_type, test_value)


class Test_parse_to_geopointt:
    # Tests for valid geography values
    @pytest.mark.parametrize(
        "input_value, expected_output",
        [
            ("40.7128,-74.0062", "SRID=4326;POINT(-74.0062 40.7128)"),
            ([40.7128, -74.0062], "SRID=4326;POINT(-74.0062 40.7128)"),
            (["40.7128", "-74.0062"], "SRID=4326;POINT(-74.0062 40.7128)"),
        ],
    )
    def test_valid_geography_values(self, input_value, expected_output):
        assert parse_to_geopoint(input_value) == expected_output

    # Tests for invalid geography values
    @pytest.mark.parametrize(
        "input_value, expected_error, expected_message",
        [
            (
                {"lat": 40.7128, "lon": -74.0062},
                ValueError,
                "Invalid input type or format:",
            ),
            ("100.0,-74.0060", ValueError, "Invalid latitude value:"),
            ("-74.0060,190.0", ValueError, "Invalid longitude value:"),
            ("100.0,-200.0", ValueError, "Invalid latitude value:"),
            ("latitude,longitude", ValueError, "Invalid geography value:"),
            ("40.7128,-74.0060,100", ValueError, "Invalid geography value:"),
            ([40.7128, -74.0060, 100], ValueError, "Invalid input type or format:"),
            ("123", ValueError, "Invalid geography value:"),
            (True, ValueError, "Invalid input type or format:"),
            ("", ValueError, "Invalid geography value:"),
            (",", ValueError, "Invalid geography value:"),
        ],
    )
    def test_invalid_geography_values(
        self, input_value, expected_error, expected_message
    ):
        with pytest.raises(expected_error, match=expected_message):
            parse_to_geopoint(input_value)


class Test_identify_data_column:
    @pytest.mark.parametrize(
        "input_value, expected_column",
        [
            ("number", "measurement_number"),
            ("string", "measurement_string"),
            ("boolean", "measurement_bool"),
            ("geography", "measurement_location"),
        ],
    )
    def test_identify_data_column_valid(self, input_value, expected_column):
        actual_column = identify_data_column(input_value)
        assert actual_column == expected_column

    @pytest.mark.parametrize(
        "input_data_type, expected_error, expected_message",
        [
            ("invalid", ValueError, r".*Unknown measurement type: invalid.*"),
            (None, ValueError, r".*Measurement type must be a string.*"),
            ("", ValueError, r".*Unknown measurement type:.*"),
            (1, ValueError, r".*Measurement type must be a string.*"),
            (1.1, ValueError, r".*Measurement type must be a string.*"),
            (True, ValueError, r".*Measurement type must be a string.*"),
        ],
    )
    def test_identify_data_column_invalid(
        self, input_data_type, expected_error, expected_message
    ):
        with pytest.raises(expected_error, match=expected_message):
            identify_data_column(input_data_type)


class Test_validate_all_fields_in_record:
    def test_with_valid_record(self):
        test_record = {
            "timestamp": "2021-01-01T00:00:00.000Z",
            "measurement_publisher": "test",
            "measurement_subject": "test",
            "correlation_id": "test",
            "measurement_of": "test",
            "measurement_data_type": "number",
            "measurement_value": "1",
        }
        actual_value = validate_all_fields_in_record(test_record)
        assert actual_value is None

    def test_with_one_missing_field(self):
        test_record = {
            "timestamp": "2021-01-01T00:00:00.000Z",
            "measurement_publisher": "test",
            "measurement_subject": "test",
            "correlation_id": "test",
            "measurement_of": "test",
            "measurement_data_type": "number",
        }
        # missing measurement_bool
        with pytest.raises(ValueError, match=r".*Missing fields:.*measurement_value.*"):
            validate_all_fields_in_record(test_record)

    def test_with_multiple_missing_fields(self):
        test_record = {
            "timestamp": "2021-01-01T00:00:00.000Z",
            "measurement_publisher": "test",
            "measurement_subject": "test",
            "correlation_id": "test",
            "measurement_of": "test",
        }
        # missing measurement_data_type, measurement_value
        with pytest.raises(
            ValueError,
            match=r"^(?=.*Missing fields:)(?=.*measurement_data_type)(?=.*measurement_value).*$",
        ):
            validate_all_fields_in_record(test_record)

    def test_with_additional_fields(self):
        test_record = {
            "timestamp": "2021-01-01T00:00:00.000Z",
            "measurement_publisher": "test",
            "measurement_subject": "test",
            "correlation_id": "test",
            "measurement_of": "test",
            "measurement_data_type": "number",
            "measurement_value": "1",
            "additional_field": "test",
        }
        actual_value = validate_all_fields_in_record(test_record)
        assert actual_value is None
