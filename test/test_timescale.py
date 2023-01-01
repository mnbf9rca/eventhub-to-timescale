import datetime
from unittest.mock import MagicMock
from typing import Any, Tuple
from dateutil import parser
import os
import sys
import importlib
import uuid
import psycopg
import pytest_mock
import json
import pytest

from jsonschema import ValidationError


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
    create_timescale_records_from_batch_of_events,
    validate_all_fields_in_record,
)

# when developing locally, use .env file to set environment variables
# TODO will move this to dotenv-vault in future
dotenv_spec = importlib.util.find_spec("dotenv")
if dotenv_spec is not None:
    print(f"loading dotenv from {os.getcwd()}")
    from dotenv import load_dotenv

    load_dotenv(verbose=True)


class db_helpers:
    """Helper functions for the database"""

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
            + "measurement_publisher"
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
        conn: psycopg.Connection, expected_record: dict[str, Any]
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
                f"SELECT {db_helpers.field_names()} FROM conditions WHERE correlation_id = %s",
                (expected_record["correlation_id"],),
            )
            actual_record = cur.fetchall()
            assert cur.rowcount == 1
            assert actual_record is not None
            db_helpers.check_record(actual_record[0], expected_record)


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
        self.conn = psycopg.connect(os.environ["TIMESCALE_CONNECTION_STRING"])

    def teardown_method(self):
        # delete all records from the DB
        with self.conn as conn:  # will close the connection after the blocks
            with conn.cursor() as cur:
                for correlation_id in self.list_of_test_correlation_ids:
                    cur.execute(
                        f"DELETE FROM conditions WHERE correlation_id = '{correlation_id}'"
                    )

    def test_of_type_number_with_int(self):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_publisher": "testpublisher",
            "measurement_of": "testname",
            "measurement_data_type": "number",
            "measurement_value": "1",
        }
        create_single_timescale_record(self.conn, sample_record)
        # check that the record was created in the DB by searching for correlation_id
        db_helpers.check_single_record_exists(self.conn, sample_record)

    def test_of_type_number_with_float(self):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_publisher": "testpublisher",
            "measurement_of": "testname",
            "measurement_data_type": "number",
            "measurement_value": "1.1",
        }
        create_single_timescale_record(self.conn, sample_record)
        # check that the record was created in the DB by searching for correlation_id
        db_helpers.check_single_record_exists(self.conn, sample_record)

    def test_of_type_string(self):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_of": "testname",
            "measurement_publisher": "testpublisher",
            "measurement_data_type": "string",
            "measurement_value": "test",
        }
        create_single_timescale_record(self.conn, sample_record)
        # check that the record was created in the DB by searching for correlation_id
        db_helpers.check_single_record_exists(self.conn, sample_record)

    def test_of_type_boolean(self):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_publisher": "testpublisher",
            "measurement_of": "testname",
            "measurement_data_type": "boolean",
            "measurement_value": "true",
        }
        create_single_timescale_record(self.conn, sample_record)
        # check that the record was created in the DB by searching for correlation_id
        db_helpers.check_single_record_exists(self.conn, sample_record)

    def test_of_type_boolean_with_false(self):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_publisher": "testpublisher",
            "measurement_of": "testname",
            "measurement_data_type": "boolean",
            "measurement_value": "false",
        }
        create_single_timescale_record(self.conn, sample_record)
        # check that the record was created in the DB by searching for correlation_id
        db_helpers.check_single_record_exists(self.conn, sample_record)

    def test_of_type_boolean_with_invalid_value(self):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_of": "testname",
            "measurement_publisher": "testpublisher",
            "measurement_data_type": "boolean",
            "measurement_value": "invalid",
        }
        with pytest.raises(ValueError, match=r".*Invalid boolean value.*"):
            create_single_timescale_record(self.conn, sample_record)

    def test_of_type_number_with_invalid_value(self):
        this_correlation_id: str = self.generate_correlation_id()
        sample_record = {
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "measurement_subject": "testsubject",
            "correlation_id": this_correlation_id,
            "measurement_of": "testname",
            "measurement_publisher": "testpublisher",
            "measurement_data_type": "number",
            "measurement_value": "invalid",
        }
        with pytest.raises(
            ValueError, match=r".*could not convert string to float: 'invalid'*"
        ):
            create_single_timescale_record(self.conn, sample_record)


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
            create_single_timescale_record(mock_conn, self.sample_record)

    def test_where_no_records_returned(self, mocker):
        mock_conn, _ = get_mock_conn_cursor(mocker)
        mock_result: MagicMock = mock_conn.cursor().__enter__().execute()
        with mock_result(new=mocker.PropertyMock):
            mock_result.rowcount = 0
        with pytest.raises(ValueError, match=r".*Failed to insert record*"):
            create_single_timescale_record(mock_conn, self.sample_record)

    def test_where_more_than_one_records_returned(self, mocker):
        mock_conn, _ = get_mock_conn_cursor(mocker)
        mock_result: MagicMock = mock_conn.cursor().__enter__().execute()
        with mock_result(new=mocker.PropertyMock):
            mock_result.rowcount = 3
        with pytest.raises(ValueError, match=r".*Inserted too many records.*"):
            create_single_timescale_record(mock_conn, self.sample_record)


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


class Test_create_timescale_records_from_batch_of_events:
    def test_from_batch_of_events(self, mocker: pytest_mock.MockFixture):
        mocked_create_single_timescale_record = mocker.patch(
            "shared_code.timescale.create_single_timescale_record", autospec=True
        )
        mock_conn, _ = get_mock_conn_cursor(mocker)
        test_value = stringify_test_data("timeseries_emon_electricitymeter")
        patch_value = None
        mocked_create_single_timescale_record.return_value = patch_value
        actual_value = create_timescale_records_from_batch_of_events(
            mock_conn, test_value
        )
        assert actual_value is None

    def test_from_batch_of_events_with_single_error(
        self, mocker: pytest_mock.MockFixture
    ):
        mocked_create_single_timescale_record = mocker.patch(
            "shared_code.timescale.create_single_timescale_record", autospec=True
        )
        mock_conn, _ = get_mock_conn_cursor(mocker)
        test_value = stringify_test_data("timeseries_emon_electricitymeter")
        patch_value = [None, None, None, Exception("test exception"), None, None, None]
        mocked_create_single_timescale_record.side_effect = patch_value
        actual_value = create_timescale_records_from_batch_of_events(
            mock_conn, test_value
        )
        assert len(actual_value) == 1
        assert actual_value[0] == patch_value[3]

    def test_from_batch_of_events_with_schema_error(
        self, mocker: pytest_mock.MockFixture
    ):

        mock_conn, _ = get_mock_conn_cursor(mocker)
        test_value = stringify_test_data(
            "timeseries_emon_electricitymeter_missing_timestamp"
        )
        actual_value = create_timescale_records_from_batch_of_events(
            mock_conn, test_value
        )
        assert len(actual_value) == 1
        assert isinstance(actual_value[0], ValidationError)

    def test_from_batch_of_events_where_create_single_timescale_record_errors(
        self, mocker: pytest_mock.MockFixture
    ):
        mocked_create_single_timescale_record = mocker.patch(
            "shared_code.timescale.create_single_timescale_record", autospec=True
        )
        mock_conn, _ = get_mock_conn_cursor(mocker)
        test_value = stringify_test_data("timeseries_emon_electricitymeter")
        side_effect = [
            Exception("test exception 1"),
            Exception("test exception 2"),
            Exception("test exception 3"),
            Exception("test exception 4"),
            Exception("test exception 5"),
            Exception("test exception 6"),
            Exception("test exception 7"),
        ]
        mocked_create_single_timescale_record.side_effect = side_effect
        actual_value = create_timescale_records_from_batch_of_events(
            mock_conn, test_value
        )
        assert len(actual_value) == 7
        for i in range(7):
            assert actual_value[i] == side_effect[i]


class Test_parse_measurement_value:
    def test_with_string_and_string(self):
        test_data_type = "string"
        test_value = "test"
        expected_value = "test"
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == str

    def test_with_string_and_number(self):
        test_data_type = "string"
        test_value = "1"
        expected_value = "1"
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == str

    def test_with_number_and_string(self):
        test_data_type = "number"
        test_value = "test"
        with pytest.raises(
            ValueError, match=r".*could not convert string to float: 'test'.*"
        ):
            parse_measurement_value(test_data_type, test_value)

    def test_with_number_and_number(self):
        test_data_type = "number"
        test_value = "1"
        expected_value = 1
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == float

    def test_with_boolean_and_string(self):
        test_data_type = "boolean"
        test_value = "test"
        with pytest.raises(ValueError, match=r".*Invalid boolean value: test.*"):
            parse_measurement_value(test_data_type, test_value)

    def test_with_boolean_and_true(self):
        test_data_type = "boolean"
        test_value = "true"
        expected_value = True
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == bool

    def test_with_boolean_and_false(self):
        test_data_type = "boolean"
        test_value = "false"
        expected_value = False
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == bool

    def test_with_number_and_float(self):
        test_data_type = "number"
        test_value = "1.1"
        expected_value = 1.1
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == float

    def test_with_number_and_negative_float(self):
        test_data_type = "number"
        test_value = "-1.1"
        expected_value = -1.1
        actual_value = parse_measurement_value(test_data_type, test_value)
        assert actual_value == expected_value
        assert type(actual_value) == float

    def test_with_invalid_measurement_type(self):
        test_data_type = "invalid"
        test_value = "test"
        with pytest.raises(
            ValueError, match=r".*Unknown measurement type: invalid*"
        ):
            parse_measurement_value(test_data_type, test_value)


class Test_identify_data_column:
    def test_with_number(self):
        test_data_type = "number"
        expected_column = "measurement_number"
        actual_column = identify_data_column(test_data_type)
        assert actual_column == expected_column

    def test_with_string(self):
        test_data_type = "string"
        expected_column = "measurement_string"
        actual_column = identify_data_column(test_data_type)
        assert actual_column == expected_column

    def test_with_boolean(self):
        test_data_type = "boolean"
        expected_column = "measurement_bool"
        actual_column = identify_data_column(test_data_type)
        assert actual_column == expected_column

    def test_with_invalid_data_type(self):
        test_data_type = "invalid"
        with pytest.raises(
            ValueError, match=r".*Unknown measurement type: invalid.*"
        ):
            identify_data_column(test_data_type)

    def test_passing_none(self):
        test_data_type = None
        with pytest.raises(
            ValueError, match=r".*Unknown measurement type: None.*"
        ):
            identify_data_column(test_data_type)

    def test_passing_empty_string(self):
        test_data_type = ""
        with pytest.raises(ValueError, match=r".*Unknown measurement type:.*"):
            identify_data_column(test_data_type)

    def test_passing_int(self):
        test_data_type = 1
        with pytest.raises(ValueError, match=r".*Unknown measurement type: 1.*"):
            identify_data_column(test_data_type)

    def test_passing_float(self):
        test_data_type = 1.1
        with pytest.raises(ValueError, match=r".*Unknown measurement type: 1.1.*"):
            identify_data_column(test_data_type)

    def test_passing_boolean(self):
        test_data_type = True
        with pytest.raises(
            ValueError, match=r".*Unknown measurement type: True.*"
        ):
            identify_data_column(test_data_type)


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
        with pytest.raises(
            ValueError, match=r".*Missing fields:.*measurement_value.*"
        ):
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
