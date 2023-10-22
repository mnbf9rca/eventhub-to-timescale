import os
from typing import Any
import logging
from dotenv_vault import load_dotenv

import psycopg as psycopg


from jsonschema import validate, ValidationError
import json

if not load_dotenv():
    logging.error("Failed to load dotenv")
    raise Exception("Failed to load dotenv")


def get_connection_string() -> str:
    """Get the connection string for the timescale database
    @return: the connection string
    """
    if connection_string := os.environ.get("TIMESCALE_CONNECTION_STRING"):
        return connection_string
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
    return f"dbname={os.environ['POSTGRES_DB']} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']} host={os.environ['POSTGRES_HOST']} port={os.environ['POSTGRES_PORT']}"  # noqa: E501


def get_table_name() -> str:
    """Get the table name for the timescale database
    @return: the table name
    """
    if table_name := os.environ.get("TABLE_NAME"):
        return table_name
    else:
        raise ValueError("Missing required environment variable: TABLE_NAME")


# load timeseries source schema
schema_path = os.sep.join(
    [os.path.dirname(os.path.abspath(__file__)), "timeseries.json"]
)
with open(schema_path) as f:
    schema = json.load(f)


def create_timescale_records_from_batch_of_events(
    conn: psycopg.Connection, record_set: str, table_name: str
) -> None:
    """Create timescale records from events
    @param events: the events to create records from
    """
    unraised_errors = []
    unwrapped_records = list(map(json.loads, json.loads(record_set)))
    try:
        validate(instance=unwrapped_records, schema=schema)
    except ValidationError as e:
        logging.error(f"Failed to validate schema of record set: {e}")
        unraised_errors.append(e)
        return unraised_errors

    for record in unwrapped_records:
        try:
            create_single_timescale_record(conn, record, table_name)
        except Exception as e:
            logging.error(f"Failed to create timescale records: {e}")
            unraised_errors.append(e)
    return unraised_errors or None


def create_single_timescale_record(
    conn: psycopg.Connection, record: dict[str, Any], table_name: str
) -> None:
    """Create a single timescale record
    @param record: the record to create
    """
    # TODO: validate(instance=record, schema=schema)
    validate_all_fields_in_record(record)
    with conn.cursor() as cur:
        result = cur.execute(
            f"INSERT INTO {table_name} (timestamp, measurement_publisher, measurement_subject, correlation_id, measurement_of, {identify_data_column(record['measurement_data_type'])}) VALUES (%s, %s, %s, %s, %s, %s)",  # noqa: E501
            (
                record["timestamp"],
                record["measurement_publisher"],
                record["measurement_subject"],
                record["correlation_id"],
                record["measurement_of"],
                parse_measurement_value(
                    record["measurement_data_type"], record["measurement_value"]
                ),
            ),
        )
        if result.rowcount < 1:
            raise ValueError(f"Failed to insert record: {record}")
        elif result.rowcount > 1:
            raise ValueError(f"Inserted too many records: {record}")


def validate_all_fields_in_record(record: dict[str, Any]) -> None:
    """Validate at least the required fields are in the record
    @param record: the record to validate
    """
    required_fields = [
        "timestamp",
        "measurement_publisher",
        "measurement_subject",
        "correlation_id",
        "measurement_of",
        "measurement_data_type",
        "measurement_value",
    ]
    if missing_fields := [field for field in required_fields if field not in record]:
        raise ValueError(f"Missing fields: {missing_fields}")


def identify_data_column(measurement_type: str) -> str:
    """Identify the column name for the data
    @param measurement_type: the measurement type
    @return: the column name for the data
    """
    if measurement_type == "boolean":
        return "measurement_bool"
    elif measurement_type == "number":
        return "measurement_number"
    elif measurement_type == "string":
        return "measurement_string"
    else:
        raise ValueError(f"Unknown measurement type: {measurement_type}")


def parse_measurement_value(measurement_type: str, measurement_value: str) -> Any:
    """Parse the measurement value
    @param measurement_type: the measurement type
    @param measurement_value: the measurement value
    @return: the parsed measurement value
    """
    if measurement_type == "boolean":
        if measurement_value.lower() in {"true", "false"}:
            return measurement_value.lower() == "true"
        else:
            raise ValueError(f"Invalid boolean value: {measurement_value}")
    elif measurement_type == "number":
        return float(measurement_value)
    elif measurement_type == "string":
        return measurement_value
    else:
        raise ValueError(f"Unknown measurement type: {measurement_type}")
