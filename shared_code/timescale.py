import os
from typing import Any, List
import logging

import psycopg as psycopg
import azure.functions as func


from jsonschema import validate, ValidationError
import json


# load timeseries source schema
schema_path = os.sep.join(
    [os.path.dirname(os.path.abspath(__file__)), "timeseries.json"]
)
with open(schema_path) as f:
    schema = json.load(f)

def create_timescale_records_from_batch_of_events(
    conn: psycopg.Connection, record_set: str
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
            create_single_timescale_record(conn, record)
        except Exception as e:
            logging.error(f"Failed to create timescale records: {e}")
            unraised_errors.append(e)
    return unraised_errors or None


def create_single_timescale_record(
    conn: psycopg.Connection, record: dict[str, Any]
) -> None:
    """Create a single timescale record
    @param record: the record to create
    """
    # TODO: validate(instance=record, schema=schema)
    with conn.cursor() as cur:
        result = cur.execute(
            f"INSERT INTO conditions (timestamp, measurement_subject, correlation_id, measurement_of, {identify_data_column(record['measurement_data_type'])}) VALUES (%s, %s, %s, %s, %s)",  # noqa: E501
            (
                record["timestamp"],
                record["measurement_subject"],
                record["correlation_id"],
                record["measurement_of"],
                parse_measurement_value(record["measurement_data_type"], record["measurement_value"]),
            ),
        )
        if result.rowcount < 1:
            raise ValueError(f"Failed to insert record: {record}")
        elif result.rowcount > 1:
            raise ValueError(f"Inserted too many records: {record}")

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
