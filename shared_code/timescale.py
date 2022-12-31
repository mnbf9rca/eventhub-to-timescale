import os
from typing import Any, List

import psycopg as psycopg

from jsonschema import validate
import json



# create a singleton method which returns a connection to the timescale database using psycopg2 and the
# connection string TIMESCALE_CONNECTION_STRING
class TimescaleConnection:
    def __init__(self, connection_string: str = None):
        self.connection_string = connection_string
        self.connection = None

    def set_connection_string(self, connection_string: str) -> None:
        self.connection_string = connection_string

    def get_connection(self):
        if self.connection is None:
            self.connection = psycopg.connect(self.connection_string)
        return self.connection

    def close_connection(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def __del__(self):
        self.close_connection()


#timescale_connection_string = os.environ.get("TIMESCALE_CONNECTION_STRING")
#db = TimescaleConnection(timescale_connection_string).get_connection()


# load timeseries source schema
schema_path = os.sep.join(
    [os.path.dirname(os.path.abspath(__file__)), "timeseries.json"]
)
with open(schema_path) as f:
    schema = json.load(f)


def create_single_timescale_record(
    conn: psycopg.Connection, record: dict[str, Any]
) -> None:
    """Create a single timescale record
    @param record: the record to create
    """
    # TODO: validate(instance=record, schema=schema)
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO conditions (timestamp, measurement_subject, correlation_id, measurement_name, {identify_data_column(record['measurement_data_type'])}) VALUES (%s, %s, %s, %s, %s)",  # noqa: E501
            (
                record["timestamp"],
                record["measurement_subject"],
                record["correlation_id"],
                record["measurement_name"],
                parse_measurement_value(record["measurement_data_type"], record["measurement_value"]),
            ),
        )

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
