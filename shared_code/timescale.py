import os
from typing import Any, Union, List
import logging
from dotenv_vault import load_dotenv

import psycopg as psycopg


from jsonschema import validate, ValidationError
import json

load_dotenv()


def get_connection_string() -> str:
    """Get the connection string for the timescale database
    @return: the connection string
    """
    # if we have a connection string in the environment, use that
    # otherwise, look for the individual components
    # which are POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, TABLE_NAME
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
        raise ValueError(f"Missing required environment variables: {missing_env_vars}")
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
    unwrapped_records = json.loads(record_set)
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

    # if measurement_type is None:
    #     raise ValueError("Measurement type cannot be None")

    if not isinstance(measurement_type, str):
        raise ValueError("Measurement type must be a string")

    if measurement_type.lower() == "boolean":
        return "measurement_bool"
    elif measurement_type.lower() == "number":
        return "measurement_number"
    elif measurement_type.lower() == "string":
        return "measurement_string"
    elif measurement_type.lower() == "geography":
        return "measurement_location"
    else:
        raise ValueError(f"Unknown measurement type: {measurement_type}")


def parse_measurement_value(
    measurement_type: str, measurement_value: str
) -> Union[bool, float, str]:
    """
    Parse a measurement value based on its type.

    Parameters:
        measurement_type (str): The type of the measurement. Expected values are "boolean", "number", or "string".
        measurement_value (str): The measurement value to be parsed.

    Returns:
        depends on: The parsed measurement value. The type of the returned value depends on `measurement_type`:
            - "boolean": returns a Python boolean (True or False)
            - "number": returns a float
            - "string": returns a string

    Raises:
        ValueError: If `measurement_type` is not one of the expected types ("boolean", "number", "string").
        ValueError: If `measurement_type` is "boolean" but `measurement_value` is not "true" or "false" (case-insensitive).

    Examples:
        >>> parse_measurement_value("boolean", "true")
        True
        >>> parse_measurement_value("number", "42.0")
        42.0
        >>> parse_measurement_value("string", "hello")
        'hello'
    """  # noqa: E501
    if measurement_type == "boolean":
        if measurement_value.lower() in {"true", "false"}:
            return measurement_value.lower() == "true"
        else:
            raise ValueError(f"Invalid boolean value: {measurement_value}")
    elif measurement_type == "number":
        try:
            return float(measurement_value)
        except ValueError:
            raise ValueError(f"Invalid number value: {measurement_value}")
    elif measurement_type == "string":
        return measurement_value
    elif measurement_type == "geography":
        return parse_to_geopoint(measurement_value)
    else:
        raise ValueError(f"Unknown measurement type: {measurement_type}")


def parse_to_geopoint(measurement_value: Union[str, List[Union[str, float]]]):
    """
    Parse a geographical point (latitude, longitude) from a given measurement_value.

    The function accepts either a string in the format "latitude,longitude"
    or a list containing two elements [latitude, longitude].
    Latitude and longitude can be either string or float.

    Parameters:
    - measurement_value (Union[str, List[Union[str, float]]]): The geographical point
      to be parsed, either as a string "latitude,longitude" or as a list [latitude, longitude].

    Returns:
    - str: A string in Well-Known Text (WKT) format representing the geographical point.

    Raises:
    - ValueError: If the input is of an invalid type or format, or if latitude or longitude
      values are out of valid ranges.

    Examples:
    >>> parse_to_geopoint("40.7128,-74.0062")
    "SRID=4326;POINT(-74.0062 40.7128)"

    >>> parse_to_geopoint([40.7128, -74.0062])
    "SRID=4326;POINT(-74.0062 40.7128)"
    """    
    # Handle string input and split it
    if isinstance(measurement_value, str):
        latlon_values = measurement_value.split(',')
    # Handle list input
    elif isinstance(measurement_value, list) and len(measurement_value) == 2:
        latlon_values = measurement_value
    else:
        raise ValueError(f"Invalid input type or format: {measurement_value}")

    # Convert to float and validate
    try:
        latitude, longitude = map(float, latlon_values)
    except ValueError:
        raise ValueError(f"Invalid geography value: {measurement_value}")

    if not (-90 <= latitude <= 90):
        raise ValueError(f"Invalid latitude value: {latitude}")
    if not (-180 <= longitude <= 180):
        raise ValueError(f"Invalid longitude value: {longitude}")

    # POINT is well known type for geography
    # SRID=4326 is the spatial reference system for WGS84
    # https://postgis.net/docs/using_postgis_dbmanagement.html#PostGIS_GeographyVSGeometry
    # https://postgis.net/docs/using_postgis_dbmanagement.html#EWKB_EWKT
    # why long, lat? because it's x,y, and longitude is clearly x https://www.drupal.org/project/geo/issues/511370
    return f"SRID=4326;POINT({longitude} {latitude})"
