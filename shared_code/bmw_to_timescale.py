from typing import Any, List, Dict, Optional, Tuple
import json
import sys
import logging
import os
from azure.functions import EventHubEvent, Out

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import shared_code as sc

# from shared_code import (
#     create_atomic_record,
#     PayloadType,
#     check_duplicate,
#     get_table_service_client,
#     store_id,
# )


def convert_bmw_to_timescale(
    events: List[EventHubEvent], outputEventHubMessage: Out[str], outputEventHubMessage_monitor: Out[str]
) -> None:
    # things we're interested in in the message body
    # vin - this is mapped to the measurement_subject
    # state.lastFetched
    # state.lastUpdatedAt - if this hasnt changed, we dont need to record the data again
    # state.location.coordinates[latitude, longitude]
    # state.location.heading
    # state.currentmileage
    # state.electricChargingState[chargingLevelPercent, range, isChargerConnected, chargingStatus]
    tsc = sc.get_table_service_client()
    logging.info("Processing BMW messages")
    for event in events:
        logging.info(f"Processing event: {event}")
        event_object = get_event_body(event)
        vin = get_vin_from_message(event_object)
        last_updated_at = get_last_updated_at_from_message(event_object)
        if sc.check_duplicate(last_updated_at, vin, tsc):
            # we've already processed this message, so we can skip it
            logging.info(f"Skipping duplicate message: {event_object}")
            continue
        messages_to_send = construct_messages(vin, last_updated_at, event_object)
        for message in messages_to_send:
            str_message = json.dumps(message)
            logging.info(f"Sending message: {str_message}")
            try:
                outputEventHubMessage.set(str_message)
                outputEventHubMessage_monitor.set(str_message)
            except Exception as e:
                logging.error(f"Error sending message: {str_message} : {e}")
                raise
        sc.store_id(last_updated_at, vin, tsc)


def get_event_body(event: EventHubEvent) -> Dict[str, Any]:
    """
    Decode and parse the body of an EventHubEvent into a Python dictionary.

    Parameters:
    - event (EventHubEvent): The EventHubEvent object containing the raw event data.

    Returns:
    - Dict[str, Any]: A dictionary containing the parsed event data.

    Raises:
    - json.JSONDecodeError: If the event body is not valid JSON.
    - UnicodeDecodeError: If the event body cannot be decoded using UTF-8.
    """
    event_body = event.get_body().decode("utf-8")
    return json.loads(event_body)


def construct_messages(
    vin: str, last_updated_at: str, event_object: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Generate a list of atomic records for electric vehicle charging states and current mileage.

    Parameters:
    - vin (str): The Vehicle Identification Number.
    - last_updated_at (str): The timestamp indicating when the data was last updated.
    - event_object (Dict[str, Any]): The event object containing the charging state information.

    Returns:
    - List[str]: A list of atomic records, each represented as a string.

    Raises:
    - TypeError: If any of the types in the event_object do not match the expected types.
    - KeyError: If expected keys are missing in the event_object.
    """
    # construct an object containing all the fields we're interested in
    all_fields = {
        **get_electric_charging_state_from_message(event_object),
        **get_current_mileage_from_message(event_object),
        **get_coordinates_from_message(event_object),
    }

    # construct a list of fields to record, their payload types, and how to calculate the value
    fields_to_record = [
        (
            "chargingLevelPercent",
            sc.PayloadType.NUMBER,
            all_fields["chargingLevelPercent"],
        ),
        ("range", sc.PayloadType.NUMBER, all_fields["range"]),
        (
            "isChargerConnected",
            sc.PayloadType.BOOLEAN,
            bool(all_fields["isChargerConnected"]),
        ),
        (
            "chargingStatus",
            sc.PayloadType.STRING,
            all_fields["chargingStatus"],
        ),
        (
            "currentMileage",
            sc.PayloadType.NUMBER,
            all_fields["currentMileage"],
        ),
        (
            "coordinates",
            sc.PayloadType.GEOGRAPHY,
            tuple(
                validate_lat_long(
                    all_fields["coordinates"]["latitude"],
                    all_fields["coordinates"]["longitude"],
                )
            ),
        ),
    ]

    return create_records_from_fields(
        vin, last_updated_at, all_fields, fields_to_record
    )


def create_records_from_fields(
    vin: str,
    last_updated_at: str,
    all_fields: Dict[str, Any],
    fields_to_record: List[Tuple[str, sc.PayloadType, Any]],
) -> List[Dict[str, Any]]:
    """
    Create a list of atomic records based on specified fields and their types.

    Parameters:
    - vin (str): The Vehicle Identification Number (VIN) serving as the subject of the measurements.
    - last_updated_at (str): Timestamp indicating when the last update occurred.
    - all_fields (Dict[str, Any]): Dictionary containing all available fields and their respective values.
    - fields_to_record (List[Tuple[str, sc.PayloadType, Any]]): List of tuples specifying the fields to record.
      Each tuple contains:
        - field name (str): The name of the field.
        - payload type (sc.PayloadType): The type of the payload (e.g., NUMBER, STRING, BOOLEAN, etc.)
        - value (Any): The calculated value for the field.

    Returns:
    - List[Dict[str, Any]]: A list of dictionaries, each representing an atomic record.

    Each atomic record is generated using the `sc.create_atomic_record` function and includes details like the source
    timestamp, measurement subject (VIN), publisher, and other metadata along with the actual measurement value.

    Exceptions during the atomic record creation are logged but do not interrupt the overall process.

    Example:
    >>> create_records_from_fields("some_vin", "2023-01-01T12:34:56Z", {"speed": 70}, [("speed", PayloadType.NUMBER, 70)])
    [{'source_timestamp': '2023-01-01T12:34:56Z', 'measurement_subject': 'some_vin', ...}]
    """  # noqa: E501
    messages = []
    for field, payload_type, value_calculation in fields_to_record:
        if field in all_fields:
            try:
                # Safely get the value calculation, if the field exists in all_fields
                value = all_fields.get(field, None) if callable(value_calculation) else value_calculation
                atomic_record = sc.create_atomic_record(
                    source_timestamp=last_updated_at,
                    measurement_subject=vin,
                    measurement_publisher="bmw",
                    measurement_of=field,
                    measurement_data_type=payload_type,
                    correlation_id=last_updated_at,
                    measurement_value=value
                )
                messages.append(atomic_record)
            except Exception as e:
                print(f"Failed to create atomic record for field {field}: {e}")

    return messages


def get_vin_from_message(messagebody: dict[str, Any]) -> str:
    return messagebody["vin"]


def get_last_updated_at_from_message(messagebody: dict[str, Any]) -> str:
    return messagebody["state"]["lastUpdatedAt"]


def get_coordinates_from_message(messagebody: Dict[str, Any]) -> list[float] | None:
    """
    Extracts location information from a message body.

    Parameters:
    - messagebody (Dict[str, Any]): The message body containing location information.

    Returns:
    - Optional[Dict[str, Union[float, int]]]: A dictionary containing latitude and longitude if available, or None otherwise.
    """  # noqa: E501
    try:
        location = messagebody.get("state", None).get("location", None)
        # check if location actually contains an object called coordinates
        if location is not None and "coordinates" in location:
            return location
    except AttributeError:
        return None
    return None


def validate_lat_long(lat: float | int, lon: float | int) -> list[float, float]:
    """
    Validates the latitude and longitude values.

    Args:
        lat (float | int): Latitude value to validate. Should be between -90 and 90.
        lon (float | int): Longitude value to validate. Should be between -180 and 180.

    Returns:
        list[float, float]: A list containing the validated latitude and longitude as floats.

    Raises:
        TypeError: If the input types for latitude or longitude are not float or int.
        ValueError: If latitude is not in the range [-90, 90] or longitude is not in the range [-180, 180].

    Examples:
        >>> validate_lat_long(45.0, -122.0)
        [45.0, -122.0]

        >>> validate_lat_long(100, 200)
        ValueError: Invalid latitude value: 100
    """
    if lat is None or lon is None:
        raise TypeError(
            f"Invalid types for latitude and/or longitude: {type(lat)}, {type(lon)}"
        )

    if not (isinstance(lat, (float, int)) and isinstance(lon, (float, int))):
        raise TypeError(
            f"Invalid types for latitude and/or longitude: {type(lat)}, {type(lon)}"
        )

    if not (-90 <= lat <= 90):
        raise ValueError(f"Invalid latitude value: {lat}")

    if not (-180 <= lon <= 180):
        raise ValueError(f"Invalid longitude value: {lon}")

    return [float(lat), float(lon)]


def get_current_mileage_from_message(
    messagebody: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Extracts current mileage information from a message body.

    Parameters:
    - messagebody (Dict[str, Any]): The message body containing mileage information.

    Returns:
    - Optional[Dict[str, Any]]: A dictionary with the current mileage if available, or None otherwise.
    """
    try:
        state = messagebody.get("state", {})
        currentMileage = state.get("currentMileage")
    except AttributeError:
        return None

    if currentMileage is None:
        return None

    if not isinstance(currentMileage, int):
        raise TypeError(f"Invalid type for currentMileage: {type(currentMileage)}")

    return {"currentMileage": currentMileage}


def get_electric_charging_state_from_message(
    messagebody: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Extracts electric charging state information from a message body.

    Parameters:
    - messagebody (Dict[str, Any]): The message body containing electric charging state information.

    Returns:
    - Dict[str, Any]: A dictionary containing extracted electric charging state information.
    """
    electric_charging_state = {}
    try:
        charging_state = messagebody.get("state", {}).get("electricChargingState", {})
    except AttributeError:
        return electric_charging_state

    if charging_state is None:
        return electric_charging_state

    if "chargingLevelPercent" in charging_state:
        electric_charging_state["chargingLevelPercent"] = charging_state[
            "chargingLevelPercent"
        ]

    if "range" in charging_state:
        electric_charging_state["range"] = charging_state["range"]

    if "isChargerConnected" in charging_state:
        electric_charging_state["isChargerConnected"] = charging_state[
            "isChargerConnected"
        ]

    if "chargingStatus" in charging_state:
        electric_charging_state["chargingStatus"] = charging_state["chargingStatus"]

    return electric_charging_state
