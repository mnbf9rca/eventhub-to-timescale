from typing import Any, List, Dict, Union, Optional, Tuple
import json
import sys
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
    events: List[EventHubEvent], outputEventHubMessage: Out[str]
) -> None:
    # things we're interested in in the message body
    # vin - this is mapped to the measurement_subject
    # state.lastFetched
    # state.lastUpdatedAt - if this hasnt changed, we dont need to record the data again
    # state.location.coordinates[lattitude, longitude]
    # state.location.heading
    # state.currentmileage
    # state.electricChargingState[chargingLevelPercent, range, isChargerConnected, chargingStatus]
    tsc = sc.get_table_service_client()
    for event in events:
        event_object = get_event_body(event)
        vin = get_vin_from_message(event_object)
        last_updated_at = get_last_updated_at_from_message(event_object)
        if sc.check_duplicate(last_updated_at, vin, tsc):
            # we've already processed this message, so we can skip it
            continue
        messages_to_send = construct_messages(vin, last_updated_at, event_object)
        for message in messages_to_send:
            outputEventHubMessage.set(message)
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
    event_object = json.loads(event_body)
    return event_object



def construct_location_message(
    vin: str, last_updated_at: str, event_object: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Generate an atomic record for vehicle location.

    This function extracts location-related metrics from the event_object,
    and then creates an atomic record using a standard schema.

    Parameters:
    - vin (str): The Vehicle Identification Number.
    - last_updated_at (str): The timestamp indicating when the data was last updated.
    - event_object (Dict[str, Any]): The event object containing the location information.

    Returns:
    - Optional[Dict[str, Any]]: An atomic record represented as a dictionary, or None if location is not available.

    Raises:
    - TypeError: If any of the types in the event_object do not match the expected types.
    - KeyError: If expected keys are missing in the event_object.
    """
    location = get_location_from_message(event_object)

    if location is None or "lat" not in location or "lon" not in location:
        return

    return sc.create_atomic_record(
        source_timestamp=last_updated_at,
        measurement_subject=vin,
        measurement_publisher="bmw",
        measurement_of="location",
        measurement_data_type=sc.PayloadType.GEOGRAPHY,
        correlation_id=last_updated_at,
        measurement_value=[location["lat"], location["lon"]],
    )


def get_value_and_type(
    field: str, all_fields: Dict[str, Any], current_mileage: Dict[str, Any]
) -> Optional[Tuple[Any, str]]:
    """
    Get the value and the payload type for a specific field.

    Returns:
    - Tuple: A tuple containing the value and payload type, or None if the field doesn't exist.
    """

    if field in all_fields:
        value = all_fields[field]
        if field == "is_charger_connected":
            value = bool(value)
        return value, payload_type_map[field]
    elif field in current_mileage:
        return current_mileage[field], payload_type_map[field]
    return None


def construct_messages(
    vin: str, last_updated_at: str, event_object: Dict[str, Any]
) -> List[str]:
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
    electric_charging_state = get_electric_charging_state_from_message(event_object)
    current_mileage = get_current_mileage_from_message(event_object)

    all_fields = {**electric_charging_state, **current_mileage}

    fields_to_record = {
        "charging_level_percent": sc.PayloadType.NUMBER,
        "range": sc.PayloadType.NUMBER,
        "is_charger_connected": sc.PayloadType.BOOLEAN,
        "charging_status": sc.PayloadType.STRING,
        "current_mileage": sc.PayloadType.NUMBER,
    }

    messages = [construct_location_message(vin, last_updated_at, event_object)]

    for field, payload_type in fields_to_record.items():
        if field in all_fields:
            value = all_fields[field]
            message = generate_atomic_record(
                vin, last_updated_at, field, payload_type, value
            )
            messages.append(message)

    return messages


def generate_atomic_record(
    vin: str, last_updated_at: str, field: str, payload_type: sc.PayloadType, value: Any
) -> str:
    """
    Generate an atomic record for a single field.

    Parameters:
    - vin (str): The Vehicle Identification Number.
    - last_updated_at (str): The timestamp indicating when the data was last updated.
    - field (str): The field name for which the atomic record is being created.
    - payload_type (str): The payload type for the field.
    - value (Any): The value for the field.

    Returns:
    - str: An atomic record as a string.
    """
    value_to_record = bool(value) if payload_type == sc.PayloadType.BOOLEAN else value

    return sc.create_atomic_record(
        source_timestamp=last_updated_at,
        measurement_subject=vin,
        measurement_publisher="bmw",
        measurement_of=field,
        measurement_data_type=payload_type,
        correlation_id=last_updated_at,
        measurement_value=value_to_record,
    )


def get_vin_from_message(messagebody: dict[str, Any]) -> str:
    return messagebody["vin"]


def get_last_updated_at_from_message(messagebody: dict[str, Any]) -> str:
    return messagebody["state"]["lastUpdatedAt"]


def get_location_from_message(
    messagebody: Dict[str, Any]
) -> Optional[Dict[str, Union[float, int]]]:
    """
    Extracts location information from a message body.

    Parameters:
    - messagebody (Dict[str, Any]): The message body containing location information.

    Returns:
    - Optional[Dict[str, Union[float, int]]]: A dictionary containing latitude and longitude if available, or None otherwise.
    """  # noqa: E501
    try:
        coordinates = (
            messagebody.get("state", {}).get("location", {}).get("coordinates", {})
        )
    except AttributeError:
        return None

    lat = coordinates.get("latitude")
    lon = coordinates.get("longitude")

    if lat is None or lon is None:
        return None

    if not (isinstance(lat, (float, int)) and isinstance(lon, (float, int))):
        raise TypeError(
            f"Invalid types for latitude and/or longitude: {type(lat)}, {type(lon)}"
        )

    if not (-90 <= lat <= 90):
        raise ValueError(f"Invalid latitude value: {lat}")

    if not (-180 <= lon <= 180):
        raise ValueError(f"Invalid longitude value: {lon}")

    return {"lat": lat, "lon": lon}


def get_current_mileage_from_message(messagebody: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extracts current mileage information from a message body.

    Parameters:
    - messagebody (Dict[str, Any]): The message body containing mileage information.

    Returns:
    - Optional[Dict[str, Any]]: A dictionary with the current mileage if available, or None otherwise.
    """
    try:
        state = messagebody.get("state", {})
        current_mileage = state.get("currentMileage")
    except AttributeError:
        return None

    if current_mileage is None:
        return None

    if not isinstance(current_mileage, int):
        raise TypeError(f"Invalid type for currentMileage: {type(current_mileage)}")

    return {"current_mileage": current_mileage}


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
        electric_charging_state["charging_level_percent"] = charging_state[
            "chargingLevelPercent"
        ]

    if "range" in charging_state:
        electric_charging_state["range"] = charging_state["range"]

    if "isChargerConnected" in charging_state:
        electric_charging_state["is_charger_connected"] = charging_state[
            "isChargerConnected"
        ]

    if "chargingStatus" in charging_state:
        electric_charging_state["charging_status"] = charging_state["chargingStatus"]

    return electric_charging_state
