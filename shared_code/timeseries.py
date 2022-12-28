from enum import Enum
from typing import Any, List


class PayloadType(Enum):
    """Enum for the types of payload that can be sent to TimescaleDB"""

    NUMBER: str = "number"
    STRING: str = "string"
    BOOLEAN: str = "boolean"
    LATLONG: str = "latlong"


def create_atomic_record(
    source_timestamp: str,
    measurement_subject: str,
    measurement_of: str,
    measurement_value: Any,
    measurement_data_type: PayloadType,
    correlation_id: str = None,
) -> dict[str, Any]:
    """Creates a record in the format expected by the TimescaleDB publisher
    Args:
        timestamp (str): timestamp in ISO format
        subject (str): subject of the record
        payload (Any): payload of the record
        payload_type (PayloadType): type of the payload
    Returns:
        dict: record in the format expected by TimescaleDB
    """
    # TODO create a class for this return type
    tsr = {
        "timestamp": source_timestamp,
        "measurement_subject": measurement_subject,
        "measurement_of": measurement_of,
        "measurement_value": measurement_value,
        "measurement_data_type": measurement_data_type.value,
        "correlation_id": correlation_id,
    }
    return tsr


def create_record_recursive(
    payload: dict,
    records: List[dict[str, Any]],
    timestamp: str,
    correlation_id: str,
    measurement_subject: str,
    ignore_keys: list = None,
    measurement_of_prefix: str = None,
) -> List[dict[str, Any]]:
    """recursively creates records in the format expected by the TimescaleDB publisher
    Args:
        payload (dict): payload of the record to be parsed
        records (Array[TimescaleRecord]): list of records to be returned
        timestamp (str): timestamp in ISO format
        correlation_id (str): unique id for the record
        measurement_subject (str): subject of the record
        ignore_keys (list): list of keys to ignore (also will not be recursed)
        measurement_of_prefix (str): prefix to add to the measurement_of field
    Returns:
        dict: record in the format expected by TimescaleDB
    """
    for key in payload:
        if ignore_keys is None or key not in ignore_keys:
            if isinstance(payload[key], dict):
                create_record_recursive(
                    payload[key],
                    records,
                    timestamp,
                    correlation_id,
                    measurement_subject,
                    ignore_keys,
                    measurement_of_prefix,
                )
            else:
                records.append(
                    create_atomic_record(
                        source_timestamp=timestamp,
                        measurement_subject=measurement_subject,
                        measurement_of=key
                        if measurement_of_prefix is None
                        else f"{measurement_of_prefix}_{key}",
                        measurement_value=payload[key],
                        measurement_data_type=get_record_type(payload[key]),
                        correlation_id=correlation_id,
                    )
                )
    return records


def get_record_type(payload):
    if isinstance(payload, str):
        return PayloadType.STRING
    elif isinstance(payload, int):
        return PayloadType.NUMBER
    elif isinstance(payload, float):
        return PayloadType.NUMBER
    elif isinstance(payload, bool):
        return PayloadType.BOOLEAN
    else:
        return None
