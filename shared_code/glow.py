import json
from typing import Any, List, Optional

from .timeseries import create_record_recursive
from .helpers import (
    to_datetime_string,
    create_correlation_id,
    validate_message_body_type_and_keys,
    validate_publisher,
    is_topic_of_interest,
)


def parse_message_payload(messagebody: dict, measurement_subject: str) -> tuple:
    message_payload = json.loads(messagebody["payload"])
    timestamp = to_datetime_string(message_payload[measurement_subject]["timestamp"])
    return message_payload, timestamp


def process_measurement_subject(
    message_payload: dict,
    timestamp: str,
    correlation_id: str,
    publisher: str,
    measurement_subject: str,
) -> List[dict]:
    ignore_keys: list[str] = get_ignore_keys()
    records = []
    if measurement_subject not in message_payload:
        return []

    energy_payload: dict = message_payload[measurement_subject]["energy"]["import"]
    records: List[dict[str, Any]] = create_record_recursive(
        payload=energy_payload,
        records=records,
        timestamp=timestamp,
        correlation_id=correlation_id,
        measurement_publisher=publisher,
        measurement_subject=measurement_subject,
        ignore_keys=ignore_keys,
        measurement_of_prefix="import",
    )

    if measurement_subject == "electricitymeter":
        power_payload: dict = message_payload[measurement_subject]["power"]
        records: List[dict[str, Any]] = create_record_recursive(
            payload=power_payload,
            records=records,
            timestamp=timestamp,
            correlation_id=correlation_id,
            measurement_publisher=publisher,
            measurement_subject=measurement_subject,
            ignore_keys=ignore_keys,
            measurement_of_prefix="power",
        )
    return records


def get_ignore_keys():
    return [
        "units",
        "mpan",
        "mprn",
        "supplier",
        "dayweekmonthvolunits",
        "cumulativevolunits",
    ]


def glow_to_timescale(
    messagebody: dict,
    topic: str,
    publisher: str,
) -> List[dict[str, Any]]:
    validate_publisher(publisher, "glow")
    validate_message_body_type_and_keys(messagebody, "glow")

    measurement_subject = is_topic_of_interest(topic, ["electricitymeter", "gasmeter"])
    if measurement_subject is None:
        return

    message_payload, timestamp = parse_message_payload(messagebody, measurement_subject)
    correlation_id = create_correlation_id()

    records = []
    records = process_measurement_subject(
        message_payload,
        timestamp,
        correlation_id,
        publisher,
        measurement_subject,
        records,
    )

    return records
