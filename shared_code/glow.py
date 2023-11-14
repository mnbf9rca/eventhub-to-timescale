import json
from typing import Any, List, Optional

from azure.functions import EventHubEvent

from .timeseries import create_record_recursive
from .helpers import to_datetime_string, create_correlation_id


def validate_publisher_and_topic(publisher: str, topic: str) -> Optional[str]:
    if publisher.lower() != "glow":
        raise ValueError(
            f"Invalid publisher: Glow processor only handles Glow messages, not {publisher}"
        )
    topic_parts = topic.split("/")
    measurement_subject = topic_parts[-1]
    if measurement_subject not in ["electricitymeter", "gasmeter"]:
        return None
    return measurement_subject


def parse_message_payload(messagebody: dict, measurement_subject: str) -> tuple:
    message_payload = json.loads(messagebody["payload"])
    timestamp = to_datetime_string(message_payload[measurement_subject]["timestamp"])
    return message_payload, timestamp


def create_records_for_subject(
    message_payload: dict,
    timestamp: str,
    correlation_id: str,
    publisher: str,
    measurement_subject: str,
    records: List[dict],
) -> List[dict]:
    ignore_keys = [
        "units",
        "mpan",
        "mprn",
        "supplier",
        "dayweekmonthvolunits",
        "cumulativevolunits",
    ]
    if measurement_subject not in message_payload:
        return []
    records = create_record_recursive(
        payload=message_payload[measurement_subject]["energy"]["import"],
        records=records,
        timestamp=timestamp,
        correlation_id=correlation_id,
        measurement_publisher=publisher,
        measurement_subject=measurement_subject,
        ignore_keys=ignore_keys,
        measurement_of_prefix="import",
    )
    if measurement_subject == "electricitymeter":
        records = create_record_recursive(
            payload=message_payload[measurement_subject]["power"],
            records=records,
            timestamp=timestamp,
            correlation_id=correlation_id,
            measurement_publisher=publisher,
            measurement_subject=measurement_subject,
            ignore_keys=ignore_keys,
            measurement_of_prefix="power",
        )
    return records


def glow_to_timescale(
    messagebody: dict,
    topic: str,
    publisher: str,
) -> List[dict[str, Any]]:
    measurement_subject = validate_publisher_and_topic(publisher, topic)
    if measurement_subject is None:
        return

    message_payload, timestamp = parse_message_payload(messagebody, measurement_subject)
    correlation_id = create_correlation_id()

    records = []
    records = create_records_for_subject(
        message_payload,
        timestamp,
        correlation_id,
        publisher,
        measurement_subject,
        records,
    )

    return records
