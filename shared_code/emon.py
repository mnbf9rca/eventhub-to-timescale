import json
from typing import Any, List

from azure.functions import EventHubEvent

from .timeseries import create_record_recursive
from .helpers import (
    is_topic_of_interest,
    to_datetime_string,
    create_correlation_id,
    validate_message_body_type_and_keys,
    validate_publisher,
)


def emon_to_timescale(
    messagebody: dict,
    topic: str,
    publisher: str,
) -> list[dict[str, Any]]:
    """Convert an emon message to a timescale record
    @param event: the eventhub event
    @param messagebody: the message body
    @param topic: the topic
    @param publisher: the publisher
    @return: a list of timescale records
    """
    # examine the topic. We're only interested in topics where the last part is in events_of_interest
    this_service_name = "emon"
    validate_message_body_type_and_keys(messagebody, this_service_name)
    validate_publisher(publisher, this_service_name)
    events_of_interest: list[str] = ["emonTx4"]
    measurement_subject = is_topic_of_interest(topic, events_of_interest)
    if measurement_subject is None:
        return
    message_payload = json.loads(messagebody["payload"])
    # the timestamp is in the message payload
    timestamp = extract_timestamp(message_payload)

    # for these messages, we need to construct an array of records, one for each value
    records = []
    return create_record_recursive(
        payload=message_payload,
        records=records,
        timestamp=timestamp,
        correlation_id=create_correlation_id(),
        measurement_publisher=publisher,
        measurement_subject=measurement_subject,
        ignore_keys=["time"],
    )


def extract_timestamp(message_payload: dict) -> str:
    """Extract the timestamp from the message payload
    @param message_payload: the message payload
    @return: the timestamp
    @throws: ValueError if the message payload does not have a timestamp
    """
    if not isinstance(message_payload, dict):
        raise ValueError(
            f"Invalid message_payload: emon processor only handles dict message_payload, not {type(message_payload)}"
        )
    if "time" not in message_payload:
        raise ValueError(
            f"Invalid message_payload: emon: missing time {message_payload}"
        )

    return to_datetime_string(message_payload["time"])
