import json
from typing import Any, List

from azure.functions import EventHubEvent

from .timeseries import create_record_recursive
from .helpers import is_topic_of_interest, to_datetime, create_correlation_id


def emon_to_timescale(
    messagebody: dict, topic: str, publisher: str
) -> List[dict[str, Any]]:
    """Convert an emon message to a timescale record
    @param event: the eventhub event
    @param messagebody: the message body
    @param topic: the topic
    @param publisher: the publisher
    @return: a list of timescale records
    """
    # examine the topic. We're only interested in topics where the last part is in events_of_interest
    validate_message_body(messagebody)
    validate_this_is_an_emon_message(publisher)
    events_of_interest = ["emonTx4"]
    measurement_subject = is_topic_of_interest(topic, events_of_interest)
    if measurement_subject is None:
        return
    message_payload = json.loads(messagebody["payload"])
    # the timestamp is in the message payload
    timestamp = extract_timestamp(message_payload)
    correlation_id = create_correlation_id()
    # for these messages, we need to construct an array of records, one for each value
    records = []
    records = create_record_recursive(
        payload=message_payload,
        records=records,
        timestamp=timestamp,
        correlation_id=correlation_id,
        measurement_publisher=publisher,
        measurement_subject=measurement_subject,
        ignore_keys=["time"],
    )

    return records


def validate_message_body(messagebody):
    if not isinstance(messagebody, dict):
        raise ValueError(
            f"Invalid messagebody: emon processor only handles dict messages, not {type(messagebody)}"
        )
    if "payload" not in messagebody:
        raise ValueError(
            f"Invalid messagebody: emon: missing payload, not {messagebody}"
        )


def extract_timestamp(message_payload):
    if "time" not in message_payload:
        raise ValueError(
            f"Invalid messagebody: emon: missing time, not {message_payload}"
        )

    timestamp = to_datetime(message_payload["time"])
    return timestamp


def validate_this_is_an_emon_message(publisher):
    if publisher.lower() != "emon":
        raise ValueError(
            f"Invalid publisher: emon processor only handles emon messages, not {publisher}"
        )
