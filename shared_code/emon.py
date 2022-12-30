import json
from typing import Any, List

from azure.functions import EventHubEvent

from .timeseries import create_record_recursive
from .helpers import is_topic_of_interest, to_datetime, create_correlation_id


def emon_to_timescale(
    event: EventHubEvent, messagebody: dict, topic: str, _publisher: str
) -> List[dict[str, Any]]:
    """Convert an emon message to a timescale record
    @param event: the eventhub event
    @param messagebody: the message body
    @param topic: the topic
    @param publisher: the publisher
    @return: a list of timescale records
    """
    # examine the topic. We're only interested in topics where the last part is in events_of_interest
    events_of_interest = ["emonTx4"]
    measurement_subject = is_topic_of_interest(topic, events_of_interest)
    if measurement_subject is None:
        return
    message_payload = json.loads(messagebody["payload"])
    # the timestamp is in the message payload
    timestamp = to_datetime(message_payload["time"])
    correlation_id = create_correlation_id(event)
    # for these messages, we need to construct an array of records, one for each value
    records = []
    records = create_record_recursive(
        message_payload,
        records,
        timestamp,
        correlation_id,
        measurement_subject,
    )

    return records
