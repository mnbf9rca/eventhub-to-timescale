import json
from typing import Any, List

from azure.functions import EventHubEvent

from .timeseries import create_record_recursive


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
    measurement_subject = topic.split("/")[-1]
    if measurement_subject not in events_of_interest:
        return
    # this timestemap is wrong - need to use the one in the messsage_payload
    timestamp = messagebody["timestamp"]
    message_payload = json.loads(messagebody["payload"])
    correlation_id = f"{event.enqueued_time.isoformat()}-{event.sequence_number}"
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
