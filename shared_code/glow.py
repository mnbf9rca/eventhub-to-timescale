import json
from typing import Any, List

from azure.functions import EventHubEvent

from .timeseries import create_record_recursive
from .helpers import to_datetime


def glow_to_timescale(
    event: EventHubEvent,
    messagebody: dict,
    topic: str,
    _publisher: str,
) -> List[dict[str, Any]]:
    """
    Convert a message from the Glow MQTT broker to a list of records for TimescaleDB
    @param event: the eventhub event
    @param messagebody: the message body
    @param topic: the topic
    @param publisher: the publisher
    @return: a list of timescale records
    """
    # examine the topic. We're only interested in topics where the last part is in events_of_interest
    events_of_interest = ["electricitymeter", "gasmeter"]
    measurement_subject = topic.split("/")[-1]
    if measurement_subject not in events_of_interest:
        return

    # convert the message to a json object
    message_payload = json.loads(messagebody["payload"])
    timestamp = to_datetime(message_payload[measurement_subject]["timestamp"])
    correlation_id = f"{event.enqueued_time.isoformat()}-{event.sequence_number}"
    # for these messages, we need to construct an array of records, one for each value
    records = []
    # ignore text fields which we dont care about:
    ignore_keys = [
        "units",
        "mpan",
        "mprn",
        "supplier",
        "dayweekmonthvolunits",
        "cumulativevolunits",
    ]
    records = create_record_recursive(
        message_payload[measurement_subject]["energy"]["import"],
        records,
        timestamp,
        correlation_id,
        measurement_subject,
        ignore_keys=ignore_keys,
        measurement_of_prefix="import",
    )

    if measurement_subject == "electricitymeter":
        records = create_record_recursive(
            message_payload[measurement_subject]["power"],
            records,
            timestamp,
            correlation_id,
            measurement_subject,
            ignore_keys=ignore_keys,
            measurement_of_prefix="power",
        )

    return records
