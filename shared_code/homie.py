from typing import Any, List

from azure.functions import EventHubEvent

from .timeseries import create_atomic_record
from .timeseries import PayloadType
from .helpers import to_datetime


def homie_to_timescale(
    event: EventHubEvent, messagebody: dict, topic: str, publisher: str
) -> List[dict[str, Any]]:
    """Convert a homie message to a timescale record
    @param event: the eventhub event
    @param messagebody: the message body
    @param topic: the topic
    @param publisher: the publisher
    @return: a list of timescale records
    """
    # examine the topic. We're only interested in topics where the last part is one we're interested in
    events_of_interest = [
        "measure-temperature",
        "heating-setpoint",
        "state",
        "mode",
        "thermostat-setpoint",
    ]
    measurement_of = topic.split("/")[-1]
    if measurement_of not in events_of_interest:
        return
    correlation_id = f"{event.enqueued_time.isoformat()}-{event.sequence_number}"
    # convert the message to a json object
    return [
        create_atomic_record(
            source_timestamp=to_datetime(messagebody["timestamp"]),
            measurement_subject=publisher,
            measurement_of=measurement_of,
            measurement_value=messagebody["payload"],
            measurement_data_type=PayloadType.STRING
            if measurement_of in ["state", "mode"]
            else PayloadType.NUMBER,
            correlation_id=correlation_id,
        )
    ]
