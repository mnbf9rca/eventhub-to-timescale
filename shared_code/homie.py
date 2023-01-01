from typing import Any, List

from azure.functions import EventHubEvent

from .timeseries import create_atomic_record
from .timeseries import PayloadType
from .helpers import to_datetime, create_correlation_id


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
    if publisher != "homie":
        raise ValueError("Invalid publisher: Homie processor only handles Homie messages not %s" % publisher)
    
    # examine the topic. We're only interested in topics where the last part is one we're interested in
    events_of_interest = [
        "measure-temperature",
        "heating-setpoint",
        "state",
        "mode",
        "thermostat-setpoint",
    ]
    topic_parts = topic.split("/")
    measurement_of = topic_parts[-1]
    if measurement_of not in events_of_interest:
        return
    correlation_id = create_correlation_id(event)
    # convert the message to a json object
    return [
        create_atomic_record(
            source_timestamp=to_datetime(messagebody["timestamp"]),
            measurement_subject=topic_parts[-2],
            measurement_publisher=publisher,
            measurement_of=measurement_of,
            measurement_value=messagebody["payload"],
            measurement_data_type=PayloadType.STRING
            if measurement_of in ["state", "mode"]
            else PayloadType.NUMBER,
            correlation_id=correlation_id,
        )
    ]
