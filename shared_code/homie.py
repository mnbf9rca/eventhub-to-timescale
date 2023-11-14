from typing import Any, List


from .timeseries import create_atomic_record, PayloadType
from .helpers import (
    to_datetime_string,
    create_correlation_id,
    validate_message_body_type_and_keys,
    validate_publisher,
)


def homie_to_timescale(
    messagebody: dict, topic: str, publisher: str
) -> List[dict[str, Any]]:
    """Convert a homie message to a timescale record
    @param event: the eventhub event
    @param messagebody: the message body
    @param topic: the topic
    @param publisher: the publisher
    @return: a list of timescale records
    """
    this_service_name = "homie"
    events_of_interest = get_events_of_interest()
    validate_publisher(publisher, this_service_name)
    validate_message_body_type_and_keys(messagebody, this_service_name, ["timestamp"])

    measurement_of, measurement_subject = get_measurement_of_and_subject(topic)
    if measurement_of not in events_of_interest:
        return

    return [
        create_atomic_record(
            source_timestamp=to_datetime_string(messagebody["timestamp"]),
            measurement_subject=measurement_subject,
            measurement_publisher=publisher,
            measurement_of=measurement_of,
            measurement_value=messagebody["payload"],
            measurement_data_type=get_payload_type(measurement_of),
            correlation_id=create_correlation_id(),
        )
    ]


def get_events_of_interest():
    return [
        "measure-temperature",
        "heating-setpoint",
        "state",
        "mode",
        "thermostat-setpoint",
    ]


def get_measurement_of_and_subject(topic: str) -> (str, str):
    """Get the measurement_of and measurement_subject from the topic
    @param topic: the topic
    @return: the measurement_of and measurement_subject
    @throws: ValueError if the topic is not a string or is None
    """
    if not isinstance(topic, str) or topic is None:
        raise ValueError(f"Invalid topic: {topic}")
    topic_parts = topic.split("/")
    measurement_of = topic_parts[-1]
    measurement_subject = topic_parts[-2]
    return measurement_of, measurement_subject


def get_payload_type(measurement_of: str) -> PayloadType:
    """Get the payload type for the measurement_of
    @param measurement_of: the measurement_of
    @return: the payload type
    """
    if measurement_of in ["state", "mode"]:
        return PayloadType.STRING
    else:
        return PayloadType.NUMBER
