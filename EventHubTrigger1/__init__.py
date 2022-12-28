import json
import logging
from typing import Any, List

import azure.functions as func

from shared_code import (PayloadType, 
create_atomic_record,
                         create_record_recursive,
                         glow_to_timescale)


def main(events: List[func.EventHubEvent]):
    return_value = []
    for event in events:
        result = parse_message(event)
        if result is not None:
            return_value.append(result)
    return return_value


def parse_message(event: func.EventHubEvent):
    try:
        messagebody = event.get_body().decode("utf-8")
        o_messagebody: dict = json.loads(messagebody)
        topic, publisher = extract_topic(o_messagebody)
    except Exception as e:
        logging.error(f"Error parsing message: {e}")
        return
    payload: List[dict[str, Any]] = None

    if publisher == "glow":
        payload = glow_to_timescale(event, o_messagebody, topic, publisher)
    elif publisher == "homie":
        payload = homie_to_timescale(event, o_messagebody, topic, publisher)
    elif publisher == "emon":
        payload = emon_to_timescale(event, o_messagebody, topic, publisher)
    else:
        logging.error(f"Unknown publisher: {publisher}")
        return

    if payload is not None:
        logging.info(f"Payload: {payload}")
        return_payload = [json.dumps(p) for p in payload]
        return return_payload
    else:
        # logging.error("Payload is None")
        pass


def homie_to_timescale(
    event: func.EventHubEvent, messagebody: dict, topic: str, publisher: str
) -> List[dict[str, Any]]:
    # examine the topic. We're only interested in topics where the last part is one we're interested in
    events_of_interest = [
        "measure-temperature",
        "heating-setpoint",
        "state",
        "mode",
        "thermostat-setpoint",
    ]
    lastpart = topic.split("/")[-1]
    if lastpart not in events_of_interest:
        return
    correlation_id = f"{event.enqueued_time.isoformat()}-{event.sequence_number}"
    # convert the message to a json object
    return [
        create_atomic_record(
            source_timestamp=messagebody["timestamp"],
            measurement_subject=publisher,
            measurement_of=lastpart,
            measurement_value=messagebody["payload"],
            measurement_data_type=PayloadType.STRING
            if lastpart in ["state", "mode"]
            else PayloadType.NUMBER,
            correlation_id=correlation_id,
        )
    ]




def emon_to_timescale(
    event: func.EventHubEvent, messagebody: dict, topic: str, publisher: str
) -> str:
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


def extract_topic(messagebody):
    topic = messagebody["topic"]
    # the publisher is the first characters to the left of the first /
    publisher = topic.split("/")[0]
    return topic, publisher
