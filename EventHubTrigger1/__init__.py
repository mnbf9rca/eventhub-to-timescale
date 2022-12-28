import json
import logging
from enum import Enum
from typing import Any, List

import azure.functions as func


def main(events: List[func.EventHubEvent]):
    for event in events:
        parse_message(event)


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
        return_payload: List[str] = [json.dumps(p) for p in payload]
        return return_payload
    else:
        # logging.error("Payload is None")
        pass


class PayloadType(Enum):
    """Enum for the types of payload that can be sent to TimescaleDB"""

    NUMBER: str = "number"
    STRING: str = "string"
    BOOLEAN: str = "boolean"
    LATLONG: str = "latlong"


def create_atomic_record(
    source_timestamp: str,
    measurement_subject: str,
    measurement_of: str,
    measurement_value: Any,
    measurement_data_type: PayloadType,
    unique_id: str = None,
) -> dict[str, Any]:
    """Creates a record in the format expected by the TimescaleDB publisher
    Args:
        timestamp (str): timestamp in ISO format
        subject (str): subject of the record
        payload (Any): payload of the record
        payload_type (PayloadType): type of the payload
    Returns:
        dict: record in the format expected by TimescaleDB
    """
    # TODO create a class for this return type
    tsr = {
        "timestamp": source_timestamp,
        "measurement_subject": measurement_subject,
        "measurement_of": measurement_of,
        "measurement_value": measurement_value,
        "measurement_data_type": measurement_data_type.value,
        "unique_id": unique_id,
    }
    return tsr


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
    unique_id = f"{event.enqueued_time.isoformat()}-{event.sequence_number}"
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
            unique_id=unique_id,
        )
    ]


def create_record_recursive(
    payload: dict,
    records: List[dict[str, Any]],
    timestamp: str,
    unique_id: str,
    measurement_subject: str,
    ignore_keys: list = None,
    measurement_of_prefix: str = None,
) -> List[dict[str, Any]]:
    """recursively creates records in the format expected by the TimescaleDB publisher
    Args:
        payload (dict): payload of the record to be parsed
        records (Array[TimescaleRecord]): list of records to be returned
        timestamp (str): timestamp in ISO format
        unique_id (str): unique id for the record
        measurement_subject (str): subject of the record
        ignore_keys (list): list of keys to ignore (also will not be recursed)
        measurement_of_prefix (str): prefix to add to the measurement_of field
    Returns:
        dict: record in the format expected by TimescaleDB
    """
    for key in payload:
        if ignore_keys is None or key not in ignore_keys:
            if isinstance(payload[key], dict):
                create_record_recursive(
                    payload[key],
                    records,
                    timestamp,
                    unique_id,
                    measurement_subject,
                    ignore_keys,
                    measurement_of_prefix,
                )
            else:
                records.append(
                    create_atomic_record(
                        source_timestamp=timestamp,
                        measurement_subject=measurement_subject,
                        measurement_of=key
                        if measurement_of_prefix is None
                        else f"{measurement_of_prefix}_{key}",
                        measurement_value=payload[key],
                        measurement_data_type=get_record_type(payload[key]),
                        unique_id=unique_id,
                    )
                )
    return records


def get_record_type(payload):
    if isinstance(payload, str):
        return PayloadType.STRING
    elif isinstance(payload, int):
        return PayloadType.NUMBER
    elif isinstance(payload, float):
        return PayloadType.NUMBER
    elif isinstance(payload, bool):
        return PayloadType.BOOLEAN
    else:
        return None


def glow_to_timescale(
    event: func.EventHubEvent, messagebody: dict, topic: str, publisher: str
) -> List[dict[str, Any]]:
    # examine the topic. We're only interested in topics where the last part is in events_of_interest
    events_of_interest = ["electricitymeter", "gasmeter"]
    measurement_subject = topic.split("/")[-1]
    if measurement_subject not in events_of_interest:
        return

    # convert the message to a json object
    message_payload = json.loads(messagebody["payload"])
    timestamp = message_payload[measurement_subject]["timestamp"]
    unique_id = f"{event.enqueued_time.isoformat()}-{event.sequence_number}"
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
        unique_id,
        measurement_subject,
        ignore_keys=ignore_keys,
        measurement_of_prefix="import",
    )

    if measurement_subject == "electricitymeter":
        records = create_record_recursive(
            message_payload[measurement_subject]["power"],
            records,
            timestamp,
            unique_id,
            measurement_subject,
            ignore_keys=ignore_keys,
            measurement_of_prefix="power",
        )

    return records


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
    unique_id = f"{event.enqueued_time.isoformat()}-{event.sequence_number}"
    # for these messages, we need to construct an array of records, one for each value
    records = []
    records = create_record_recursive(
        message_payload,
        records,
        timestamp,
        unique_id,
        measurement_subject,
    )

    return records

    # convert the message to a json object


#  o_messagebody = json.loads(messagebody)
# get the payload
# payload = o_messagebody["payload"]
# get the timestamp
# timestamp = o_messagebody["timestamp"]


def extract_topic(messagebody):
    topic = messagebody["topic"]
    # the publisher is the first characters to the left of the first /
    publisher = topic.split("/")[0]
    return topic, publisher
