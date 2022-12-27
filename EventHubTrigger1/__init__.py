import json
import logging
from enum import Enum
from typing import MutableSequence

import azure.functions as func


def main(events: MutableSequence[func.EventHubEvent]):
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

    logging.info(f"Publisher: {publisher}")
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
        # TODO: send the payload to another eventhub called timescale
    else:
        logging.error("Payload is None")


class PayloadType(Enum):
    """Enum for the types of payload that can be sent to TimescaleDB"""

    NUMBER: str = "number"
    STRING: str = "string"
    BOOLEAN: str = "boolean"
    LATLONG: str = "latlong"


class TimescaleRecord:
    def __init__(
        self,
        timestamp: str,
        subject: str,
        payload: any,
        payload_type: PayloadType,
        unique_id: str = None,
    ):
        """Creates a record in the format expected by the TimescaleDB publisher
        Args:
            timestamp (str): timestamp in ISO format
            subject (str): subject of the record
            payload (any): payload of the record
            payload_type (PayloadType): type of the payload
        """
        self.timestamp = timestamp
        self.subject = subject
        self.payload = payload
        self.payload_type = self._validate_payload_type(payload_type)
        self.unique_id = unique_id

    def __dict__(self):
        return {
            "timestamp": self.timestamp,
            "subject": self.subject,
            "payload": self.payload,
            "payload_type": self.payload_type.value,
            "unique_id": self.unique_id,
        }

    def __str__(self):
        return json.dumps(self.__dict__())

    def __repr__(self):
        return self.__str__()
    
    def toString(self):
        return self.__str__()
    
    def repr(self):
        return self.__repr__()

    def _validate_payload_type(self, payload: PayloadType) -> PayloadType:
        if payload not in PayloadType:
            raise ValueError(f"Invalid payload type: {payload}")
        else:
            return payload


def create_atomic_record(
    source_timestamp: str,
    subject: str,
    payload: any,
    payload_type: PayloadType,
    unique_id: str = None,
) -> TimescaleRecord:
    """Creates a record in the format expected by the TimescaleDB publisher
    Args:
        timestamp (str): timestamp in ISO format
        subject (str): subject of the record
        payload (any): payload of the record
        payload_type (PayloadType): type of the payload
    Returns:
        dict: record in the format expected by TimescaleDB
    """
    # TODO create a class for this return type
    tsr = TimescaleRecord(
        source_timestamp, subject, payload, payload_type, unique_id
    )
    return tsr


def homie_to_timescale(
    event: func.EventHubEvent, messagebody: dict, topic: str, publisher: str
) -> str:
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
    return create_atomic_record(
        source_timestamp=messagebody["timestamp"],
        subject=publisher,
        payload=messagebody["payload"],
        payload_type=PayloadType.STRING if lastpart == "measure-temperature" else PayloadType.STRING,
        unique_id=unique_id,
    )


def glow_to_timescale(
    event: func.EventHubEvent, messagebody: str, topic: str, publisher: str
) -> str:
    # examine the topic. We're only interested in topics where the last part is in events_of_interest
    events_of_interest = ["electricitymeter", "gasmeter"]
    lastpart = topic.split("/")[-1]
    if lastpart not in events_of_interest:
        return

    # convert the message to a json object
    o_messagebody = json.loads(messagebody)
    # get the payload
    payload = o_messagebody["payload"]
    # get the timestamp
    timestamp = o_messagebody["timestamp"]


def emon_to_timescale(
    event: func.EventHubEvent, messagebody: str, topic: str, publisher: str
) -> str:
    # examine the topic. We're only interested in topics where the last part is in events_of_interest
    events_of_interest = ["electricitymeter", "gasmeter"]
    lastpart = topic.split("/")[-1]
    if lastpart not in events_of_interest:
        return

    # convert the message to a json object
    o_messagebody = json.loads(messagebody)
    # get the payload
    payload = o_messagebody["payload"]
    # get the timestamp
    timestamp = o_messagebody["timestamp"]


def extract_topic(messagebody):
    topic = messagebody["topic"]
    # the publisher is the first characters to the left of the first /
    publisher = topic.split("/")[0]
    return topic, publisher
