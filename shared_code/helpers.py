"""
common functions used by the azure functions
"""
import json
from typing import Any, List
from datetime import datetime
from dateutil import parser
from azure.functions import EventHubEvent
from uuid import uuid4


def is_topic_of_interest(topic: str, events_of_interest: List[str]):
    """Check if the topic is of interest by examining the last part of the topic
    @param topic: the topic
    @param events_of_interest: the list of events of interest
    @return: if the topic is of interest, return the last part of the topic, otherwise return None
    """
    measurement_subject = topic.split("/")[-1]
    if measurement_subject in events_of_interest:
        return measurement_subject
    else:
        return None


def to_datetime(timestamp) -> str:
    # Check if the input is a number (int or float)
    if isinstance(timestamp, (int, float)):
        if not (0 <= timestamp <= 253402300799):
            raise ValueError(f"Timestamp out of range: {timestamp}")
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Check if the input is a string
    elif isinstance(timestamp, str):
        try:
            parsed_time = parser.parse(timestamp)
            return parsed_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        except parser.ParserError as pe:
            raise ValueError(f"Invalid string timestamp format: {timestamp}") from pe

    # Raise an error for unsupported types
    else:
        raise TypeError(f"Unsupported type for timestamp: {type(timestamp).__name__}")


def create_correlation_id() -> str:
    """Create a correlation id. Note this used to be based on the event but now just returns a v4 uuid

    @return: the correlation id
    """
    return str(uuid4())
    # if event is None:
    #     raise ValueError("event cannot be None")
    # if event.sequence_number is None:
    #     raise ValueError("event.sequence_number cannot be None")
    # enqueued_time_str = event.enqueued_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
    # return f"{enqueued_time_str}-{event.sequence_number}"


def recursively_deserialize(item: Any) -> dict:
    """Recursively deserialize a string
    @param string: the string
    @return: the deserialized string
    """
    if isinstance(item, dict):
        return {key: recursively_deserialize(value) for key, value in item.items()}
    elif isinstance(item, (list, tuple)):
        return [recursively_deserialize(value) for value in item]
    if not isinstance(item, str):
        return item
    try:
        deserialized_item = json.loads(item)
        # if it's an iterative type, then recursively deserialize it
        # otherwise return the original item
        # this list comes from https://docs.python.org/3/library/json.html#json.JSONDecoder
        return (
            recursively_deserialize(deserialized_item)
            if isinstance(deserialized_item, (dict, list, tuple))
            else item
        )
    except json.JSONDecodeError:
        return item
