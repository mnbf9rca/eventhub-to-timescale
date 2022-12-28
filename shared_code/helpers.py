import json
from typing import List


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


def recursive_json_parser(data) -> dict:
    """
    recursively parse JSON object
    @param data: a string representing the JSON object
    @return: the parsed JSON object
    """

    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            pass
    elif isinstance(data, dict):
        for key, value in data.items():
            data[key] = recursive_json_parser(value)
    return data
