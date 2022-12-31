import json
import logging
from typing import Any, List

import azure.functions as func

from shared_code import glow_to_timescale, homie_to_timescale, emon_to_timescale


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
        raise

    try:
        payload: List[dict[str, Any]] = send_to_converter(
            publisher, event, o_messagebody, topic
        )
    except Exception as e:
        logging.error(f"Error converting message: {e}")
        raise

    logging.debug(f"Parsed payload: {payload}")
    return [json.dumps(p) for p in payload] if payload else None


def send_to_converter(
    publisher: str, event: func.EventHubEvent, o_messagebody: Any, topic: str
) -> list[dict[str, Any]]:
    if publisher == "glow":
        return glow_to_timescale(event, o_messagebody, topic, publisher)
    elif publisher == "homie":
        return homie_to_timescale(event, o_messagebody, topic, publisher)
    elif publisher == "emon":
        return emon_to_timescale(event, o_messagebody, topic, publisher)
    else:
        logging.error(f"Unknown publisher: {publisher}")
        raise ValueError(f"Unknown publisher: {publisher}")


def extract_topic(messagebody: dict) -> tuple[str, str]:
    try:
        if topic := messagebody.get("topic"):
            publisher = topic.split("/")[0]
            return topic, publisher
        else:
            logging.error(f"Error extracting topic: {messagebody}")
            raise ValueError(f"Error extracting topic: {messagebody}")
    except Exception as e:
        logging.error(f"Error extracting topic: {e}")
        raise
    