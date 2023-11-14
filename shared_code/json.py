import json
import logging
from typing import Any, List
from shared_code import glow_to_timescale, homie_to_timescale, emon_to_timescale
import azure.functions as func


def parse_message(event: str):
    try:
        if isinstance(event, str):
            o_messagebody = json.loads(event)
        elif isinstance(event, func.EventHubEvent):
            o_messagebody = json.loads(event.get_body().decode("utf-8"))
        # messagebody = event.get_body().decode("utf-8")
        # o_messagebody: dict = json.loads(messagebody)
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
    return payload if payload else None
    # return [json.dumps(p) for p in payload] if payload else None


def send_to_converter(
    publisher: str, event: func.EventHubEvent, o_messagebody: Any, topic: str
) -> list[dict[str, Any]]:
    if publisher == "glow":
        return glow_to_timescale(o_messagebody, topic, publisher)
    elif publisher == "homie":
        return homie_to_timescale(o_messagebody, topic, publisher)
    elif publisher == "emon":
        return emon_to_timescale(o_messagebody, topic, publisher)
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
