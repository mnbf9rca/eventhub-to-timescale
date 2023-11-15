import json
import logging
from typing import Any, List, Iterator
from shared_code.glow import glow_to_timescale
from shared_code.homie import homie_to_timescale
from shared_code.emon import emon_to_timescale


# from shared_code import glow_to_timescale, homie_to_timescale, emon_to_timescale
import azure.functions as func


def convert_json_to_timeseries(
    events: List[func.EventHubEvent | str] | func.EventHubEvent | str,
    outputEventHubMessage: func.Out[List[str]],
) -> None:
    array_of_events_as_strings = [get_event_as_str(event) for event in to_list(events)]
    messages: filter[list[dict[str, Any]]] = filter(
        None, (convert_event(event) for event in array_of_events_as_strings)
    )
    send_messages(messages, outputEventHubMessage)


def to_list(events):
    return events if isinstance(events, list) else [events]


def get_event_as_str(event):
    if not isinstance(event, str) and not isinstance(event, func.EventHubEvent):
        try:
            message_json = json.dumps(event)
        except json.JSONDecodeError:
            message_json = "<non-serializable object>"
        except Exception as e:
            message_json = "<unknown object>"
            logging.error(f"Error serializing event in get_event_as_str: {e}")

        error_message = (
            f"Event {message_json} is of type: {type(event)} not str or EventHubEvent"
        )
        logging.debug(error_message)
        raise TypeError(error_message)
    try:
        return event if isinstance(event, str) else event.get_body().decode("utf-8")
    except Exception as e:
        logging.error(f"Error getting event body: {e}")
        raise


def convert_event(event_str):
    try:
        o_messagebody = json.loads(event_str)
        topic, publisher = extract_topic(o_messagebody)
        payload = send_to_converter(publisher, o_messagebody, topic)
        logging.debug(f"Parsed payload: {payload}")
        return payload if payload else None
    except Exception as e:
        logging.error(f"Error in event conversion: {e}")
        return None


def send_messages(
    messages: Iterator[Any], outputEventHubMessage: func.Out[List[str]]
) -> None:
    for message in messages:
        try:
            payload = json.dumps(message)
            outputEventHubMessage.set(payload)
        except (json.JSONDecodeError, ValueError, TypeError):
            logging.error(f"json_converter: Error serializing message: {message}")
        except Exception as e:
            logging.error(f"json_converter: Error sending message: {e}")


# def convert_json_to_timeseries(
#     event: List[func.EventHubEvent] | List[str],
#     outputEventHubMessage: func.Out[List[str]],
# ) -> None:
#     return_value = []

#     events_to_process = event if isinstance(event, list) else [event]
#     for event in events_to_process:
#         result = parse_message(event)
#         if result is not None:
#             return_value.append(result)
#     if len(return_value) > 0:
#         for p in return_value:
#             outputEventHubMessage.set(json.dumps(p))


# def parse_message(event: str):
#     try:
#         if isinstance(event, str):
#             o_messagebody = json.loads(event)
#         elif isinstance(event, func.EventHubEvent):
#             o_messagebody = json.loads(event.get_body().decode("utf-8"))
#         topic, publisher = extract_topic(o_messagebody)
#     except Exception as e:
#         logging.error(f"Error parsing message: {e}")
#         raise

#     try:
#         payload: List[dict[str, Any]] = send_to_converter(
#             publisher, o_messagebody, topic
#         )
#     except Exception as e:
#         logging.error(f"Error converting message: {e}")
#         raise

#     logging.debug(f"Parsed payload: {payload}")
#     return payload if payload else None
# return [json.dumps(p) for p in payload] if payload else None


def send_to_converter(
    publisher: str, o_messagebody: Any, topic: str
) -> list[dict[str, Any]]:
    """Send the message to the appropriate converter
    @param publisher: the publisher of the message
    @param o_messagebody: the message body
    @param topic: the topic of the message
    @return: the converted message
    @raises ValueError: if the publisher is unknown
    """
    if publisher.lower() == "glow":
        return glow_to_timescale(o_messagebody, topic, publisher)
    elif publisher.lower() == "homie":
        return homie_to_timescale(o_messagebody, topic, publisher)
    elif publisher.lower() == "emon":
        return emon_to_timescale(o_messagebody, topic, publisher)
    else:
        logging.error(f"Unknown publisher: {publisher}")
        raise ValueError(f"Unknown publisher: {publisher}")


def extract_topic(messagebody: dict) -> tuple[str, str]:
    if topic := messagebody.get("topic"):
        publisher = topic.split("/")[0]
        return topic, publisher
    else:
        logging.error(f"Error extracting topic: {messagebody}")
        raise ValueError(f"Error extracting topic: {messagebody}")
