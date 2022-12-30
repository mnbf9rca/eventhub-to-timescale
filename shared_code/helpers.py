from typing import List
from datetime import datetime
from dateutil import parser
from azure.functions import EventHubEvent


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


def to_datetime(timestamp: str) -> str:
    """Convert a timestamp to a datetime
    @param timestamp: the timestamp
    @return: the datetime
    """
    # first, check that the timestamp is in the correct format
    # if it's an int or a float, then try to convert it to
    try:
        timestamp_float = float(timestamp)
        # we can only assume this is UTC - we havent seen enough from the emon messages to know
        # looking at the source, which i think is
        # https://github.com/emoncms/emoncms/blob/master/scripts/phpmqtt_input.php#L272
        # it looks like the timestamp is generated using php time() which is timezone agnostic
        # and for homie i'm not sure where the timestamp is coming from
        # check that it's in the max and min range for a timestamp
        if timestamp_float > 253402300799 or timestamp_float < 0:
            raise ValueError("timestamp is not in a recognisable format: %s", timestamp)
        return datetime.fromtimestamp(float(timestamp)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    except ValueError:
        pass
    except Exception as e:
        raise e
    # if it's a string, parse it and return the datetime
    try:
        return parser.parse(timestamp).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    except parser.ParserError:
        raise ValueError("timestamp is not in a recognisable format: %s", timestamp)
    except Exception as e:
        raise e


def create_correlation_id(event: EventHubEvent) -> str:
    """Create a correlation id from the event
    @param event: the event
    @return: the correlation id
    """
    if event is None:
        raise ValueError("event cannot be None")
    if event.sequence_number is None:
        raise ValueError("event.sequence_number cannot be None")
    enqueued_time_str = event.enqueued_time.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return f"{enqueued_time_str}-{event.sequence_number}"
