import json
import os
import sys
from typing import List
from azure.functions import EventHubEvent
import datetime
from dateutil import parser

# add the shared_code directory to the path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shared_code import recursively_deserialize  # noqa: E402


def load_test_data():
    """
    Load test data from a JSON file
    @return: a dictionary of test data
    """
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    test_data_path = os.sep.join([SCRIPT_DIR, "test_data.json"])

    with open(test_data_path, "r") as f:
        raw_test_data = f.read()
    whole_object = recursively_deserialize(raw_test_data)
    # in functions, the payload is a string, but in tests it is a dict because it is loaded from JSON
    # within whole_object, for each item, replace [properties][body] with json.dumps([properties][body])
    for item in whole_object:
        if (
            isinstance(whole_object[item], (dict, List, tuple))
            and "properties" in whole_object[item]
            and "body" in whole_object[item]["properties"]
            and isinstance(whole_object[item]["properties"]["body"]["payload"], dict)
        ):
            whole_object[item]["properties"]["body"]["payload"] = json.dumps(
                whole_object[item]["properties"]["body"]["payload"]
            )
    return whole_object


def create_event_hub_event(event_properties: dict) -> EventHubEvent:
    """
    Create an EventHubEvent from a dictionary of properties
    event_properties = {
      body: string, # The body of the event
      partition_key: string, # The partition key of the event
      offset: string, # The offset of the event
      sequence_number: int, # The sequence number of the event
      enqueued_time: datetime, # The enqueued time of the event
      properties: dict, # The properties of the event
      system_properties: dict, # The system properties of the event
    }
    """

    body: str | None = event_properties.get("body")
    if body is not None:
        if isinstance(body, str):
            body = body.encode("UTF-8")
        else:
            body = json.dumps(body).encode("UTF-8")
    if "enqueued_time" in event_properties:
        # Convert the enqueued time to a datetime object
        enqueued_time = parser.parse(event_properties["enqueued_time"])
    else:
        enqueued_time = datetime.datetime.now()

    return EventHubEvent(
        # azure functions encodes the message body as utf-8
        body=body,
        trigger_metadata=event_properties.get("trigger_metadata", {}),
        enqueued_time=enqueued_time,
        partition_key=event_properties.get("partition_key"),
        sequence_number=event_properties[
            "sequence_number"
        ],  # sequence_number is required
        offset=event_properties["offset"],  # offset is required
        iothub_metadata=event_properties.get("iothub_metadata", {}),
    )


# allow execution of this file as a script

if __name__ == "__main__":  # pragma: no cover
    print(load_test_data())
