import json
from azure.functions import EventHubEvent
import datetime


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

def load_test_data():
    """
    Load test data from a JSON file
    @return: a dictionary of test data
    """
    with open("./test/test_data.json", "r") as f:
        test_data = json.load(f)
    return recursive_json_parser(test_data)


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
    trigger_metadata: str | None = event_properties.get("trigger_metadata")
    if "enqueued_time" in event_properties:
        # Convert the enqueued time to a datetime object
        enqueued_time = datetime.datetime.strptime(
          event_properties["enqueued_time"],
          "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        enqueued_time = datetime.datetime.now()
    partition_key: str | None = event_properties.get("partition_key")
    sequence_number: int | None = event_properties.get("sequence_number")
    offset: int | None = event_properties.get("offset")
    iothub_metadata = event_properties.get("iothub_metadata")

    return EventHubEvent(
        # azure functions encodes the message body as utf-8
        body=json.dumps(body).encode("utf-8"),
        trigger_metadata=trigger_metadata,
        enqueued_time=enqueued_time,
        partition_key=partition_key,
        sequence_number=sequence_number,
        offset=offset,
        iothub_metadata=iothub_metadata,
    )
