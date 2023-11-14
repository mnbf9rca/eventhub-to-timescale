import json
import logging
from typing import Any, List

import azure.functions as func

from shared_code.json import parse_message


def main(events: str, outputEventHubMessage: func.Out[str]) -> None:
    return_value = []
    events_to_process = events if isinstance(events, list) else [events]
    for event in events_to_process:
        result = parse_message(event)
        if result is not None:
            return_value.append(result)
    if len(return_value) > 0:
        for p in return_value:
            outputEventHubMessage.set(json.dumps(p))
