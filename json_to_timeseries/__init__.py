import logging
from typing import List

import azure.functions as func

from shared_code.json_converter import convert_json_to_timeseries


def main(
    events: List[func.EventHubEvent], outputEventHubMessage: func.Out[List[str]]
) -> None:
    logging.info("Processing events...")
    convert_json_to_timeseries(events, outputEventHubMessage)
