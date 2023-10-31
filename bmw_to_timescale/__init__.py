import sys
import os
from typing import List
import logging

import azure.functions as func


# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from shared_code import convert_bmw_to_timescale


def main(events: List[func.EventHubEvent], outputEventHubMessage: func.Out[str], outputEventHubMessage_monitor: func.Out[str]) -> None:
    logging.info('Processing events...')
    convert_bmw_to_timescale(events, outputEventHubMessage, outputEventHubMessage_monitor)

