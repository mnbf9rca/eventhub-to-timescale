import sys
import os
from typing import List
import logging

import azure.functions as func


# Add the project root directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from shared_code import convert_bmw_to_timescale


def main(event: func.EventHubEvent, outputEventHubMessage: func.Out[List[str]], outputEHMonitor: func.Out[List[str]]) -> None:
    logging.info('Processing events...')
    convert_bmw_to_timescale(event, outputEventHubMessage, outputEHMonitor)

