from typing import List

import azure.functions as func

from shared_code import convert_bmw_to_timescale


def main(events: List[func.EventHubEvent], outputEventHubMessage: func.Out[str]) -> None:
    convert_bmw_to_timescale(events, outputEventHubMessage)

