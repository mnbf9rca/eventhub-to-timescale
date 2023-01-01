import logging
import os
import json
from typing import List

import azure.functions as func
import psycopg

from shared_code import (create_timescale_records_from_batch_of_events)


def main(events: List[func.EventHubEvent]):
    conn = psycopg.connect(os.environ["TIMESCALE_CONNECTION_STRING"])
    errors: List[Exception] = []
    with conn:  # will close the connection when done
        for event in events:
            try:
                record_batch = event.get_body().decode('utf-8')
                raised_errors = create_timescale_records_from_batch_of_events(conn, record_batch)
                if raised_errors:
                    errors.extend(raised_errors)
            except Exception as e:
                logging.error(f"Error creating timescale records: {e}")
                errors.append(e)
    if len(errors) > 0:
        raise Exception(errors)

                
