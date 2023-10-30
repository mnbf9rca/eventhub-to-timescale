import logging
from dotenv_vault import load_dotenv
from typing import List

import azure.functions as func
import psycopg

from shared_code import (
    create_single_timescale_record,
    get_connection_string,
    get_table_name,
)

load_dotenv()


def main(events: List[func.EventHubEvent]):
    # print(f"Connection string: {get_connection_string()}")
    # print(f"Table name: {get_table_name()}")
    conn = psycopg.connect(get_connection_string())
    errors: List[Exception] = []
    with conn:  # will close the connection when done
        for event in events:
            try:
                record_batch = event.get_body().decode("utf-8")
                if raised_errors := create_single_timescale_record(
                    conn, record_batch, get_table_name()
                ):
                    errors.extend(raised_errors)
            except Exception as e:
                logging.error(f"Error creating timescale records: {e}")
                errors.append(e)
    if errors:
        raise Exception(errors)
