from dotenv_vault import load_dotenv
from typing import List

import azure.functions as func

from shared_code.timescale import store_data

# from shared_code import (
#     create_single_timescale_record,
#     get_connection_string,
#     get_table_name,
# )

load_dotenv()


def main(events: List[func.EventHubEvent]):
    # print(f"Connection string: {get_connection_string()}")
    # print(f"Table name: {get_table_name()}")
    store_data(events)
