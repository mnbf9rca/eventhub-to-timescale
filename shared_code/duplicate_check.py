from azure.data.tables import TableServiceClient, TableEntity
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from dotenv import load_dotenv
import os

load_dotenv()


# Function to initialize table service client
def get_table_service_client() -> TableServiceClient:
    """
    Initializes and returns a TableServiceClient for Azure Table Storage.

    Returns:
    - TableServiceClient: The initialized client.

    Raises:
    - Exception: If unable to initialize the client.
    """
    return TableServiceClient.from_connection_string(os.environ["AZURE_STORAGE_CONNECTION_STRING"])


# Function to ensure table exists
def ensure_table_exists(table_name: str, table_service_client: TableServiceClient):
    """
    Ensures that the specified table exists in Azure Table Storage.

    Parameters:
    - table_name (str): The name of the table to check or create.
    - table_service_client (TableServiceClient): The client to interact with Azure Table Storage.

    Raises:
    - Exception: If unable to ensure that the table exists.
    """
    try:
        table_service_client.create_table(table_name)
    except ResourceExistsError:
        pass  # Table already exists, no action needed
    except Exception as e:
        raise Exception(f"Failed to ensure table exists: {e}")


# Function to store an identifier in a table
def store_id(identifier: str, context: str, table_service_client: TableServiceClient) -> bool:
    """
    Stores a unique identifier in a specified Azure Table Storage table.

    Parameters:
    - identifier (str): The unique identifier to store.
    - context (str): The table where the identifier will be stored.
    - table_service_client (TableServiceClient): The client to interact with Azure Table Storage.

    Returns:
    - bool: True if the operation was successful, False otherwise.

    Raises:
    - Exception: If unable to store the identifier.
    """
    ensure_table_exists(context, table_service_client)
    table_client = table_service_client.get_table_client(context)
    entity = TableEntity(PartitionKey="messages", RowKey=identifier)
    try:
        table_client.create_entity(entity)
        return True
    except ResourceExistsError:
        return True  # Already exists, so the operation is idempotent
    except Exception as e:
        raise Exception(f"Failed to write to table: {e}")


# Function to check if an identifier already exists in a table
def check_duplicate(identifier: str, context: str, table_service_client: TableServiceClient) -> bool:
    """
    Checks if a unique identifier already exists in a specified Azure Table Storage table.

    Parameters:
    - identifier (str): The unique identifier to check.
    - context (str): The table where the identifier will be checked.
    - table_service_client (TableServiceClient): The client to interact with Azure Table Storage.

    Returns:
    - bool: True if the identifier is a duplicate, False otherwise.

    Raises:
    - Exception: If unable to check for the duplicate identifier.
    """
    ensure_table_exists(context, table_service_client)
    table_client = table_service_client.get_table_client(context)
    try:
        table_client.get_entity("messages", identifier)
        return True  # Entity exists, so it's a duplicate
    except ResourceNotFoundError:  # Entity not found
        return False
    except Exception as e:
        raise Exception(f"Failed to check for duplicate: {e}")
