from azure.data.tables import TableServiceClient, TableEntity, UpdateMode, TableClient
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
import logging
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
    return TableServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )


# Function to ensure table exists
def ensure_table_exists(
    table_name: str, table_service_client: TableServiceClient
) -> TableClient:
    """
    Ensures that the specified table exists in Azure Table Storage.

    Parameters:
    - table_name (str): The name of the table to check or create.
    - table_service_client (TableServiceClient): The client to interact with Azure Table Storage.

    Raises:
    - Exception: If unable to ensure that the table exists.
    """
    try:
        return table_service_client.create_table_if_not_exists(table_name)
    except Exception as e:
        raise Exception(f"Failed to ensure table exists: {e}")


# Function to store an identifier in a table
def store_id(
    identifier: str, context: str, table_service_client: TableServiceClient
) -> bool:
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
    table = ensure_table_exists(context, table_service_client)
    entity = TableEntity(PartitionKey="messages", RowKey=identifier)
    try:
        table.create_entity(entity)
        return True
    except ResourceExistsError:
        return True  # Already exists, so the operation is idempotent
    except Exception as e:
        raise Exception(f"Failed to write to table: {e}")


# Function to check if an identifier already exists in a table
def check_duplicate(
    identifier: str, context: str, table_service_client: TableServiceClient
) -> bool:
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
    table = ensure_table_exists(context, table_service_client)
    try:
        table.get_entity("messages", identifier)
        return True  # Entity exists, so it's a duplicate
    except ResourceNotFoundError:  # Entity not found
        return False
    except Exception as e:
        raise Exception(f"Failed to check for duplicate: {e}")


def retrieve_serialized_object(
    key: str, table_name: str, table_service_client: TableServiceClient
) -> str | None:
    """
    Retrieves a serialised object from a specified Azure Table Storage table.

    Parameters:
    - key (str): The unique identifier to retrieve.
    - table_name (str): The table where the identifier will be retrieved.
    - table_service_client (TableServiceClient): The client to interact with Azure Table Storage.

    Returns:
    - str: The serialised object if the operation was successful, None otherwise.

    Raises:
    - Exception: If unable to retrieve the serialised object.
    """
    try:
        table_client: TableClient = table_service_client.get_table_client(table_name)
        # Fetching the entity
        entity: TableEntity = table_client.get_entity(
            partition_key=table_name, row_key=key
        )
        return entity.get("body")
    except ResourceNotFoundError:
        return None  # table or entity not found
    except Exception as e:
        logging.error(
            f"An error of type {type(e).__name__} occurred while fetching entity: {e}, Key: {key}, Table: {table_name}"
        )
        raise


# Function to store an object at a key in a table
def store_serialized_object(
    key: str,
    table_name: str,
    serialised_object: str,
    table_service_client: TableServiceClient,
    overwrite: bool = True,
) -> bool:
    """
    Stores a serialised object in a specified Azure Table Storage table.

    Parameters:
    - key (str): The unique identifier to store.
    - table_name (str): The table where the identifier will be stored.
    - serialised_object (str): The serialised object to store.
    - table_service_client (TableServiceClient): The client to interact with Azure Table Storage.
    - overwrite (bool, optional): Whether to overwrite an existing object with the same key. Defaults to True.

    Returns:
    - bool: True if the operation was successful, False otherwise.

    Raises:
    - OverwriteError: If the key already exists and overwrite is False.
    - StorageError: If unable to store the serialised object.

    Example:
    >>> store_serialized_object('some_key', 'some_table', 'some_object', table_service_client)
    True
    """
    table_client: TableClient = ensure_table_exists(table_name, table_service_client)

    try:
        # Checking if the entity already exists
        existing_entity: TableEntity = table_client.get_entity(
            partition_key=table_name, row_key=key
        )
        if existing_entity and not overwrite:
            raise Exception(
                f"Entity with key {key} already exists and overwrite is False"
            )
    except ResourceNotFoundError:
        pass
    except Exception as e:
        logging.error(
            f"An error occurred while fetching entity: {e}, Key: {key}, Table: {table_name}"
        )
        raise

    try:
        # Constructing the entity to be stored
        entity = TableEntity(
            PartitionKey=table_name, RowKey=key, body=serialised_object
        )
        # Storing the entity
        table_client.upsert_entity(entity, mode=UpdateMode.REPLACE)
        return True
    except Exception as e:
        logging.error(f"Failed to write to table: {e}, Key: {key}, Table: {table_name}")
        raise
