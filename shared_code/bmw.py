from typing import List
from bimmer_connected.account import MyBMWAccount
from bimmer_connected.vehicle import MyBMWVehicle
from bimmer_connected.api.regions import Regions
from bimmer_connected.utils import MyBMWJSONEncoder
import asyncio
import json


# from azure.eventhub import EventHubProducerClient, EventData
# import json
import os

from dotenv_vault import load_dotenv

load_dotenv()


def get_vehicle_by_vin(
    account: MyBMWAccount, vin: List[str]
) -> List[MyBMWVehicle] | None:
    """
    Fetches vehicles from a ConnectedDrive account based on a list of VINs.

    Parameters:
        account (ConnectedDriveAccount): The ConnectedDrive account containing the vehicles.
        vin (List[str]): A list of Vehicle Identification Numbers (VINs) to search for.

    Returns:
        Union[List[MyBMWVehicle], None]: A list of MyBMWVehicle objects whose VINs match any of the VINs in the provided list.
                                        Returns None if no matches are found.

    """  # noqa: E501
    asyncio.run(account.get_vehicles())
    matching_vehicles = [vehicle for vehicle in account.vehicles if vehicle.vin in vin]
    return matching_vehicles if matching_vehicles else None


def get_bmw_region_from_string(region: str) -> Regions:
    """
    Converts a string to a Regions enum.

    Parameters:
        region (str): A string representing a region.

    Returns:
        Regions: A Regions enum.

    """  # noqa: E501
    return Regions[region.upper()]


def get_bmw_account() -> MyBMWAccount:
    """
    Creates and returns a MyBMWAccount object using environment variables.

    Environment variables used:
        BMW_USERNAME: The username for the ConnectedDrive account.
        BMW_PASSWORD: The password for the ConnectedDrive account.
        BMW_REGION: The region for the ConnectedDrive account, converted to the correct enum using get_bmw_region_from_string().

    Returns:
        MyBMWAccount: An initialized MyBMWAccount object.
    """  # noqa: E501
    username = os.environ["BMW_USERNAME"]
    password = os.environ["BMW_PASSWORD"]
    region = get_bmw_region_from_string(os.environ["BMW_REGION"])
    return MyBMWAccount(username, password, region)


def get_my_cars():
    """Retrieves a list of MyBMWVehicle objects associated with specific VINs from a ConnectedDrive account.

    Environment variables used:
        BMW_VINS: Comma-separated list of Vehicle Identification Numbers (VINs) to search for.

    Returns:
        List[MyBMWVehicle]: A list of MyBMWVehicle objects whose VINs match those specified in the BMW_VINS environment variable.

    Raises:
        Exception: If no cars are found matching the VINs specified in the BMW_VINS environment variable.
    """  # noqa: E501
    account = get_bmw_account()
    my_vins = os.environ["BMW_VINS"].split(",")
    my_cars = get_vehicle_by_vin(account, my_vins)
    if my_cars is None:
        raise Exception("No cars found")
    return my_cars


def serialise_car_data(car: MyBMWVehicle) -> str:
    return json.dumps(car.data, cls=MyBMWJSONEncoder)


def get_and_serialise_car_data():
    """Retrieves a list of MyBMWVehicle objects associated with specific VINs from a ConnectedDrive account, and serialises the data.

    Environment variables used:
        BMW_VINS: Comma-separated list of Vehicle Identification Numbers (VINs) to search for.
        BMW_USERNAME: The username for the ConnectedDrive account.
        BMW_PASSWORD: The password for the ConnectedDrive account.
        BMW_REGION: The region for the ConnectedDrive account, converted to the correct enum using get_bmw_region_from_string().

    Returns:
        List[str]: A list of JSON strings representing the data for each vehicle.

    Raises:
        Exception: If no cars are found matching the VINs specified in the BMW_VINS environment variable.
    """  # noqa: E501
    cars = get_my_cars()
    return [serialise_car_data(car) for car in cars]
