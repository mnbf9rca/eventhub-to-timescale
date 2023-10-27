import azure.functions as func
from shared_code.bmw import get_and_serialise_car_data


def main(mytimer: func.TimerRequest, outputEventHubMessage: func.Out[list]) -> None:
    data = get_and_serialise_car_data()
    outputEventHubMessage.set(data)
