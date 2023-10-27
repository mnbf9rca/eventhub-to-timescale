import azure.functions as func
from shared_code.bmw import get_and_serialise_car_data


def main(mytimer: func.TimerRequest) -> list:
    return get_and_serialise_car_data()
