import json
import os
import sys
import pytest
import asyncio
from unittest.mock import MagicMock, patch

from bimmer_connected.api.regions import Regions
from bimmer_connected.account import MyBMWAccount
from bimmer_connected.vehicle import MyBMWVehicle
from bimmer_connected.utils import MyBMWJSONEncoder


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
from shared_code.bmw import (
    get_vehicle_by_vin,
    get_bmw_region_from_string,
    get_bmw_account,
    get_my_cars,
    serialise_car_data
)


# Mock MyBMWVehicle class
class MockMyBMWVehicle:
    def __init__(self, vin):
        self.vin = vin


# Mock asyncio.run
async def mock_asyncio_run(coroutine):
    return await coroutine


# Test class
class TestGetVehicleByVIN:
    @pytest.fixture
    def mock_account(self):
        account = MagicMock()
        account.vehicles = [
            MockMyBMWVehicle("123"),
            MockMyBMWVehicle("456"),
            MockMyBMWVehicle("789"),
        ]
        return account

    @pytest.mark.asyncio
    async def test_no_matching_vehicles(self, mock_account):
        with patch("asyncio.run", new=mock_asyncio_run):
            result = get_vehicle_by_vin(mock_account, ["999", "111"])
            assert result is None

    @pytest.mark.asyncio
    async def test_single_matching_vehicle(self, mock_account):
        with patch("asyncio.run", new=mock_asyncio_run):
            result = get_vehicle_by_vin(mock_account, ["123"])
            assert len(result) == 1
            assert result[0].vin == "123"

    @pytest.mark.asyncio
    async def test_multiple_matching_vehicles(self, mock_account):
        with patch("asyncio.run", new=mock_asyncio_run):
            result = get_vehicle_by_vin(mock_account, ["123", "789"])
            assert len(result) == 2
            assert [vehicle.vin for vehicle in result] == ["123", "789"]

    @pytest.mark.asyncio
    async def test_get_vehicles_called(self, mock_account):
        with patch("asyncio.run", new=mock_asyncio_run):
            get_vehicle_by_vin(mock_account, ["123"])
            mock_account.get_vehicles.assert_called_once()


class TestGetBMWRegionFromString:
    def test_valid_north_america(self):
        assert get_bmw_region_from_string("north_america") == Regions.NORTH_AMERICA

    def test_valid_china(self):
        assert get_bmw_region_from_string("china") == Regions.CHINA

    def test_valid_rest_of_world(self):
        assert get_bmw_region_from_string("rest_of_world") == Regions.REST_OF_WORLD

    def test_case_insensitivity(self):
        assert get_bmw_region_from_string("NoRtH_AmErIcA") == Regions.NORTH_AMERICA

    def test_invalid_region(self):
        with pytest.raises(KeyError):
            get_bmw_region_from_string("invalid_region")


# Test class
class TestGetBMWAccount:
    @pytest.fixture
    def mock_environ(self):
        with patch.dict(
            os.environ,
            {
                "BMW_USERNAME": "testuser",
                "BMW_PASSWORD": "testpass",
                "BMW_REGION": "north_america",
            },
        ):
            yield

    @pytest.fixture
    def mock_MyBMWAccount(self):
        with patch("shared_code.bmw.MyBMWAccount", autospec=True) as MockedMyBMWAccount:
            yield MockedMyBMWAccount

    def test_get_bmw_account(self, mock_environ, mock_MyBMWAccount):
        expected_account = MagicMock()
        mock_MyBMWAccount.return_value = (
            expected_account
        )

        actual_account = get_bmw_account()

        mock_MyBMWAccount.assert_called_once_with(
            "testuser", "testpass", Regions.NORTH_AMERICA
        )
        assert (
            actual_account is expected_account
        )  # Assert that the returned account is the specific MagicMock instance


class TestGetMyCars:

    @pytest.fixture
    def mock_environ(self):
        with patch.dict(os.environ, {'BMW_VINS': '123,456'}):
            yield

    @pytest.fixture
    def mock_account(self):
        return MagicMock()

    @pytest.fixture
    def mock_cars(self):
        return [MagicMock(spec=MyBMWVehicle), MagicMock(spec=MyBMWVehicle)]

    def test_get_my_cars_success(self, mock_environ, mock_account, mock_cars):
        with patch('shared_code.bmw.get_bmw_account', return_value=mock_account), \
             patch('shared_code.bmw.get_vehicle_by_vin', return_value=mock_cars):
            
            result = get_my_cars()

        assert result == mock_cars

    def test_get_my_cars_no_cars_found(self, mock_environ, mock_account):
        with patch('shared_code.bmw.get_bmw_account', return_value=mock_account), \
             patch('shared_code.bmw.get_vehicle_by_vin', return_value=None):
            
            with pytest.raises(Exception) as e:
                get_my_cars()

        assert str(e.value) == 'No cars found'


class TestSerialiseCarData:

    def setup_method(self):
        self.mock_car = MagicMock(spec=MyBMWVehicle)
        self.mock_car.data = {
            'attribute': 'value',
        }

    def test_serialise_car_data(self):
        expected_data = {'attribute': 'value'}
        self.mock_car.data = expected_data
        with patch('json.dumps') as mock_json_dumps:

            expected_json = '{"attribute": "value"}' 
            mock_json_dumps.return_value = expected_json

            result = serialise_car_data(self.mock_car)

        mock_json_dumps.assert_called_once_with(expected_data, cls=MyBMWJSONEncoder)
        assert result == expected_json

