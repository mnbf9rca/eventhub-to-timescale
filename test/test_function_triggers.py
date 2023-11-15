from unittest.mock import patch

from bmw_to_timescale import main as bmw_to_timescale_main
from bmw_update import main as bmw_update_main
from json_to_timeseries import main as json_to_timeseries_main
from timeseries_to_timescale import main as timeseries_to_timescale_main


@patch("bmw_to_timescale.convert_bmw_to_timescale")
def test_bmw_to_timescale(mock_convert_bmw_to_timescale):
    bmw_to_timescale_main("event", "outputEventHubMessage", "outputEHMonitor")
    mock_convert_bmw_to_timescale.assert_called_once_with(
        "event", "outputEventHubMessage", "outputEHMonitor"
    )


@patch("bmw_update.get_and_serialise_car_data")
def test_bmw_update(mock_get_and_serialise_car_data):
    mock_get_and_serialise_car_data.return_value = "mytimer_data_return_value"
    return_value = bmw_update_main("mytimer_data")
    mock_get_and_serialise_car_data.assert_called_once_with()
    assert return_value == mock_get_and_serialise_car_data.return_value


@patch("json_to_timeseries.convert_json_to_timeseries")
def test_json_to_timeseries(mock_convert_json_to_timeseries):
    json_to_timeseries_main(["event"], ["outputEventHubMessage"])
    mock_convert_json_to_timeseries.assert_called_once_with(
        ["event"], ["outputEventHubMessage"]
    )


@patch("timeseries_to_timescale.store_data")
def test_timeseries_to_timescale(mock_store_data):
    timeseries_to_timescale_main(["event"])
    mock_store_data.assert_called_once_with(["event"])
