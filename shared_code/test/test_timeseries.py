import pytest
from unittest.mock import patch
from shared_code import timeseries


class Test_get_record_type:
    def test_with_string(self):
        actual_value = timeseries.get_record_type("a string")
        assert actual_value == timeseries.PayloadType.STRING

    def test_with_int(self):
        actual_value = timeseries.get_record_type(1)
        assert actual_value == timeseries.PayloadType.NUMBER

    def test_with_float(self):
        actual_value = timeseries.get_record_type(1.1)
        assert actual_value == timeseries.PayloadType.NUMBER

    def test_with_none(self):
        with pytest.raises(TypeError, match=r".*Unknown payload type: NoneType.*"):
            timeseries.get_record_type(None)

    def test_with_empty_string(self):
        actual_value = timeseries.get_record_type("")
        assert actual_value == timeseries.PayloadType.STRING

    def test_with_boolean(self):
        actual_value = timeseries.get_record_type(True)
        assert actual_value == timeseries.PayloadType.BOOLEAN

    def test_with_dict(self):
        with pytest.raises(TypeError, match=r".*Unknown payload type: dict.*"):
            timeseries.get_record_type({"a": 1})

    def test_with_invalid_list(self):
        with pytest.raises(
            TypeError, match=r".*List is not a valid coordinate pair: .*"
        ):
            timeseries.get_record_type(["a", 1])

    def test_with_invalid_list_of_three(self):
        with pytest.raises(
            TypeError, match=r".*List is not a valid coordinate pair: .*"
        ):
            timeseries.get_record_type([40.7128, -74.0062, 10])

    def test_with_valid_geography_list(self):
        actual_value = timeseries.get_record_type([40.7128, -74.0062])
        assert actual_value == timeseries.PayloadType.GEOGRAPHY


class TestCreateAtomicRecord:
    def test_standard_use_case(self):
        record = timeseries.create_atomic_record(
            "2023-01-01T00:00:00+00:00",
            "electricitymeter",
            "glow",
            "power",
            100,
            timeseries.PayloadType.NUMBER,
            "correlation_id_123",
        )
        expected_record = {
            "timestamp": "2023-01-01T00:00:00+00:00",
            "measurement_subject": "electricitymeter",
            "measurement_publisher": "glow",
            "measurement_of": "power",
            "measurement_value": 100,
            "measurement_data_type": timeseries.PayloadType.NUMBER.value,
            "correlation_id": "correlation_id_123",
        }
        assert record == expected_record

    @pytest.mark.parametrize("value", ["string value", 123.45, [1, 2, 3]])
    def test_different_data_types_for_measurement_value(self, value):
        record = timeseries.create_atomic_record(
            "2023-01-01T00:00:00+00:00",
            "electricitymeter",
            "glow",
            "power",
            value,
            timeseries.PayloadType.STRING,
        )
        assert record["measurement_value"] == value

    def test_optional_correlation_id(self):
        record_with_id = timeseries.create_atomic_record(
            "2023-01-01T00:00:00+00:00",
            "electricitymeter",
            "glow",
            "power",
            100,
            timeseries.PayloadType.NUMBER,
            "correlation_id_123",
        )
        record_without_id = timeseries.create_atomic_record(
            "2023-01-01T00:00:00+00:00",
            "electricitymeter",
            "glow",
            "power",
            100,
            timeseries.PayloadType.NUMBER,
        )
        assert record_with_id["correlation_id"] == "correlation_id_123"
        assert record_without_id["correlation_id"] is None

    def test_boundary_cases(self):
        record = timeseries.create_atomic_record(
            "", " ", "@!#", "\n\t", "", timeseries.PayloadType.NUMBER, ""
        )
        expected_record = {
            "timestamp": "",
            "measurement_subject": " ",
            "measurement_publisher": "@!#",
            "measurement_of": "\n\t",
            "measurement_value": "",
            "measurement_data_type": timeseries.PayloadType.NUMBER.value,
            "correlation_id": "",
        }
        assert record == expected_record


class TestCreateRecordRecursive:
    @patch("shared_code.timeseries.create_atomic_record")
    @patch("shared_code.timeseries.get_record_type")
    def test_standard_use_case(self, mock_get_record_type, mock_create_atomic_record):
        mock_get_record_type.side_effect = lambda x: "MockType"
        payload = {"level1": {"level2": {"data": 100}}}
        records = []
        timestamp = "2023-01-01T00:00:00+00:00"
        correlation_id = "test_correlation_id"
        measurement_publisher = "glow"
        measurement_subject = "electricitymeter"

        result = timeseries.create_record_recursive(
            payload,
            records,
            timestamp,
            correlation_id,
            measurement_publisher,
            measurement_subject,
        )

        assert len(result) == 1
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp=timestamp,
            measurement_publisher=measurement_publisher,
            measurement_subject=measurement_subject,
            measurement_of="data",
            measurement_value=100,
            measurement_data_type="MockType",
            correlation_id=correlation_id,
        )

    @patch("shared_code.timeseries.create_atomic_record")
    @patch("shared_code.timeseries.get_record_type")
    def test_empty_payload(self, mock_get_record_type, mock_create_atomic_record):
        records = []
        result = timeseries.create_record_recursive(
            {},
            records,
            "2023-01-01T00:00:00+00:00",
            "test_correlation_id",
            "glow",
            "electricitymeter",
        )
        assert result == records
        mock_create_atomic_record.assert_not_called()

    @patch("shared_code.timeseries.create_atomic_record")
    @patch("shared_code.timeseries.get_record_type")
    def test_payload_with_ignored_keys(
        self, mock_get_record_type, mock_create_atomic_record
    ):
        payload = {"ignore": 100, "data": 200}
        records = []
        ignore_keys = ["ignore"]
        result = timeseries.create_record_recursive(
            payload,
            records,
            "2023-01-01T00:00:00+00:00",
            "test_correlation_id",
            "glow",
            "electricitymeter",
            ignore_keys,
        )
        assert len(result) == 1
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp="2023-01-01T00:00:00+00:00",
            measurement_publisher="glow",
            measurement_subject="electricitymeter",
            measurement_of="data",
            measurement_value=200,
            measurement_data_type=mock_get_record_type.return_value,
            correlation_id="test_correlation_id",
        )

    @patch("shared_code.timeseries.create_atomic_record")
    @patch("shared_code.timeseries.get_record_type")
    def test_measurement_prefix(self, mock_get_record_type, mock_create_atomic_record):
        payload = {"data": 200}
        records = []
        measurement_of_prefix = "prefix"
        result = timeseries.create_record_recursive(
            payload,
            records,
            "2023-01-01T00:00:00+00:00",
            "test_correlation_id",
            "glow",
            "electricitymeter",
            None,
            measurement_of_prefix,
        )
        assert len(result) == 1
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp="2023-01-01T00:00:00+00:00",
            measurement_publisher="glow",
            measurement_subject="electricitymeter",
            measurement_of="prefix_data",
            measurement_value=200,
            measurement_data_type=mock_get_record_type.return_value,
            correlation_id="test_correlation_id",
        )

    @patch("shared_code.timeseries.create_atomic_record")
    @patch("shared_code.timeseries.get_record_type")
    def test_deeply_nested_payload(
        self, mock_get_record_type, mock_create_atomic_record
    ):
        payload = {"level1": {"level2": {"data": 100}}}
        records = []
        result = timeseries.create_record_recursive(
            payload,
            records,
            "2023-01-01T00:00:00+00:00",
            "test_correlation_id",
            "glow",
            "electricitymeter",
        )
        assert len(result) == 1
        mock_create_atomic_record.assert_called_once_with(
            source_timestamp="2023-01-01T00:00:00+00:00",
            measurement_publisher="glow",
            measurement_subject="electricitymeter",
            measurement_of="data",
            measurement_value=100,
            measurement_data_type=mock_get_record_type.return_value,
            correlation_id="test_correlation_id",
        )
