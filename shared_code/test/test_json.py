import pytest

from shared_code import json


class TestSendToConverter:
    @pytest.fixture()
    def mock_glow_to_timescale(self, mocker):
        return mocker.patch("shared_code.json.glow_to_timescale")

    @pytest.fixture()
    def mock_homie_to_timescale(self, mocker):
        return mocker.patch("shared_code.json.homie_to_timescale")

    @pytest.fixture()
    def mock_emon_to_timescale(self, mocker):
        return mocker.patch("shared_code.json.emon_to_timescale")

    @pytest.fixture()
    def mock_logger(self, mocker):
        return mocker.patch("shared_code.json.logging")

    @pytest.mark.parametrize(
        "publisher, expected_converter_name",
        [
            ("glow", "mock_glow_to_timescale"),
            ("homie", "mock_homie_to_timescale"),
            ("emon", "mock_emon_to_timescale"),
        ],
    )
    def test_send_to_converter(
        self,
        mock_glow_to_timescale,
        mock_homie_to_timescale,
        mock_emon_to_timescale,
        mock_logger,
        publisher,
        expected_converter_name,
        request: pytest.FixtureRequest,
    ):
        all_converters = [
            mock_glow_to_timescale,
            mock_homie_to_timescale,
            mock_emon_to_timescale,
        ]
        expected_converter = request.getfixturevalue(expected_converter_name)
        mock_messagebody = "mock_messagebody"
        mock_topic = "mock_topic"

        result = json.send_to_converter(publisher, mock_messagebody, mock_topic)
        for converter in all_converters:
            if converter == expected_converter:
                converter.assert_called_once_with(
                    mock_messagebody, mock_topic, publisher
                )
            else:
                converter.assert_not_called()
        assert result == expected_converter.return_value

    def test_send_to_converter_invalid_converter(
        self,
        mock_glow_to_timescale,
        mock_homie_to_timescale,
        mock_emon_to_timescale,
        mock_logger,
    ):
        all_converters = [
            mock_glow_to_timescale,
            mock_homie_to_timescale,
            mock_emon_to_timescale,
        ]
        mock_publisher = "not_a_converter"
        mock_messagebody = "mock_messagebody"
        mock_topic = "mock_topic"

        with pytest.raises(ValueError) as e:
            json.send_to_converter(mock_publisher, mock_messagebody, mock_topic)
        assert str(e.value) == f"Unknown publisher: {mock_publisher}"
        for converter in all_converters:
            converter.assert_not_called()
        assert mock_logger.error.call_count == 1
