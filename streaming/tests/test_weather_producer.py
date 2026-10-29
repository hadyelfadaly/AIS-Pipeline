from datetime import datetime, timezone

import pytest

from producers import weather_producer


@pytest.mark.parametrize(
    ("latitude", "longitude", "expected"),
    [
        (0.0, 0.0, (0.5, 0.5)),
        (-0.1, -0.1, (-0.5, -0.5)),
        (40.7128, -74.006, (40.5, -74.5)),
    ],
)
def test_get_grid_coordinates(latitude, longitude, expected):
    assert weather_producer.get_grid_coordinates(latitude, longitude) == expected


def test_get_cell_id_uses_stable_two_decimal_format():
    assert weather_producer.get_cell_id(40.5, -74.5) == "40.50_-74.50"


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        (
            {
                "MetaData": {"latitude": "40.5", "longitude": "-74.5"},
                "Message": {},
            },
            (40.5, -74.5),
        ),
        (
            {
                "MetaData": {},
                "Message": {
                    "PositionReport": {"Latitude": 40.5, "Longitude": -74.5}
                },
            },
            (40.5, -74.5),
        ),
        (
            {"MetaData": {"latitude": 91, "longitude": 0}},
            None,
        ),
        (
            {"MetaData": {"latitude": "not-a-number", "longitude": 0}},
            None,
        ),
        ({}, None),
    ],
)
def test_extract_position(record, expected):
    assert weather_producer.extract_position(record) == expected


def test_parse_utc_hour_normalizes_timezone():
    parsed = weather_producer.parse_utc_hour("2026-10-29T12:00:00+03:00")

    assert parsed == datetime(2026, 10, 29, 9, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("12.5", 12.5),
        (None, None),
        ("invalid", None),
        ("nan", None),
        ("inf", None),
    ],
)
def test_to_float_or_none(value, expected):
    assert weather_producer.to_float_or_none(value) == expected
