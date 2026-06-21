"""Pytest unit tests for histdatacom.fx_enums.py."""

from histdatacom.fx_enums import TimePrecision


def test_fx_enums() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_time_precision_values_do_not_require_influxdb_client() -> None:
    """Influx precision metadata should stay importable without Influx."""
    assert TimePrecision.ASCII_M1.value == "s"
    assert TimePrecision.ASCII_T.value == "ms"
    assert TimePrecision.list_values() == {"s", "ms"}
