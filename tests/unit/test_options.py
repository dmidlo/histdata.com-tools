"""Pytest unit tests for histdatacom.options.py."""

from histdatacom.options import Options


def test_options() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_sidecar_options_default_to_production_sidecar() -> None:
    """Sidecar execution is now the production default runtime."""
    options = Options()

    assert options.use_sidecar
    assert options.sidecar_start
    assert options.sidecar_wait_result
