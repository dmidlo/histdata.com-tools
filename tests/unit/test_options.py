"""Pytest unit tests for histdatacom.options.py."""

from histdatacom.options import Options


def test_options() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_sidecar_options_default_to_legacy_execution() -> None:
    """Sidecar execution must stay opt-in during migration."""
    options = Options()

    assert not options.use_sidecar
    assert not options.sidecar_start
    assert options.sidecar_wait_result
