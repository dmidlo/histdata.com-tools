"""Pytest unit tests for histdatacom.options.py."""

import pytest

from histdatacom.options import Options


def test_options() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_orchestration_options_default_to_production_runtime() -> None:
    """Orchestration execution is the production default runtime."""
    options = Options()

    assert options.use_orchestration
    assert options.orchestration_start
    assert options.orchestration_wait_result
    assert not hasattr(options, "use_sidecar")
    assert not hasattr(options, "sidecar_start")
    assert not hasattr(options, "sidecar_wait_result")


@pytest.mark.parametrize(
    ("removed_name", "replacement"),
    (
        ("use_sidecar", "use_orchestration"),
        ("sidecar_start", "orchestration_start"),
        ("sidecar_wait_result", "orchestration_wait_result"),
    ),
)
def test_removed_sidecar_option_names_are_rejected(
    removed_name: str,
    replacement: str,
) -> None:
    """Removed option names should fail instead of becoming stale state."""
    options = Options()

    with pytest.raises(AttributeError, match=replacement):
        setattr(options, removed_name, False)
