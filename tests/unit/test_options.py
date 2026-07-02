"""Pytest unit tests for histdatacom.options.py."""

from histdatacom.options import Options


def test_options() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_orchestration_options_default_to_production_runtime() -> None:
    """Orchestration execution is the production default runtime."""
    options = Options()

    assert options.use_orchestration
    assert options.orchestration_start
    assert not options.orchestration_keep_runtime
    assert options.orchestration_wait_result
    assert not options.no_overlap
    assert options.schedule_key == ""


def test_options_reject_unknown_runtime_fields() -> None:
    """Only the documented Options surface should accept assignment."""
    options = Options()

    try:
        options.unpublished_runtime_toggle = False
    except AttributeError:
        pass
    else:  # pragma: no cover - assertion branch
        raise AssertionError("Options accepted an unknown runtime field")
