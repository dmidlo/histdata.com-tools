"""Tests for the Temporal sidecar cutover policy boundary."""

from histdatacom.sidecar.cutover import (
    FOREGROUND_RUNTIME,
    SIDECAR_RUNTIME,
    cutover_policy_payload,
    selected_runtime,
    should_submit_to_sidecar,
)


def test_cutover_policy_keeps_foreground_as_default_runtime() -> None:
    """The production default should stay explicit while migration continues."""
    policy = cutover_policy_payload()

    assert policy["default_runtime"] == FOREGROUND_RUNTIME
    assert "explicit" in policy["sidecar_activation"]
    assert "compatibility" in policy["foreground_lifecycle"]
    assert "config.ARGS" in policy["config_globals_lifecycle"]


def test_runtime_selection_requires_explicit_sidecar_flag() -> None:
    """CLI/API cutover selection should be controlled by use_sidecar."""
    assert not should_submit_to_sidecar({"use_sidecar": False})
    assert selected_runtime({"use_sidecar": False}) == FOREGROUND_RUNTIME
    assert should_submit_to_sidecar({"use_sidecar": True})
    assert selected_runtime({"use_sidecar": True}) == SIDECAR_RUNTIME
