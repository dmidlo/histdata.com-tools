"""Tests for the Temporal sidecar cutover policy boundary."""

from histdatacom.sidecar.cutover import (
    FOREGROUND_RUNTIME,
    SIDECAR_RUNTIME,
    cutover_policy_payload,
    selected_runtime,
    should_submit_to_sidecar,
)


def test_cutover_policy_uses_sidecar_as_default_runtime() -> None:
    """The production default should now be the sidecar runtime."""
    policy = cutover_policy_payload()

    assert policy["default_runtime"] == SIDECAR_RUNTIME
    assert "default" in policy["sidecar_activation"]
    assert "compatibility" in policy["foreground_lifecycle"]
    assert "config.ARGS" in policy["config_globals_lifecycle"]


def test_runtime_selection_allows_explicit_foreground_opt_out() -> None:
    """CLI/API cutover selection should allow foreground opt-out."""
    assert should_submit_to_sidecar({})
    assert selected_runtime({}) == SIDECAR_RUNTIME
    assert not should_submit_to_sidecar({"use_sidecar": False})
    assert selected_runtime({"use_sidecar": False}) == FOREGROUND_RUNTIME
    assert should_submit_to_sidecar({"use_sidecar": True})
    assert selected_runtime({"use_sidecar": True}) == SIDECAR_RUNTIME
