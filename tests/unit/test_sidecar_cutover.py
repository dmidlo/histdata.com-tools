"""Tests for the Temporal sidecar cutover policy boundary."""

from histdatacom.sidecar.cutover import (
    FOREGROUND_RUNTIME_REMOVED_MESSAGE,
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
    assert "removed" in policy["foreground_lifecycle"]
    assert "--foreground" in policy["foreground_lifecycle"]
    assert "rejected" in policy["foreground_lifecycle"]
    assert "config.ARGS" in policy["config_globals_lifecycle"]
    assert "RunRequest" in policy["config_globals_lifecycle"]


def test_runtime_selection_rejects_removed_foreground_opt_out() -> None:
    """CLI/API cutover selection should reject the retired runtime."""
    assert should_submit_to_sidecar({})
    assert selected_runtime({}) == SIDECAR_RUNTIME
    assert should_submit_to_sidecar({"use_sidecar": True})
    assert selected_runtime({"use_sidecar": True}) == SIDECAR_RUNTIME
    try:
        should_submit_to_sidecar({"use_sidecar": False})
    except ValueError as err:
        assert str(err) == FOREGROUND_RUNTIME_REMOVED_MESSAGE
    else:  # pragma: no cover
        raise AssertionError("foreground opt-out should be rejected")


def test_foreground_removed_message_is_visible() -> None:
    """Foreground removal should have a stable operator-facing message."""
    assert "foreground compatibility runtime has been removed" in (
        FOREGROUND_RUNTIME_REMOVED_MESSAGE
    )
