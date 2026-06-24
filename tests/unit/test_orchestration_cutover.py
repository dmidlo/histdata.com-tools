"""Tests for the Temporal orchestration cutover policy boundary."""

from histdatacom.orchestration.cutover import (
    FOREGROUND_RUNTIME_REMOVED_MESSAGE,
    ORCHESTRATION_RUNTIME,
    cutover_policy_payload,
    selected_runtime,
    should_submit_to_orchestration,
)


def test_cutover_policy_uses_orchestration_as_default_runtime() -> None:
    """The production default should be the orchestration runtime."""
    policy = cutover_policy_payload()

    assert policy["default_runtime"] == ORCHESTRATION_RUNTIME
    assert "default" in policy["orchestration_activation"]
    assert "Temporal orchestration" in policy["orchestration_activation"]
    assert "removed" in policy["foreground_lifecycle"]
    assert "--foreground" in policy["foreground_lifecycle"]
    assert "Options.use_orchestration" in policy["foreground_lifecycle"]
    assert "rejected" in policy["foreground_lifecycle"]
    assert "ambient parser state" in policy["config_globals_lifecycle"]
    assert "RunRequest" in policy["config_globals_lifecycle"]
    assert "orchestration requests" in policy["unavailable_runtime_behavior"]


def test_runtime_selection_rejects_removed_foreground_opt_out() -> None:
    """CLI/API cutover selection should reject the retired runtime."""
    assert should_submit_to_orchestration({})
    assert selected_runtime({}) == ORCHESTRATION_RUNTIME
    assert should_submit_to_orchestration({"use_orchestration": True})
    assert (
        selected_runtime({"use_orchestration": True}) == ORCHESTRATION_RUNTIME
    )
    try:
        should_submit_to_orchestration({"use_orchestration": False})
    except ValueError as err:
        assert str(err) == FOREGROUND_RUNTIME_REMOVED_MESSAGE
    else:  # pragma: no cover
        raise AssertionError("foreground opt-out should be rejected")


def test_foreground_removed_message_is_visible() -> None:
    """Foreground removal should have a stable operator-facing message."""
    assert "foreground compatibility runtime has been removed" in (
        FOREGROUND_RUNTIME_REMOVED_MESSAGE
    )
