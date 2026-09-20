"""Closed deterministic transition policy, independent of wall scheduling."""

from .contracts import (
    BrokerLifecycleError,
    BrokerLifecycleReason as Reason,
    BrokerLifecycleState as State,
)

_ALLOWED = {
    State.DISCOVERED: {State.CONFIGURED, State.STOPPING, State.FAILED},
    State.CONFIGURED: {State.STARTING, State.STOPPING, State.FAILED},
    State.STARTING: {
        State.ACTIVE,
        State.DEGRADED,
        State.STOPPING,
        State.FAILED,
    },
    State.ACTIVE: {State.DEGRADED, State.STOPPING},
    State.DEGRADED: {State.RECONNECTING, State.STOPPING, State.FAILED},
    State.RECONNECTING: {State.STARTING, State.STOPPING, State.FAILED},
    State.STOPPING: {State.STOPPED, State.FAILED},
    State.STOPPED: set(),
    State.FAILED: set(),
}
_TARGET_REASONS = {
    State.CONFIGURED: {Reason.CONFIGURED},
    State.STARTING: {Reason.STARTING},
    State.ACTIVE: {Reason.ACTIVE},
    State.RECONNECTING: {Reason.RETRY},
    State.DEGRADED: {Reason.GAP, Reason.RECONNECT, Reason.HEALTH_ERROR},
    State.STOPPED: {Reason.CLOSED, Reason.CANCELLED},
    State.STOPPING: {
        Reason.EOF,
        Reason.CANCELLED,
        Reason.STARTUP_TIMEOUT,
        Reason.RUN_TIMEOUT,
        Reason.SHUTDOWN_TIMEOUT,
        Reason.QUEUE_SATURATED,
        Reason.OUTPUT_LIMIT,
        Reason.FRAME_LIMIT,
        Reason.MALFORMED_IPC,
        Reason.MALFORMED_EVENT,
        Reason.PLUGIN_FAILURE,
        Reason.WORKER_DIED,
        Reason.FORCED,
        Reason.RECONNECT,
        Reason.RETRY_EXHAUSTED,
        Reason.EVENT_LIMIT,
        Reason.HEALTH_LIMIT,
        Reason.PERSISTENCE,
        Reason.INTEGRITY,
    },
    State.FAILED: {
        Reason.CANCELLED,
        Reason.STARTUP_TIMEOUT,
        Reason.RUN_TIMEOUT,
        Reason.SHUTDOWN_TIMEOUT,
        Reason.QUEUE_SATURATED,
        Reason.OUTPUT_LIMIT,
        Reason.FRAME_LIMIT,
        Reason.MALFORMED_IPC,
        Reason.MALFORMED_EVENT,
        Reason.PLUGIN_FAILURE,
        Reason.WORKER_DIED,
        Reason.FORCED,
        Reason.RETRY_EXHAUSTED,
        Reason.EVENT_LIMIT,
        Reason.HEALTH_LIMIT,
        Reason.PERSISTENCE,
        Reason.INTEGRITY,
    },
}


def validate_transition(
    previous: State, current: State, reason: Reason
) -> None:
    if (
        type(previous) is not State
        or type(current) is not State
        or type(reason) is not Reason
        or current not in _ALLOWED[previous]
    ):
        raise BrokerLifecycleError(Reason.INTEGRITY)
    if current in _TARGET_REASONS and reason not in _TARGET_REASONS[current]:
        raise BrokerLifecycleError(Reason.INTEGRITY)
