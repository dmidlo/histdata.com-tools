"""Nonce comparisons over verified observations, never new execution or replay."""

from __future__ import annotations

from histdatacom.broker_plugins import BrokerSessionV1


def session_nonces_are_fresh(
    sessions: tuple[BrokerSessionV1, ...],
    *,
    earlier: tuple[BrokerSessionV1, ...] = (),
) -> bool:
    """Compare distinct openings, not repeated reads of one retained session.

    Callers first verify the physical-run, candidate, event and provenance
    bindings. This predicate establishes observed uniqueness only; it cannot
    prove entropy or guarantee uniqueness across unobserved future executions.
    """
    if not sessions or any(
        type(session) is not BrokerSessionV1 for session in sessions + earlier
    ):
        return False
    current_nonces = {session.instance_nonce for session in sessions}
    earlier_nonces = {session.instance_nonce for session in earlier}
    return (
        len(current_nonces) == len(sessions)
        and len(earlier_nonces) == len(earlier)
        and current_nonces.isdisjoint(earlier_nonces)
    )
