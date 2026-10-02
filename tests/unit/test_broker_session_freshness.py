"""Synthetic session identity checks; no native capture, receipt or worker.

The fixture tests call the bundled generated implementation directly. Their
resources stub grants no authority and refuses any attempted resource access.
"""

from typing import cast

import pytest

from histdatacom.broker_plugin_conformance import fixture_plugin
from histdatacom.broker_plugin_conformance.fixtures import FIXTURE_FAULTS
from histdatacom.broker_plugin_conformance.sessions import (
    session_nonces_are_fresh,
)
from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerHostResourcesV1,
    BrokerPluginMetadataV1,
    BrokerSessionV1,
)


def _session(
    nonce: int,
    *,
    opened: int = 100,
    clock: str = "generated-clock",
    plugin: str = "org.example.session-freshness",
) -> BrokerSessionV1:
    metadata = BrokerPluginMetadataV1(plugin, "1.0.0", "Generated session")
    return BrokerSessionV1(metadata.artifact_id, f"{nonce:032x}", opened, clock)


@pytest.mark.parametrize("earlier", ((), (_session(1),)))
def test_empty_current_sessions_never_establish_freshness(earlier):
    assert session_nonces_are_fresh((), earlier=earlier) is False


@pytest.mark.parametrize("count", (1, 2, 8))
def test_one_run_requires_only_nonempty_unique_nonce_inventory(count):
    current = tuple(_session(index) for index in range(1, count + 1))
    assert session_nonces_are_fresh(current) is True
    assert session_nonces_are_fresh(current, earlier=()) is True


def test_equal_timestamps_are_not_duplicate_sessions_when_nonces_are_fresh():
    first, second = _session(1, opened=100), _session(2, opened=100)
    assert first.opened_at_utc_ns == second.opened_at_utc_ns
    assert session_nonces_are_fresh((first, second)) is True
    assert session_nonces_are_fresh((second,), earlier=(first,)) is True


def test_distinct_unique_current_and_earlier_inventories_are_fresh():
    current = (_session(3), _session(4))
    earlier = (_session(1), _session(2))
    assert session_nonces_are_fresh(current, earlier=earlier) is True
    assert (
        session_nonces_are_fresh(current[::-1], earlier=earlier[::-1]) is True
    )


def test_repeated_exact_current_session_is_not_fresh():
    first = _session(1)
    assert session_nonces_are_fresh((first, first)) is False


@pytest.mark.parametrize(
    "changed",
    (
        _session(1, opened=101),
        _session(1, clock="another-generated-clock"),
        _session(1, plugin="org.example.another-plugin"),
        _session(
            1,
            opened=102,
            clock="another-generated-clock",
            plugin="org.example.another-plugin",
        ),
    ),
)
def test_nonce_reuse_is_rejected_despite_distinct_artifact_ids(changed):
    first = _session(1)
    assert first.artifact_id != changed.artifact_id
    assert first.instance_nonce == changed.instance_nonce
    assert session_nonces_are_fresh((first, changed)) is False
    assert session_nonces_are_fresh((changed,), earlier=(first,)) is False


@pytest.mark.parametrize("change_time", (False, True))
def test_duplicate_earlier_inventory_is_not_mistaken_for_fresh_history(
    change_time,
):
    earlier = (_session(1), _session(1, opened=101 if change_time else 100))
    assert session_nonces_are_fresh((_session(2),), earlier=earlier) is False


def test_cross_run_nonce_reuse_is_rejected_for_any_member():
    earlier = (_session(1), _session(2))
    current = (_session(3), _session(2, opened=999))
    assert session_nonces_are_fresh(current, earlier=earlier) is False


class _NoResources:
    """Explicit direct-unit stub; it cannot supply a host permission or effect."""

    def __getattr__(self, name: str):
        raise AssertionError("session unit fixture attempted a host resource")


def _fixture():
    return fixture_plugin.ConformanceFixture(
        cast(BrokerHostResourcesV1, _NoResources())
    )


def _finite_semantics(plugin, session):
    events = tuple(plugin.iter_events(session))
    assert tuple(event.kind for event in events) == (
        BrokerEventKind.HEALTH,
        BrokerEventKind.QUOTE,
        BrokerEventKind.QUOTE,
        BrokerEventKind.QUOTE,
    )
    assert all(event.session_id == session.artifact_id for event in events)
    # Retain every semantic field, including source/receive clocks, quote values,
    # diagnostics and sequences. Only binding identity is expected to change.
    return tuple(
        {
            name: value
            for name, value in event.to_dict().items()
            if name not in {"artifact_id", "session_id"}
        }
        for event in events
    )


def test_real_generated_fixture_open_reopen_has_fresh_nonces(monkeypatch):
    monkeypatch.setattr(fixture_plugin, "FAULT", "none")
    plugin = _fixture()
    first = plugin.open_session({"mode": "finite"})
    first_semantics = _finite_semantics(plugin, first)
    plugin.close_session(first)
    second = plugin.open_session({"mode": "finite"})
    assert second.instance_nonce != first.instance_nonce
    assert second.metadata_id == first.metadata_id
    assert second.receive_clock_id == first.receive_clock_id
    assert session_nonces_are_fresh((first, second)) is True
    assert session_nonces_are_fresh((second,), earlier=(first,)) is True
    assert _finite_semantics(plugin, second) == first_semantics
    plugin.close_session(second)


def test_new_fixture_instances_do_not_restart_a_constant_nonce(monkeypatch):
    monkeypatch.setattr(fixture_plugin, "FAULT", "none")
    first_plugin, second_plugin = _fixture(), _fixture()
    first = first_plugin.open_session({"mode": "finite"})
    second = second_plugin.open_session({"mode": "finite"})
    assert session_nonces_are_fresh((second,), earlier=(first,)) is True
    first_plugin.close_session(first)
    second_plugin.close_session(second)


@pytest.mark.parametrize(
    "fault,changing_time",
    (
        ("reused-session-nonce", False),
        ("reused-session-nonce-changing-time", True),
    ),
)
def test_real_fault_variants_reuse_nonce_without_changing_event_semantics(
    monkeypatch, fault, changing_time
):
    assert fault in FIXTURE_FAULTS
    monkeypatch.setattr(fixture_plugin, "FAULT", "none")
    control = _fixture()
    control_session = control.open_session({"mode": "finite"})
    expected = _finite_semantics(control, control_session)
    control.close_session(control_session)

    monkeypatch.setattr(fixture_plugin, "FAULT", fault)
    plugin = _fixture()
    first = plugin.open_session({"mode": "finite"})
    first_semantics = _finite_semantics(plugin, first)
    plugin.close_session(first)
    second = plugin.open_session({"mode": "finite"})
    assert first.instance_nonce == second.instance_nonce
    assert first.metadata_id == second.metadata_id
    assert first.receive_clock_id == second.receive_clock_id
    if changing_time:
        assert first.opened_at_utc_ns != second.opened_at_utc_ns
        assert first.artifact_id != second.artifact_id
    else:
        assert first.opened_at_utc_ns == second.opened_at_utc_ns
        assert first.artifact_id == second.artifact_id
    assert session_nonces_are_fresh((first, second)) is False
    assert session_nonces_are_fresh((second,), earlier=(first,)) is False
    assert first_semantics == _finite_semantics(plugin, second) == expected
    plugin.close_session(second)
