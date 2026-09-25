"""Check generated third-party adversaries without fabricating host metrics."""

import ast
from dataclasses import replace
from pathlib import Path
import time

import pytest

from histdatacom.broker_plugins import (
    BrokerEventKind,
    BrokerEventV1,
    BrokerReasonCode,
)
from tests.fixtures.broker_host_health_external import HealthFixture
from tests.fixtures.broker_permission_wheel import permission_fixture_schema


class RefuseResources:
    def __getattr__(self, name):
        raise AssertionError(
            "health-only generated source requested a resource"
        )


@pytest.mark.parametrize(
    "mode", ["honest", "duplicate", "reorder", "gap", "delay", "unchanged"]
)
def test_public_sdk_generated_health_adversary_carries_only_native_evidence(
    mode,
):
    fixture = HealthFixture(RefuseResources())
    assert fixture.configuration_schema == permission_fixture_schema()
    session = fixture.open_session({"mode": mode})
    started = time.monotonic()
    events = tuple(fixture.iter_events(session))
    elapsed = time.monotonic() - started
    assert [event.sequence for event in events] == list(range(len(events)))
    assert all(
        BrokerEventV1.from_json(event.to_json()) == event for event in events
    )
    quotes = [event for event in events if event.kind is BrokerEventKind.QUOTE]
    assert len(quotes) == 3
    claims = [
        event
        for event in events
        if event.diagnostic is not None
        and event.diagnostic.code is BrokerReasonCode.HEALTHY
    ]
    assert len(claims) == (0 if mode == "honest" else 1)
    assert all(
        event.diagnostic.code is BrokerReasonCode.HEALTHY for event in claims
    )
    if mode == "duplicate":
        assert quotes[0].source_time == quotes[1].source_time
        assert quotes[0].quote == quotes[1].quote
        assert quotes[0].artifact_id != quotes[1].artifact_id
    elif mode == "reorder":
        assert [event.source_time.timestamp_ns for event in quotes] == [
            3_000_000_000,
            1_000_000_000,
            2_000_000_000,
        ]
    elif mode == "gap":
        gap = next(event.gap for event in events if event.gap is not None)
        assert gap.missing_message_count == 987654
    elif mode == "delay":
        assert elapsed >= 0.15
        assert (
            sum(event.kind is BrokerEventKind.HEARTBEAT for event in events)
            == 3
        )
    elif mode == "unchanged":
        assert len({event.quote.artifact_id for event in quotes}) == 1
        assert len({event.source_time.artifact_id for event in quotes}) == 3


def test_health_adversary_has_no_private_host_imports():
    path = Path(__file__).parents[1] / "fixtures/broker_host_health_external.py"
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module.startswith(
            "histdatacom"
        ):
            assert node.module == "histdatacom.broker_plugins"


def test_current_health_admission_rechecks_rights_after_observation_replay(
    tmp_path, monkeypatch
):
    from histdatacom.broker_plugin_health import runtime_legacy
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyRevocationV1,
    )
    from histdatacom.broker_plugin_policy.scope import BrokerPolicyError
    from tests.fixtures.broker_provider_policy import generated_provider_scope
    from tests.unit.test_broker_host_health_runtime_legacy import capture

    request, captured = capture(tmp_path)
    original = runtime_legacy._observations
    with generated_provider_scope(request) as source:
        context = source.current

        def revoke_while_reading(*args):
            for observation in original(*args):
                if not source.current.revocations:
                    source.current = replace(
                        context,
                        revocations=(
                            BrokerPolicyRevocationV1(
                                context.selected_policy_ids[0],
                                20,
                                20,
                                (context.evidence[0].artifact_id,),
                                "withdrawn",
                            ),
                        ),
                    )
                yield observation

        monkeypatch.setattr(
            runtime_legacy, "_observations", revoke_while_reading
        )
        with pytest.raises(BrokerPolicyError):
            runtime_legacy.require_legacy_capture_health(
                tmp_path, captured.manifest, provider_request=request
            )


@pytest.mark.parametrize("operation", ["read", "write_existing"])
def test_health_qualification_rechecks_rights_after_retained_reference_read(
    tmp_path, monkeypatch, operation
):
    from histdatacom.broker_capture import fit_broker_delivery_fingerprint
    from histdatacom.broker_plugin_health import qualification
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyRevocationV1,
    )
    from histdatacom.broker_plugin_policy.scope import BrokerPolicyError
    from tests.fixtures.broker_provider_policy import (
        generated_provider_scope,
        legacy_policy_inputs,
    )
    from tests.unit.test_broker_host_health_runtime_legacy import capture

    inputs = legacy_policy_inputs()
    request, captured = capture(
        tmp_path, messages=inputs.messages[:7] + inputs.messages[-1:]
    )
    with generated_provider_scope(request) as source:
        fingerprint = fit_broker_delivery_fingerprint(
            tmp_path, (captured.manifest,), provider_requests=(request,)
        )
        original = qualification._read
        context = source.current

        def revoke_after_read(*args):
            text = original(*args)
            source.current = replace(
                context,
                revocations=(
                    BrokerPolicyRevocationV1(
                        context.selected_policy_ids[0],
                        20,
                        20,
                        (context.evidence[0].artifact_id,),
                        "withdrawn",
                    ),
                ),
            )
            return text

        monkeypatch.setattr(qualification, "_read", revoke_after_read)
        boundary = (
            qualification.read_current_broker_health_qualification
            if operation == "read"
            else qualification.write_broker_health_qualification
        )
        with pytest.raises(BrokerPolicyError):
            boundary(tmp_path, fingerprint, (captured.manifest,), (request,))


@pytest.fixture(scope="module")
def sdk_health_install(tmp_path_factory):
    from tests.unit.test_broker_plugin_lifecycle import installed

    return installed.__wrapped__(tmp_path_factory)


@pytest.mark.parametrize("replayed_input", ["native_records", "observations"])
def test_sdk_health_reader_rechecks_rights_after_iterable_advancement(
    sdk_health_install, tmp_path, monkeypatch, replayed_input
):
    from histdatacom.broker_plugin_health import storage
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyRevocationV1,
    )
    from histdatacom.broker_plugin_policy.scope import BrokerPolicyError
    from tests.unit import test_broker_plugin_lifecycle as lifecycle

    inputs = lifecycle.request_data.__wrapped__(sdk_health_install, monkeypatch)
    captured = lifecycle.run(inputs, tmp_path)
    request = lifecycle._PROVIDER_REQUESTS[captured.directory]
    records = tuple(lifecycle.replay_broker_lifecycle(captured.directory))
    with lifecycle.runtime_scope(request) as source:
        context = source.current

        def revoke(values):
            for item in values:
                if not source.current.revocations:
                    source.current = replace(
                        context,
                        revocations=(
                            BrokerPolicyRevocationV1(
                                context.selected_policy_ids[0],
                                20,
                                20,
                                (context.evidence[0].artifact_id,),
                                "withdrawn",
                            ),
                        ),
                    )
                yield item

        supplied = records
        if replayed_input == "native_records":
            supplied = revoke(records)
        else:
            original = storage._observations
            monkeypatch.setattr(
                storage, "_observations", lambda *args: revoke(original(*args))
            )
        with pytest.raises(BrokerPolicyError):
            storage.read_lifecycle_host_health(
                captured.health_directory, captured.manifest, supplied, request
            )


@pytest.mark.parametrize("boundary", ["metadata", "observation"])
def test_health_journal_rechecks_retention_after_guard_callback(
    tmp_path, boundary
):
    from histdatacom.broker_plugin_health import (
        BrokerHostHealthObservationKind as Kind,
        BrokerHostHealthObservationV1 as Observation,
    )
    from histdatacom.broker_plugin_health.runtime_legacy import _request_json
    from histdatacom.broker_plugin_health.storage import (
        HostHealthEvidenceWriter,
    )
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyOperation,
        BrokerPolicyRevocationV1,
    )
    from histdatacom.broker_plugin_policy.health_bindings import (
        BrokerHostHealthEvidenceV1,
    )
    from histdatacom.broker_plugin_policy.scope import (
        BrokerPolicyError,
        require_provider_operation,
    )
    from tests.fixtures.broker_provider_policy import generated_provider_scope
    from tests.unit.test_broker_host_health_replay import legacy_fixture

    header, _, manifest, _, decision, request = legacy_fixture()
    observation = Observation(
        0,
        Kind.QUEUE,
        0,
        header.started_at_utc_ns,
        header.started_at_monotonic_ns,
        queue_items=0,
    )
    invocation = _request_json(request)
    trigger = invocation if boundary == "metadata" else observation.to_json()
    directory = tmp_path / "guard-revoked-health"
    with generated_provider_scope(request) as source:
        context = source.current

        def authorize(artifact):
            require_provider_operation(
                request, BrokerPolicyOperation.RETAIN_LOCAL
            )
            require_provider_operation(
                BrokerHostHealthEvidenceV1(
                    request, manifest.session, header, artifact
                ),
                BrokerPolicyOperation.RETAIN_LOCAL,
            )

        def guard(text):
            if text == trigger:
                source.current = replace(
                    context,
                    revocations=(
                        BrokerPolicyRevocationV1(
                            context.selected_policy_ids[0],
                            20,
                            20,
                            (context.evidence[0].artifact_id,),
                            "withdrawn",
                        ),
                    ),
                )

        writer = None
        with pytest.raises(BrokerPolicyError):
            try:
                writer = HostHealthEvidenceWriter(
                    directory,
                    header,
                    {
                        "provider-decision.json": decision.to_json(),
                        "invocation.json": invocation,
                    },
                    authorize_artifact=authorize,
                    guard_text=guard,
                )
                writer.append(observation)
            finally:
                if writer is not None:
                    writer.close()
        assert source.current.revocations
        assert not (directory / "audit.json").exists()
        if boundary == "metadata":
            assert not (directory / "invocation.json").exists()
        else:
            assert (directory / "observations.jsonl").read_bytes() == b""
