"""Pure snapshot substitution checks; no process or native capture is run."""

import hashlib
from dataclasses import replace
from types import SimpleNamespace

import pytest

from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1
from histdatacom.broker_plugin_conformance import verification
from histdatacom.broker_plugin_conformance.contracts import (
    BrokerConformanceCaseResultV1,
    BrokerConformanceReason,
    BrokerConformanceStatus,
    BrokerConformanceSubject,
)
from histdatacom.broker_plugin_lifecycle import (
    BrokerLifecycleCompletion,
    BrokerLifecycleManifestV1,
    BrokerLifecyclePartitionV1,
    BrokerLifecycleRecordV1,
    BrokerLifecycleSessionV1,
    BrokerLifecycleState,
)
from histdatacom.broker_plugin_permissions import BrokerPermissionError
from histdatacom.broker_plugin_policy import BrokerPolicyError
from histdatacom.broker_plugins import BrokerSessionV1
from tests.unit.test_broker_plugin_conformance_terminals import (
    _verifier_case,
    generated_native_header as generated_native_header,
    verifier_fixture as verifier_fixture,
)


def _receipt(ordinal, records):
    # Independent writer-byte reconstruction, never the production binder.
    payload = b"".join(
        (record.to_json() + "\n").encode("ascii") for record in records
    )
    return BrokerLifecyclePartitionV1(
        ordinal,
        hashlib.sha256(payload).hexdigest(),
        len(payload),
        records[0].capture_sequence,
        len(records),
        sum(record.kind == "event" for record in records),
    )


@pytest.fixture
def binding_input(generated_native_header):
    header = generated_native_header
    session = BrokerSessionV1(
        "broker-plugin-metadata:sha256:" + "1" * 64,
        "a" * 32,
        100,
        "generated-clock",
    )
    records = tuple(
        BrokerLifecycleRecordV1(
            header.artifact_id,
            index,
            0,
            100 + index,
            200 + index,
            "session",
            BrokerLifecycleSessionV1(
                replace(session, instance_nonce=f"{index + 1:032x}"), ()
            ).to_json(),
        )
        for index in range(3)
    )
    manifest = BrokerLifecycleManifestV1(
        header,
        BrokerLifecycleState.STOPPED,
        BrokerLifecycleCompletion.COMPLETE,
        (_receipt(0, records[:2]), _receipt(1, records[2:])),
        appended_records=3,
        worker_reaped=True,
    )
    return manifest, records


@pytest.mark.parametrize("partial", (False, True))
def test_exact_multipart_and_partial_inventory_is_bound(binding_input, partial):
    manifest, records = binding_input
    if partial:
        manifest = replace(
            manifest,
            completion=BrokerLifecycleCompletion.PARTIAL,
            partitions=manifest.partitions[:1],
            partial_partition=manifest.partitions[1],
        )
    checked = verification._bind_native_records(manifest, iter(records))
    assert checked == records
    assert all(left is not right for left, right in zip(checked, records))


@pytest.mark.parametrize(
    "change",
    ("nonce", "nonce-and-time", "run", "sequence", "reorder", "delete", "type"),
)
def test_substituted_records_cannot_borrow_expected_receipts(
    binding_input, change
):
    manifest, original = binding_input
    records = list(original)
    if change.startswith("nonce"):
        opened = BrokerLifecycleSessionV1.from_json(records[0].payload_json)
        session = replace(
            opened.session,
            instance_nonce="f" * 32,
            opened_at_utc_ns=101 if change == "nonce-and-time" else 100,
        )
        records[0] = replace(
            records[0], payload_json=replace(opened, session=session).to_json()
        )
    elif change == "run":
        records[0] = replace(
            records[0], run_id="broker-lifecycle-header:sha256:" + "0" * 64
        )
    elif change == "sequence":
        records[0] = replace(records[0], capture_sequence=1)
    elif change == "reorder":
        records[0], records[1] = records[1], records[0]
    elif change == "delete":
        records.pop()
    else:
        records[0] = object()
    with pytest.raises(ValueError, match="native observation"):
        verification._bind_native_records(manifest, iter(records))


def test_extra_iterator_row_stops_at_expected_inventory_plus_one(binding_input):
    manifest, records = binding_input
    consumed = []

    def source():
        for record in records:
            consumed.append(record)
            yield record
        consumed.append("extra")
        yield records[0]
        pytest.fail("binder drained an unbounded substituted iterator")

    with pytest.raises(ValueError, match="exceed expected record inventory"):
        verification._bind_native_records(manifest, source())
    assert len(consumed) == manifest.appended_records + 1


def test_generator_cannot_mutate_previously_bound_record(binding_input):
    manifest, records = binding_input
    original = records[0].to_json()

    def source():
        yield records[0]
        object.__setattr__(records[0], "receive_utc_ns", 999)
        yield from records[1:]

    checked = verification._bind_native_records(manifest, source())
    assert checked[0].to_json() == original
    assert checked[0].receive_utc_ns == 100
    assert records[0].receive_utc_ns == 999


@pytest.mark.parametrize("change", ("hash", "bytes", "events", "total-events"))
def test_expected_receipt_and_event_totals_are_enforced(binding_input, change):
    manifest, records = binding_input
    first = manifest.partitions[0]
    if change == "hash":
        first = replace(first, sha256="0" * 64)
    elif change == "bytes":
        first = replace(first, byte_count=first.byte_count + 1)
    elif change == "events":
        first = replace(first, event_count=1)
    else:
        manifest = replace(manifest, appended_events=1, received_events=1)
    manifest = replace(manifest, partitions=(first, manifest.partitions[1]))
    with pytest.raises(ValueError, match="native observation"):
        verification._bind_native_records(manifest, records)


@pytest.mark.parametrize("change", ("open", "type", "count", "policy"))
def test_invalid_expected_envelope_refuses_without_consumption(
    binding_input, change
):
    manifest, records = binding_input
    if change == "open":
        manifest = replace(
            manifest,
            completion=BrokerLifecycleCompletion.OPEN,
            state=BrokerLifecycleState.ACTIVE,
        )
    elif change == "type":
        manifest = object()
    elif change == "count":
        manifest = replace(
            manifest,
            completion=BrokerLifecycleCompletion.PARTIAL,
            appended_records=4,
        )
    else:
        manifest = replace(
            manifest,
            header=replace(
                manifest.header,
                policy=replace(manifest.header.policy, max_partitions=1),
            ),
            completion=BrokerLifecycleCompletion.PARTIAL,
            partitions=manifest.partitions[:1],
            partial_partition=manifest.partitions[1],
        )

    def source():
        pytest.fail("invalid expected envelope consumed source")
        yield from records

    with pytest.raises(ValueError):
        verification._bind_native_records(manifest, source())


@pytest.mark.parametrize(
    "limit", ("records", "partition-bytes", "capture-bytes")
)
def test_original_policy_limits_bound_iterator_consumption(
    binding_input, limit
):
    manifest, records = binding_input
    policy = manifest.header.policy
    first_bytes = manifest.partitions[0].byte_count
    assert first_bytes > 1024
    if limit == "records":
        policy = replace(policy, partition_events=1)
    elif limit == "partition-bytes":
        policy = replace(policy, partition_bytes=first_bytes - 1)
    else:
        policy = replace(
            policy,
            partition_bytes=first_bytes,
            max_capture_bytes=first_bytes,
        )
    manifest = replace(manifest, header=replace(manifest.header, policy=policy))

    def source():
        pytest.fail("invalid original policy consumed substituted records")
        yield from records

    with pytest.raises(ValueError, match="native observations"):
        verification._bind_native_records(manifest, source())


def test_partial_tail_must_match_its_original_canonical_receipt(binding_input):
    manifest, records = binding_input
    manifest = replace(
        manifest,
        completion=BrokerLifecycleCompletion.PARTIAL,
        partitions=manifest.partitions[:1],
        partial_partition=manifest.partitions[1],
    )
    changed = records[:-1] + (replace(records[-1], receive_utc_ns=999),)
    with pytest.raises(ValueError, match="native observations differ"):
        verification._bind_native_records(manifest, changed)


def test_assessment_rejects_aba_records_before_pinned_original_replay(
    verifier_fixture, tmp_path, monkeypatch
):
    plan, evidence, records, _, _, _ = _verifier_case(
        verifier_fixture, tmp_path, monkeypatch, "lifecycle.reconnect", "none"
    )
    changed = list(records)
    index = next(
        i for i, record in enumerate(records) if record.kind == "session"
    )
    payload = BrokerLifecycleSessionV1.from_json(records[index].payload_json)
    substituted_session = replace(payload.session, instance_nonce="f" * 32)
    changed[index] = replace(
        records[index],
        payload_json=replace(payload, session=substituted_session).to_json(),
    )
    # This remains a pure reader seam, not a physically replayed lifecycle.
    # Still bind B's associated event payloads to its substituted opening so
    # the asserted refusal is the original receipt, not a dangling session ID.
    for index, record in enumerate(changed):
        if record.kind == "event":
            admitted = BrokerAdmittedEventV1.from_json(record.payload_json)
            if admitted.event.session_id == payload.session.artifact_id:
                changed[index] = replace(
                    record,
                    payload_json=replace(
                        admitted,
                        event=replace(
                            admitted.event,
                            session_id=substituted_session.artifact_id,
                        ),
                    ).to_json(),
                )
    calls = []

    def swapped_reader(*args, **kwargs):
        calls.append("B")
        # The directory may already be back at pinned A before this tuple is
        # assessed. Pre/post manifest equality could not expose this snapshot.
        return iter(changed)

    def pinned_original(*args, **kwargs):
        pytest.fail("unbound B records reached the otherwise pinned A reader")

    monkeypatch.setattr(verification, "replay_broker_lifecycle", swapped_reader)
    monkeypatch.setattr(
        verification, "read_lifecycle_capture_provenance", pinned_original
    )
    with pytest.raises(ValueError, match="native observations differ"):
        verification.verify_conformance_evidence(plan, evidence, tmp_path)
    assert calls == ["B"]


@pytest.mark.parametrize(
    "gate", ("permission", "provider-policy", "provenance")
)
def test_binding_preserves_existing_refusal_propagation(
    verifier_fixture, tmp_path, monkeypatch, gate
):
    plan, evidence, *_ = _verifier_case(
        verifier_fixture, tmp_path, monkeypatch, "quotes.symbols", "none"
    )
    target, error = {
        "permission": (
            "verify_permission_execution",
            BrokerPermissionError("generated permission refusal"),
        ),
        "provider-policy": (
            "replay_broker_lifecycle",
            BrokerPolicyError("generated current-policy refusal"),
        ),
        "provenance": (
            "read_lifecycle_capture_provenance",
            ValueError("generated pinned-provenance refusal"),
        ),
    }[gate]
    calls = []

    def refuse(*args, **kwargs):
        calls.append(target)
        raise error

    monkeypatch.setattr(verification, target, refuse)
    with pytest.raises(type(error)) as caught:
        verification.verify_conformance_evidence(plan, evidence, tmp_path)
    assert caught.value is error
    assert calls == [target]


@pytest.mark.parametrize(
    "case", ("replay.determinism", "session.freshness", "execution.equivalence")
)
@pytest.mark.parametrize("reused", (False, True))
def test_pair_uses_only_the_observations_assessed_once(
    tmp_path, monkeypatch, case, reused
):
    # Explicit assessment seam stub, not a claim of native replay. All reads
    # after assessment are forbidden, including the historical third replay.
    first = SimpleNamespace(
        case_id=case,
        native_family="isolated",
        stopped_at_ns=10,
        artifact_id="broker-conformance-evidence:sha256:" + "1" * 64,
    )
    second = SimpleNamespace(
        case_id=case,
        native_family=(
            "trusted" if case == "execution.equivalence" else "isolated"
        ),
        started_at_ns=11,
        artifact_id="broker-conformance-evidence:sha256:" + "2" * 64,
    )
    subject = BrokerConformanceSubject.CANDIDATE
    plan = SimpleNamespace(subject=subject)
    session = BrokerSessionV1(
        "broker-plugin-metadata:sha256:" + "1" * 64,
        "a" * 32,
        100,
        "generated-clock",
    )
    observed = []
    event = SimpleNamespace(
        kind="quote",
        instrument="EURUSD",
        quote="generated",
        source_time=None,
        gap=None,
        diagnostic=None,
        raw_provenance=None,
    )

    def assess(actual_plan, evidence, directory, *, _paired_second=False):
        assert actual_plan is plan
        is_second = evidence is second
        assert directory == (
            tmp_path
            / ("trusted" if case == "execution.equivalence" else "repeat")
            if is_second
            else tmp_path
        )
        assert _paired_second is (is_second and case == "execution.equivalence")
        observed.append(evidence)
        opening = replace(
            session,
            instance_nonce="b" * 32 if is_second and not reused else "a" * 32,
            opened_at_utc_ns=200 if is_second else 100,
        )
        return verification._EvidenceAssessment(
            BrokerConformanceCaseResultV1(
                case,
                BrokerConformanceStatus.PASS,
                BrokerConformanceReason.VERIFIED,
                subject,
                (evidence.artifact_id,),
                evidence.native_family,
            ),
            (opening,),
            (event,),
        )

    def forbidden(*args, **kwargs):
        pytest.fail("pairing performed an unpinned observation reread")

    monkeypatch.setattr(verification, "_assess_conformance_evidence", assess)
    monkeypatch.setattr(verification, "replay_broker_lifecycle", forbidden)
    monkeypatch.setattr(
        verification, "read_lifecycle_capture_provenance", forbidden
    )
    result = verification.verify_conformance_equivalence(
        plan, first, second, tmp_path
    )
    assert observed == [first, second]
    assert result.status is (
        BrokerConformanceStatus.FAIL if reused else BrokerConformanceStatus.PASS
    )
    assert result.reason is (
        BrokerConformanceReason.CONTRACT
        if reused
        else BrokerConformanceReason.VERIFIED
    )
