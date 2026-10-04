"""Genuine offline native sources for the clock-to-matching consumer."""

from dataclasses import replace
import shutil
from fractions import Fraction

import pytest

from histdatacom.broker_plugin_policy.contracts import BrokerPolicyRevocationV1
from histdatacom.broker_plugin_provenance import (
    read_lifecycle_capture_provenance,
    require_legacy_capture_provenance,
)
from tests.fixtures.cross_feed_native import (
    SECOND,
    SHIFT,
    build_legacy_capture,
    capture_scope,
    installed_sdk_capture,
    legacy_quotes,
)


@pytest.fixture(scope="module")
def legacy_capture(tmp_path_factory):
    return build_legacy_capture(tmp_path_factory.mktemp("cross-feed-legacy"))


@pytest.fixture(scope="module")
def sdk_capture(tmp_path_factory):
    with installed_sdk_capture(
        tmp_path_factory.mktemp("cross-feed-sdk")
    ) as capture:
        yield capture


def native_verify(capture, *, root=None):
    if capture.family == "legacy":
        return require_legacy_capture_provenance(
            capture.directory,
            capture.result.manifest,
            provider_request=capture.request,
            expected_root=capture.seal if root is None else root,
        )
    return read_lifecycle_capture_provenance(
        capture.directory,
        capture.result.manifest,
        provider_request=capture.request,
        expected_root=capture.seal if root is None else root,
    )


def test_genuine_legacy_capture_preserves_declared_fixture_clock(
    legacy_capture,
):
    with capture_scope(legacy_capture):
        proof = native_verify(legacy_capture)
    assert proof.complete and proof.anchored
    assert proof.seal == legacy_capture.seal
    assert (
        proof.seal.terminal.health_audit_id == legacy_capture.health.artifact_id
    )
    quotes = legacy_quotes(legacy_capture)
    assert [event.message.source_event_time_ns for event in quotes] == [
        SECOND + SHIFT,
        2 * SECOND + SHIFT,
        3 * SECOND + SHIFT,
    ]
    assert [event.message.bid_text for event in quotes] == [
        "1.1000",
        "1.1001",
        "1.1002",
    ]


def test_genuine_installed_sdk_capture_retains_unknown_upstream(sdk_capture):
    from histdatacom.broker_plugin_health import (
        BrokerHostHealthFailure,
        BrokerHostHealthState,
    )

    with capture_scope(sdk_capture):
        proof = native_verify(sdk_capture)
    assert proof.complete and proof.anchored
    assert proof.seal == sdk_capture.seal
    assert sdk_capture.health.state is BrokerHostHealthState.INSUFFICIENT
    assert sdk_capture.health.failures == (
        BrokerHostHealthFailure.UPSTREAM_UNKNOWN,
    )
    assert sdk_capture.result.manifest.worker_reaped
    assert proof.header.distribution_name == "histdatacom-permission-fixture"


@pytest.mark.parametrize("family", ["legacy", "sdk"])
def test_native_seal_alone_cannot_authorize_read_without_current_rights(
    family, legacy_capture, sdk_capture
):
    capture = legacy_capture if family == "legacy" else sdk_capture
    with pytest.raises(
        ValueError, match="current_process_policy_scope_required"
    ):
        native_verify(capture)


@pytest.mark.parametrize("family", ["legacy", "sdk"])
def test_real_provider_revocation_refuses_previously_valid_capture(
    family, legacy_capture, sdk_capture
):
    capture = legacy_capture if family == "legacy" else sdk_capture
    with capture_scope(capture) as source:
        assert native_verify(capture).anchored
        context = source.current
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
        with pytest.raises(ValueError):
            native_verify(capture)


@pytest.mark.parametrize("family", ["legacy", "sdk"])
def test_native_external_root_mismatch_is_not_relabelled(
    family, legacy_capture, sdk_capture
):
    capture = legacy_capture if family == "legacy" else sdk_capture
    foreign = replace(capture.seal, root_sha256="0" * 64)
    with capture_scope(capture), pytest.raises(ValueError):
        native_verify(capture, root=foreign)


@pytest.mark.parametrize(
    "mutation", ["missing_chain", "changed_chain", "missing_health"]
)
def test_actual_legacy_bytes_are_reread(legacy_capture, tmp_path, mutation):
    copied = tmp_path / "copy"
    shutil.copytree(legacy_capture.directory, copied)
    changed = replace(legacy_capture, directory=copied)
    provenance = copied / legacy_capture.result.provenance_directory.name
    if mutation == "missing_chain":
        (provenance / "chain.jsonl").unlink()
    elif mutation == "changed_chain":
        with (provenance / "chain.jsonl").open("ab") as stream:
            stream.write(b"{}\n")
    else:
        (
            copied / legacy_capture.result.health_directory.name / "audit.json"
        ).unlink()
    with capture_scope(changed), pytest.raises((ValueError, OSError)):
        native_verify(changed)


def matching_policy():
    from histdatacom.cross_feed._wire import RationalV1
    from histdatacom.cross_feed.matching_contracts import MatchingPolicyV1

    r = RationalV1.from_fraction
    return MatchingPolicyV1(
        r(1_000_000),
        r(SECOND),
        r(1),
        r(1),
        r(1),
        r(1),
        (r(1), r(1), r(1), r(1), r(0)),
        r(0),
        r(1_000_000),
    )


@pytest.fixture(scope="module")
def native_model(legacy_capture, sdk_capture):
    from histdatacom.cross_feed.api import fit_clock_model
    from histdatacom.cross_feed.clock_contracts import (
        ClockCandidatePairV1,
        ClockFitRequestV1,
    )
    from histdatacom.cross_feed.native import admit_capture

    with capture_scope(legacy_capture, sdk_capture):
        left = admit_capture(legacy_capture.reference())
        right = admit_capture(sdk_capture.reference())
        assert len(left.quotes) == len(right.quotes) == 3
        pairs = tuple(
            sorted(
                (
                    ClockCandidatePairV1(a.event_id, b.event_id)
                    for a, b in zip(left.quotes, right.quotes)
                ),
                key=lambda pair: (pair.left_event_id, pair.right_event_id),
            )
        )
        request = ClockFitRequestV1(
            pairs, left.quotes[-1].sequence, right.quotes[-1].sequence
        )
        model = fit_clock_model(
            legacy_capture.reference(), sdk_capture.reference(), request
        )
    return left, right, model


def test_public_native_clock_to_match_flow_preserves_known_source_correspondence(
    legacy_capture, sdk_capture, native_model
):
    from histdatacom.cross_feed.api import match_captures, replay_clock_model
    from histdatacom.cross_feed.evaluation import evaluate_matching
    from histdatacom.cross_feed.matching_contracts import (
        KnownCorrespondenceV1,
        MatchingTruthV1,
    )

    left, right, model = native_model
    assert model.status == "ready"
    assert len(model.segments) == 1
    assert model.segments[0].offset_ns.value == SHIFT
    assert model.segments[0].drift.value == 0
    with capture_scope(legacy_capture, sdk_capture):
        replayed = replay_clock_model(
            legacy_capture.reference(),
            sdk_capture.reference(),
            model,
            expected_model_id=model.artifact_id,
        )
        matched = match_captures(
            legacy_capture.reference(),
            sdk_capture.reference(),
            model,
            matching_policy(),
            expected_model_id=model.artifact_id,
        )
    assert replayed.to_json() == model.to_json()
    known_pairs = tuple(
        sorted(
            (a.event_id, b.event_id) for a, b in zip(left.quotes, right.quotes)
        )
    )
    assert tuple(pair.key for pair in matched.matches) == known_pairs
    assert not matched.source_only
    assert all(pair.status == "confident" for pair in matched.matches)
    assert matched.model_id == model.artifact_id
    truth = MatchingTruthV1(
        "independent-generated-sdk-legacy-fixed-shift",
        left.root_id,
        right.root_id,
        tuple(sorted(q.event_id for q in left.quotes)),
        tuple(sorted(q.event_id for q in right.quotes)),
        tuple(KnownCorrespondenceV1(*pair) for pair in known_pairs),
    )
    evaluation = evaluate_matching(
        left,
        right,
        model,
        matched,
        truth,
        expected_report_id=matched.artifact_id,
        expected_truth_id=truth.artifact_id,
    )
    assert evaluation.precision.value == evaluation.recall.value == 1
    assert evaluation.false_positive == evaluation.false_negative == 0
    assert evaluation.time_residuals_ns == tuple(
        type(model.segments[0].offset_ns)("0", "1") for _ in range(3)
    )


def test_sdk_projection_uses_actual_host_record_clock_not_plugin_claims(
    sdk_capture, native_model
):
    from histdatacom.broker_plugin_capabilities import BrokerAdmittedEventV1
    from histdatacom.broker_plugin_lifecycle import replay_broker_lifecycle
    from histdatacom.broker_plugins import BrokerEventKind

    _left, right, _model = native_model
    with capture_scope(sdk_capture):
        records = tuple(
            replay_broker_lifecycle(
                sdk_capture.directory, provider_request=sdk_capture.request
            )
        )
    actual = {}
    for record in records:
        if record.kind == "event":
            event = BrokerAdmittedEventV1.from_json(record.payload_json).event
            if event.kind is BrokerEventKind.QUOTE:
                actual[event.artifact_id] = record, event
    for quote in right.quotes:
        record, event = actual[quote.event_id]
        assert quote.record_id == record.artifact_id
        assert (
            quote.receive_utc_ns,
            quote.receive_monotonic_ns,
            quote.sequence,
        ) == (
            record.receive_utc_ns,
            record.receive_monotonic_ns,
            record.capture_sequence,
        )
        assert quote.receive_utc_ns != event.receive_time.utc_ns
        assert quote.source_time_ns == event.source_time.timestamp_ns
        assert quote.bid.value == Fraction(event.quote.bid)
        assert quote.health_reasons == tuple(
            sorted(f.value for f in sdk_capture.health.failures)
        )


def test_newly_denied_fingerprint_derivation_blocks_existing_valid_model_only(
    legacy_capture, sdk_capture, native_model
):
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyOperation,
        require_provider_operation,
    )
    from histdatacom.broker_plugin_permissions import broker_permission_scope
    from histdatacom.broker_plugin_policy.contracts import (
        BrokerPolicyDataClass,
        BrokerPolicyStatus,
    )
    from histdatacom.broker_plugin_policy.scope import (
        BrokerPolicyError,
        provider_policy_scope,
    )
    from histdatacom.cross_feed.api import match_captures

    _left, _right, model = native_model
    with capture_scope(legacy_capture, sdk_capture) as source:
        replacements = {
            old.artifact_id: replace(
                old,
                rules=tuple(
                    (
                        replace(rule, status=BrokerPolicyStatus.DENIED)
                        if (
                            rule.operation is BrokerPolicyOperation.DERIVE
                            and rule.data_class
                            is BrokerPolicyDataClass.FINGERPRINTS
                        )
                        else rule
                    )
                    for rule in old.rules
                ),
            )
            for old in source.current.policies
        }
        new_policies = tuple(
            sorted(replacements.values(), key=lambda p: p.artifact_id)
        )
        acknowledgements = tuple(
            sorted(
                (
                    replace(
                        ack, policy_id=replacements[ack.policy_id].artifact_id
                    )
                    for ack in source.current.acknowledgements
                ),
                key=lambda ack: ack.artifact_id,
            )
        )
        denied_context = replace(
            source.current,
            policies=new_policies,
            selected_policy_ids=tuple(p.artifact_id for p in new_policies),
            acknowledgements=acknowledgements,
        )
    # Selected policy IDs are immutable within a scope. A subsequent operation
    # gets a new scope with the changed policy, retaining the actual SDK grant.
    source.current = denied_context
    with (
        provider_policy_scope(source),
        broker_permission_scope(
            sdk_capture.authority, resources=sdk_capture.resources
        ),
    ):
        for capture in (legacy_capture, sdk_capture):
            assert require_provider_operation(
                capture.request, BrokerPolicyOperation.MATERIAL_USE
            ).allowed
        with pytest.raises(
            BrokerPolicyError, match="operation_not_allowed"
        ) as refused:
            match_captures(
                legacy_capture.reference(),
                sdk_capture.reference(),
                model,
                matching_policy(),
                expected_model_id=model.artifact_id,
            )
        assert refused.value.decision is not None
        assert not refused.value.decision.allowed
        assert (
            refused.value.decision.request.operation
            is BrokerPolicyOperation.DERIVE
        )
        denied_cells = tuple(
            cell
            for cell in refused.value.decision.cells
            if cell.status is not BrokerPolicyStatus.ALLOWED
        )
        assert denied_cells
        assert all(
            cell.data_class is BrokerPolicyDataClass.FINGERPRINTS
            and cell.status is BrokerPolicyStatus.DENIED
            for cell in denied_cells
        )


def test_resealed_mathematical_clock_change_cannot_pass_actual_refit(
    legacy_capture, sdk_capture, native_model
):
    from histdatacom.cross_feed._wire import RationalV1
    from histdatacom.cross_feed.api import match_captures

    _left, _right, model = native_model
    altered = replace(
        model,
        segments=(
            replace(
                model.segments[0],
                offset_ns=RationalV1.from_fraction(
                    model.segments[0].offset_ns.value + 1
                ),
            ),
        ),
    )
    assert altered.artifact_id != model.artifact_id
    with (
        capture_scope(legacy_capture, sdk_capture),
        pytest.raises(ValueError, match="does not replay"),
    ):
        match_captures(
            legacy_capture.reference(),
            sdk_capture.reference(),
            altered,
            matching_policy(),
            expected_model_id=altered.artifact_id,
        )


@pytest.mark.parametrize(
    "change", ["missing_root", "health_id", "foreign_manifest"]
)
def test_native_intake_requires_exact_external_root_and_health(
    change, legacy_capture, sdk_capture
):
    from histdatacom.cross_feed.native import admit_capture

    reference = legacy_capture.reference()
    if change == "missing_root":
        reference = replace(reference, expected_root=None)
    elif change == "health_id":
        reference = replace(
            reference, expected_health_id=sdk_capture.health.artifact_id
        )
    else:
        reference = replace(reference, manifest=sdk_capture.result.manifest)
    with capture_scope(legacy_capture), pytest.raises(ValueError):
        admit_capture(reference)


@pytest.mark.parametrize("scenario", ["affine_drift", "duplicate_burst"])
def test_real_legacy_pair_drives_public_clock_and_match(scenario, tmp_path):
    from histdatacom.cross_feed.api import fit_clock_model, match_captures
    from histdatacom.cross_feed.clock_contracts import (
        ClockCandidatePairV1,
        ClockFitRequestV1,
    )
    from histdatacom.cross_feed.native import admit_capture

    if scenario == "affine_drift":
        times = tuple(SECOND * n for n in range(1, 6))
        altered_times = tuple(t + SHIFT + (t - SECOND) // 1000 for t in times)
        prices = tuple((f"1.100{n}", f"1.200{n}") for n in range(5))
    else:
        times = (SECOND, SECOND, 2 * SECOND, 3 * SECOND)
        altered_times = tuple(t + SHIFT for t in times)
        prices = (
            ("1.1000", "1.2000"),
            ("1.1000", "1.2000"),
            ("1.1001", "1.2001"),
            ("1.1002", "1.2002"),
        )
    left_capture = build_legacy_capture(
        tmp_path / "left",
        source_times=altered_times,
        prices=prices,
        label=f"{scenario}-left",
        precision_ns=1000,
    )
    right_capture = build_legacy_capture(
        tmp_path / "right",
        source_times=times,
        prices=prices,
        label=f"{scenario}-right",
        precision_ns=1000,
    )
    with capture_scope(left_capture, right_capture):
        left = admit_capture(left_capture.reference())
        right = admit_capture(right_capture.reference())
        request = ClockFitRequestV1(
            tuple(
                sorted(
                    (
                        ClockCandidatePairV1(a.event_id, b.event_id)
                        for a, b in zip(left.quotes, right.quotes)
                    ),
                    key=lambda p: (p.left_event_id, p.right_event_id),
                )
            ),
            left.quotes[-1].sequence,
            right.quotes[-1].sequence,
        )
        model = fit_clock_model(
            left_capture.reference(), right_capture.reference(), request
        )
        report = match_captures(
            left_capture.reference(),
            right_capture.reference(),
            model,
            matching_policy(),
            expected_model_id=model.artifact_id,
        )
    assert model.status == "ready"
    assert len(report.matches) == len(times)
    assert not report.source_only
    if scenario == "affine_drift":
        assert model.segments[0].drift.value == Fraction(1, 1000)
        assert all(pair.status == "confident" for pair in report.matches)
    else:
        assert model.segments[0].drift.value == 0
        assert sum(pair.status == "ambiguous" for pair in report.matches) == 2
        assert len({pair.left_event_id for pair in report.matches}) == 4
        assert len({pair.right_event_id for pair in report.matches}) == 4
    assert all(edge.time_residual_ns.value == 0 for edge in report.candidates)


def test_actual_late_host_clock_fault_cannot_rewrite_earlier_persisted_health(
    tmp_path,
):
    from histdatacom.broker_plugin_health.storage import _observations
    from histdatacom.cross_feed.clock import fit_clock
    from histdatacom.cross_feed.clock_contracts import (
        ClockCandidatePairV1,
        ClockFitRequestV1,
    )
    from histdatacom.cross_feed.native import admit_capture
    from tests.fixtures.broker_host_health import SyntheticHostHealthClock
    from tests.fixtures.broker_provider_policy import legacy_policy_inputs

    class LateClock(SyntheticHostHealthClock):
        calls = 0

        def sample(self):
            self.calls += 1
            # One initial queue observation plus six ingress/persistence pairs
            # precede PROCESS_STOP. This is a predeclared clock fault, not a
            # search for a placement which yields a passing health decision.
            if self.calls == 14:
                self.wall += 2 * SECOND
            return super().sample()

    baseline = build_legacy_capture(
        tmp_path / "baseline", label="prefix-causality", fsync_each_event=True
    )
    late = build_legacy_capture(
        tmp_path / "late",
        label="prefix-causality",
        fsync_each_event=True,
        clock=LateClock(legacy_policy_inputs().session),
    )
    assert baseline.health.artifact_id != late.health.artifact_id
    assert "clock_discontinuity" in {
        failure.value for failure in late.health.failures
    }
    with capture_scope(baseline, late):
        first = admit_capture(baseline.reference())
        second = admit_capture(late.reference())
    assert len(first.quotes) == len(second.quotes) == 3
    assert [q.health_reasons for q in first.quotes] == [
        q.health_reasons for q in second.quotes
    ]
    assert all(
        "clock_discontinuity" not in q.health_reasons for q in second.quotes
    )
    assert [
        (q.source_time_ns, q.receive_utc_ns, q.receive_monotonic_ns)
        for q in first.quotes
    ] == [
        (q.source_time_ns, q.receive_utc_ns, q.receive_monotonic_ns)
        for q in second.quotes
    ]
    observations = tuple(_observations(late.result.health_directory, 1000))
    positions = {
        observation.artifact_id: index
        for index, observation in enumerate(observations)
    }
    faults = [
        index
        for index, (before, after) in enumerate(
            zip(observations, observations[1:]), start=1
        )
        if (
            after.utc_ns
            - after.monotonic_ns
            - before.utc_ns
            + before.monotonic_ns
            > SECOND
        )
    ]
    assert faults
    assert max(
        positions[q.prefix_health_observation_id] for q in second.quotes
    ) < min(faults)
    request = ClockFitRequestV1(
        tuple(
            sorted(
                (
                    ClockCandidatePairV1(a.event_id, b.event_id)
                    for a, b in zip(first.quotes, second.quotes)
                ),
                key=lambda pair: (pair.left_event_id, pair.right_event_id),
            )
        ),
        first.quotes[-1].sequence,
        second.quotes[-1].sequence,
    )
    model = fit_clock(first, second, request)
    assert model.status == "ready"
    assert (
        model.segments[0].offset_ns.value == model.segments[0].drift.value == 0
    )


def test_prefix_pass_rejects_structurally_valid_journal_changed_after_real_audit(
    legacy_capture, tmp_path
):
    from histdatacom.broker_plugin_health import BrokerHostHealthObservationV1
    from histdatacom.broker_plugin_health.runtime_legacy import (
        read_legacy_host_health,
    )
    from histdatacom.cross_feed.native import _prefix_health

    copied = tmp_path / "copy"
    shutil.copytree(legacy_capture.directory, copied)
    changed = replace(legacy_capture, directory=copied)
    health_directory = copied / legacy_capture.result.health_directory.name
    with capture_scope(changed):
        audit = read_legacy_host_health(
            copied,
            changed.result.manifest,
            provider_request=changed.request,
        )
    assert _prefix_health(health_directory, audit)
    path = health_directory / "observations.jsonl"
    rows = path.read_text().splitlines()
    final = BrokerHostHealthObservationV1.from_json(rows[-1])
    altered = replace(final, utc_ns=final.utc_ns + 1)
    assert altered.artifact_id != final.artifact_id
    rows[-1] = altered.to_json()
    path.write_text("\n".join(rows) + "\n")
    # The final row is a valid freshly identified DTO, not malformed JSON.
    assert BrokerHostHealthObservationV1.from_json(rows[-1]) == altered
    with pytest.raises(
        ValueError, match="prefix observations differ from verified audit"
    ):
        _prefix_health(health_directory, audit)
