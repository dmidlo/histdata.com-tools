"""Real generated SDK parents, exact proposal lineage, and refusal boundaries."""

from dataclasses import replace

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerDerivedArtifactV1,
    BrokerPolicyError,
    BrokerPolicyOperation,
    BrokerPolicyStatus,
    provider_native_inputs,
    provider_policy_scope,
    read_broker_scientific_lineage,
    require_provider_operation,
    resolve_provider_subject,
    verify_broker_policy_receipt,
    write_broker_policy_receipt,
)
from histdatacom.broker_plugin_policy.scientific_storage import (
    BrokerScientificLineageV1,
    BrokerScientificLineageV2,
)
from histdatacom.data_quality.training_contracts import training_json
from histdatacom.data_quality.training_join_contracts import (
    JoinFamily,
    TrainingJoinBatchV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
)
from histdatacom.data_quality.training_provider_policy import (
    training_provider_subject,
    verify_training_policy_receipt,
)
from histdatacom.synthetic import (
    BrokerTransferConfigV1,
    BrokerTransferStatus,
    InformationMode,
    ReferenceMotifConditionV1,
    ReferenceMotifQueryV1,
    condition_broker_proposal,
    render_broker_delivery,
)
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    generated_provider_scope,
    policy_context,
)
from tests.fixtures.broker_sdk_science import qualified_sdk_capture
from tests.fixtures.training_join_v1 import join_fixture
from tests.unit.test_synthetic_broker_transfer import _group_with_constraints


@pytest.fixture(scope="module")
def native_capture(tmp_path_factory):
    return qualified_sdk_capture(tmp_path_factory.mktemp("sdk-consumer"))


def _proposal(fingerprint):
    query = ReferenceMotifQueryV1(
        ReferenceMotifConditionV1(
            "EURUSD",
            "generated-only",
            "generated",
            metrics={"spread": 0.002, "price_precision_digits": 8.0},
        ),
        InformationMode.EX_POST_RECONSTRUCTION,
        fingerprint.support_end_utc_ns,
    )
    result = condition_broker_proposal(
        query,
        fingerprint,
        requested_condition={"symbol": "EURUSD"},
        selected_at_utc_ns=fingerprint.support_end_utc_ns,
        config=BrokerTransferConfigV1(strength=0.5),
    )
    assert result.selection.applied
    assert result.conditioned_query.query_id != query.query_id
    assert result.metrics_after["spread"] == pytest.approx(0.0011)
    assert result.metrics_after["price_precision_digits"] == 6.5
    assert "timestamp_precision_ns" not in result.metrics_after
    return result


def test_native_proposal_persists_exact_v2_lineage_and_current_rights(
    tmp_path, native_capture
):
    result, request, fingerprint = native_capture
    path = tmp_path / "proposal.json"
    with (
        generated_provider_scope(request, capture_roots=(result.directory,)),
        provider_native_inputs(fingerprint),
    ):
        proposal = _proposal(fingerprint)
        subject = BrokerDerivedArtifactV1((fingerprint,), proposal)
        data = proposal.to_json().encode("ascii")
        receipt = write_broker_policy_receipt(
            path.with_name(path.name + ".provider-policy.json"),
            subject,
            native_artifact_name=path.name,
            native_file_bytes=data,
        )
        path.write_bytes(data)
        lineage = read_broker_scientific_lineage(path)
        assert type(lineage) is BrokerScientificLineageV2
        assert lineage.fingerprints == (fingerprint,)
        assert (
            lineage.fingerprints[0].capture_roots == fingerprint.capture_roots
        )
        assert BrokerScientificLineageV2.from_json(lineage.to_json()) == lineage
        with pytest.raises(ValueError):
            BrokerScientificLineageV1.from_json(lineage.to_json())
        verify_broker_policy_receipt(receipt, subject, path)

    (binding,) = resolve_provider_subject(fingerprint).bindings
    denied = MutablePolicySource(
        policy_context(binding, status=BrokerPolicyStatus.DENIED)
    )
    with provider_policy_scope(denied), pytest.raises(BrokerPolicyError):
        require_provider_operation(subject, BrokerPolicyOperation.MATERIAL_USE)
    path.write_bytes(data + b" ")
    with pytest.raises(ValueError, match="differs from native file"):
        read_broker_scientific_lineage(path)


def test_native_training_parent_requires_receipt_without_force_flag(
    tmp_path, native_capture
):
    _, _, fingerprint = native_capture
    generic = join_fixture(tmp_path / "generic")
    binding = TrainingJoinSourceV1(
        "broker",
        JoinFamily.BROKER,
        paths=(
            str(tmp_path / "not-read"),
            str(tmp_path / "not-read/manifest.json"),
        ),
        evidence_json=(training_json(fingerprint.to_dict()),),
    )
    # This checks retained metadata and mandatory receipt admission only.
    # It is deliberately not a claim of a qualified physical training join.
    batch = TrainingJoinBatchV1(
        replace(generic, sources=(binding,), columns=()),
        tuple(
            replace(row, values=())
            for row in materialize_training_joins(generic).rows
        ),
    )
    subject = training_provider_subject(batch)
    assert subject is not None
    assert subject.fingerprints == (fingerprint,)
    with pytest.raises(
        ValueError, match="requires its provider-policy receipt"
    ):
        verify_training_policy_receipt(subject, tmp_path / "training.json")
    assert not (tmp_path / "not-read").exists()


def test_native_render_refuses_unavailable_source_batch_semantics(
    native_capture,
):
    result, request, fingerprint = native_capture
    run, window, group, constraints = _group_with_constraints()
    with (
        generated_provider_scope(request, capture_roots=(result.directory,)),
        provider_native_inputs(fingerprint),
    ):
        rendered = render_broker_delivery(
            run=run,
            window=window,
            group=group,
            fingerprint=fingerprint,
            constraints=constraints,
            selected_at_utc_ns=fingerprint.support_end_utc_ns,
            config=BrokerTransferConfigV1(strength=0.5),
        )
    assert rendered.status is BrokerTransferStatus.REFUSED
    assert not rendered.streams
    assert (
        "native_render_semantics_unavailable:source_batch_quote_count"
        in rendered.manifest.reason_codes
    )
