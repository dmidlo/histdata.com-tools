from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.generation import EMPIRICAL_MOTIF_GENERATOR_ID
from histdatacom.synthetic.proposal_engines import (
    ProposalEligibility,
    ProposalEngineBindingV1,
    ProposalEngineEvidenceV1,
    ProposalEnginePortfolioV1,
    ProposalPortfolioEvaluationV1,
    audit_proposal_engine_binding,
    build_histdata_proposal_portfolio,
    proposal_engine_default_configs,
    proposal_engine_registry,
)
from histdatacom.synthetic.qualification import QualificationStatus
from histdatacom.synthetic.schrodinger_bridge import SB_GENERATOR_ID


def _ref(kind: str, label: str) -> ArtifactRef:
    digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
    return ArtifactRef(
        kind=kind,
        path=f"/tmp/{label}-{digest}.json",
        size_bytes=128,
        sha256=digest,
        metadata={"label": label},
    )


def _bindings() -> tuple[ProposalEngineBindingV1, ...]:
    registry = proposal_engine_registry()
    configs = proposal_engine_default_configs()
    dataset = _ref("dataset_resolution_v1", "dataset")
    context = _ref("feed_epoch_definition_v2", "context")
    scorecard = _ref("reverse_degradation_scorecard_v1", "scorecard")
    return tuple(
        ProposalEngineBindingV1(
            engine_id=item.engine_id,
            descriptor_id=item.descriptor_id,
            config_id=configs[item.engine_id].config_id,
            config_ref=_ref("proposal_engine_config_v1", item.variant),
            dataset_ref=dataset,
            context_refs=(context,),
            evidence_refs=(scorecard,),
        )
        for item in registry.descriptors
    )


def test_registry_discovers_every_concrete_engine_deterministically() -> None:
    first = proposal_engine_registry()
    second = proposal_engine_registry()

    assert first == second
    assert len(first.descriptors) == 13
    assert len(proposal_engine_default_configs()) == 13
    assert (
        first.descriptor(EMPIRICAL_MOTIF_GENERATOR_ID).fit_schema_versions == ()
    )
    assert first.descriptor(SB_GENERATOR_ID).requires_broker_target is True
    assert sum(item.requires_broker_target for item in first.descriptors) == 1
    assert type(first).from_dict(first.to_dict()) == first


def test_eligibility_keeps_failed_and_broker_engines_out_of_product() -> None:
    registry = proposal_engine_registry()
    bindings = {item.engine_id: item for item in _bindings()}
    classical = registry.descriptor("histdatacom.event-clock.nhpp")
    failed = ProposalEngineEvidenceV1(
        engine_id=classical.engine_id,
        campaign_id="campaign:sha256:" + "1" * 64,
        corpus_id="corpus:sha256:" + "0" * 64,
        report_id="report:sha256:" + "2" * 64,
        candidate_id="candidate:sha256:" + "3" * 64,
        method_name="non_homogeneous_poisson",
        promotion_eligible=False,
        provisional=False,
        failure_count=0,
        refusal_count=0,
        failed_gate_ids=("candidate-event-count",),
        config_ids=(bindings[classical.engine_id].config_id,),
        fit_ids=("fit:sha256:" + "4" * 64,),
        checkpoint_ids=(),
        training_dataset_ids=(),
    )

    classical_audit = audit_proposal_engine_binding(
        classical,
        bindings[classical.engine_id],
        evidence=(failed,),
    )
    bridge_audit = audit_proposal_engine_binding(
        registry.descriptor(SB_GENERATOR_ID),
        bindings[SB_GENERATOR_ID],
        evidence=(),
    )

    assert classical_audit.eligibility is ProposalEligibility.BENCHMARK_ELIGIBLE
    assert classical_audit.reconstruction_eligible is False
    assert "retained_campaign_failed_promotion_gates" in (
        classical_audit.reason_codes
    )
    assert bridge_audit.eligibility is ProposalEligibility.RESEARCH_ONLY
    assert bridge_audit.benchmark_eligible is False
    assert "broker_target_deferred_from_histdata_milestone" in (
        bridge_audit.reason_codes
    )


def test_promotion_evidence_must_match_the_bound_engine_configuration() -> None:
    registry = proposal_engine_registry()
    bindings = {item.engine_id: item for item in _bindings()}
    descriptor = registry.descriptor("histdatacom.event-clock.nhpp")
    mismatched = ProposalEngineEvidenceV1(
        engine_id=descriptor.engine_id,
        campaign_id="campaign:sha256:" + "1" * 64,
        corpus_id="corpus:sha256:" + "0" * 64,
        report_id="report:sha256:" + "2" * 64,
        candidate_id="candidate:sha256:" + "3" * 64,
        method_name="non_homogeneous_poisson",
        promotion_eligible=True,
        provisional=False,
        failure_count=0,
        refusal_count=0,
        failed_gate_ids=(),
        config_ids=("different-config:sha256:" + "4" * 64,),
        fit_ids=("fit:sha256:" + "5" * 64,),
        checkpoint_ids=(),
        training_dataset_ids=(),
    )

    audit = audit_proposal_engine_binding(
        descriptor,
        bindings[descriptor.engine_id],
        evidence=(mismatched,),
    )

    assert not audit.reconstruction_eligible
    assert "retained_evidence_config_identity_differs" in audit.reason_codes


def test_portfolio_is_explicitly_single_engine_without_silent_fallback() -> (
    None
):
    registry = proposal_engine_registry()
    binding = next(
        item
        for item in _bindings()
        if item.engine_id == EMPIRICAL_MOTIF_GENERATOR_ID
    )
    evidence = ProposalEngineEvidenceV1(
        engine_id=EMPIRICAL_MOTIF_GENERATOR_ID,
        campaign_id="campaign:sha256:" + "1" * 64,
        corpus_id="corpus:sha256:" + "0" * 64,
        report_id="report:sha256:" + "2" * 64,
        candidate_id="candidate:sha256:" + "3" * 64,
        method_name="empirical_motif",
        promotion_eligible=True,
        provisional=False,
        failure_count=0,
        refusal_count=0,
        failed_gate_ids=(),
        config_ids=(binding.config_id,),
        fit_ids=(),
        checkpoint_ids=(),
        training_dataset_ids=(),
    )
    portfolio = build_histdata_proposal_portfolio(
        registry=registry,
        dataset_version_id="dataset-version:sha256:" + "a" * 64,
        bindings=_bindings(),
        evidence=(evidence,),
        motif_qualification={
            "candidate_promotion_eligible": True,
            "candidate_provisional": False,
        },
    )

    assert portfolio.selected_engine_ids == (EMPIRICAL_MOTIF_GENERATOR_ID,)
    assert portfolio.fallback_policy == "refuse-no-silent-fallback-v1"
    assert portfolio.to_dict()["portfolio_diversity_claim"] == (
        "single-qualified-engine"
    )
    assert ProposalEnginePortfolioV1.from_dict(portfolio.to_dict()) == portfolio
    motif_audit = next(
        item
        for item in portfolio.eligibility_audits
        if item.engine_id == EMPIRICAL_MOTIF_GENERATOR_ID
    )
    assert motif_audit.ensemble_eligible is True
    assert all(
        not item.reconstruction_eligible
        for item in portfolio.eligibility_audits
        if item.engine_id != EMPIRICAL_MOTIF_GENERATOR_ID
    )


def test_portfolio_preserves_declared_order_and_selection() -> None:
    registry = proposal_engine_registry()
    order = (
        "histdatacom.event-clock.nhpp",
        EMPIRICAL_MOTIF_GENERATOR_ID,
    )
    bindings = {item.engine_id: item for item in _bindings()}
    evidence = ProposalEngineEvidenceV1(
        engine_id=EMPIRICAL_MOTIF_GENERATOR_ID,
        campaign_id="campaign:sha256:" + "1" * 64,
        corpus_id="corpus:sha256:" + "0" * 64,
        report_id="report:sha256:" + "2" * 64,
        candidate_id="candidate:sha256:" + "3" * 64,
        method_name="empirical_motif",
        promotion_eligible=True,
        provisional=False,
        failure_count=0,
        refusal_count=0,
        failed_gate_ids=(),
        config_ids=(bindings[EMPIRICAL_MOTIF_GENERATOR_ID].config_id,),
        fit_ids=(),
        checkpoint_ids=(),
        training_dataset_ids=(),
    )
    portfolio = build_histdata_proposal_portfolio(
        registry=registry,
        dataset_version_id="dataset-version:sha256:" + "a" * 64,
        bindings=tuple(bindings[engine_id] for engine_id in order),
        evidence=(evidence,),
        motif_qualification={"qualified": True},
        engine_ids=order,
        selected_engine_ids=(EMPIRICAL_MOTIF_GENERATOR_ID,),
    )

    assert tuple(item.engine_id for item in portfolio.entries) == order
    assert portfolio.selected_engine_ids == (EMPIRICAL_MOTIF_GENERATOR_ID,)


def test_portfolio_refuses_selection_without_motif_qualification() -> None:
    with pytest.raises(
        ValueError,
        match="not reconstruction eligible",
    ):
        build_histdata_proposal_portfolio(
            registry=proposal_engine_registry(),
            dataset_version_id="dataset-version:sha256:" + "a" * 64,
            bindings=_bindings(),
            evidence=(),
            motif_qualification={"candidate_promotion_eligible": False},
        )


def test_conflicting_exact_config_evidence_revokes_product_eligibility() -> (
    None
):
    registry = proposal_engine_registry()
    binding = next(
        item
        for item in _bindings()
        if item.engine_id == EMPIRICAL_MOTIF_GENERATOR_ID
    )

    def observation(label: str, promoted: bool) -> ProposalEngineEvidenceV1:
        return ProposalEngineEvidenceV1(
            engine_id=EMPIRICAL_MOTIF_GENERATOR_ID,
            campaign_id=f"campaign-{label}:sha256:" + "1" * 64,
            corpus_id="corpus:sha256:" + "0" * 64,
            report_id=f"report-{label}:sha256:" + "2" * 64,
            candidate_id=f"candidate-{label}:sha256:" + "3" * 64,
            method_name="empirical_motif",
            promotion_eligible=promoted,
            provisional=False,
            failure_count=0,
            refusal_count=0,
            failed_gate_ids=(() if promoted else ("candidate-event-count",)),
            config_ids=(binding.config_id,),
            fit_ids=(),
            checkpoint_ids=(),
            training_dataset_ids=(),
        )

    audit = audit_proposal_engine_binding(
        registry.descriptor(EMPIRICAL_MOTIF_GENERATOR_ID),
        binding,
        evidence=(observation("pass", True), observation("fail", False)),
        motif_qualification={"qualified": True},
    )

    assert not audit.reconstruction_eligible
    assert "conflicting_exact_config_promotion_evidence" in audit.reason_codes


def test_powered_qualification_can_only_reduce_legacy_eligibility() -> None:
    registry = proposal_engine_registry()
    descriptor = registry.descriptor(EMPIRICAL_MOTIF_GENERATOR_ID)
    binding = next(
        item
        for item in _bindings()
        if item.engine_id == EMPIRICAL_MOTIF_GENERATOR_ID
    )

    def observation(promoted: bool) -> ProposalEngineEvidenceV1:
        return ProposalEngineEvidenceV1(
            engine_id=EMPIRICAL_MOTIF_GENERATOR_ID,
            campaign_id="campaign:sha256:" + "1" * 64,
            corpus_id="corpus:sha256:" + "0" * 64,
            report_id="report:sha256:" + "2" * 64,
            candidate_id="candidate:sha256:" + "3" * 64,
            method_name="empirical_motif",
            promotion_eligible=promoted,
            provisional=False,
            failure_count=0,
            refusal_count=0,
            failed_gate_ids=(() if promoted else ("candidate-event-count",)),
            config_ids=(binding.config_id,),
            fit_ids=(),
            checkpoint_ids=(),
            training_dataset_ids=(),
        )

    insufficient = SimpleNamespace(
        engine_id=EMPIRICAL_MOTIF_GENERATOR_ID,
        benchmark_eligible=True,
        reconstruction_eligible=False,
        ensemble_eligible=False,
        decision_id="decision-insufficient:sha256:" + "4" * 64,
        status=QualificationStatus.INSUFFICIENT_EVIDENCE,
    )
    reduced = audit_proposal_engine_binding(
        descriptor,
        binding,
        evidence=(observation(True),),
        motif_qualification={"qualified": True},
        qualification_decision=insufficient,
    )

    assert reduced.benchmark_eligible
    assert not reduced.reconstruction_eligible
    assert not reduced.ensemble_eligible
    assert "powered_qualification_insufficient_evidence" in (
        reduced.reason_codes
    )

    passed = SimpleNamespace(
        engine_id=EMPIRICAL_MOTIF_GENERATOR_ID,
        benchmark_eligible=True,
        reconstruction_eligible=True,
        ensemble_eligible=True,
        decision_id="decision-passed:sha256:" + "5" * 64,
        status=QualificationStatus.PASSED,
    )
    not_promoted = audit_proposal_engine_binding(
        descriptor,
        binding,
        evidence=(observation(False),),
        motif_qualification={"qualified": True},
        qualification_decision=passed,
    )

    assert not not_promoted.reconstruction_eligible
    assert not not_promoted.ensemble_eligible


def test_evaluation_contract_retains_execution_and_refusal_evidence() -> None:
    registry = proposal_engine_registry()
    evidence = ProposalEngineEvidenceV1(
        engine_id="histdatacom.event-clock.nhpp",
        campaign_id="campaign:sha256:" + "1" * 64,
        corpus_id="corpus:sha256:" + "5" * 64,
        report_id="report:sha256:" + "2" * 64,
        candidate_id="candidate:sha256:" + "3" * 64,
        method_name="non_homogeneous_poisson",
        promotion_eligible=False,
        provisional=False,
        failure_count=0,
        refusal_count=0,
        failed_gate_ids=("candidate-event-count",),
        config_ids=(
            proposal_engine_default_configs()[
                "histdatacom.event-clock.nhpp"
            ].config_id,
        ),
        fit_ids=("fit:sha256:" + "4" * 64,),
        checkpoint_ids=(),
        training_dataset_ids=(),
    )
    result = ProposalPortfolioEvaluationV1(
        registry_id=registry.registry_id,
        corpus_id="corpus:sha256:" + "5" * 64,
        campaign_id=evidence.campaign_id,
        requested_engine_ids=(evidence.engine_id, SB_GENERATOR_ID),
        reference_engine_ids=(),
        executed_engine_ids=(evidence.engine_id,),
        refused_engine_ids=(SB_GENERATOR_ID,),
        engine_evidence=(evidence,),
        artifact_refs={"scorecard": _ref("scorecard", "evaluation")},
    )

    assert result.to_dict()["automatic_winner"] is False
    assert result.to_dict()["current_provider_id"] == "histdata.com"
    assert ProposalPortfolioEvaluationV1.from_dict(result.to_dict()) == result
