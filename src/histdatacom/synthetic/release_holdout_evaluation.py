"""Gate-derived evaluation of an exact reconstruction release holdout.

The original :mod:`histdatacom.synthetic.release_holdout` contracts establish
the sealed split and atomic one-time ledger.  This module supplies the stricter
release path: the manifest, evaluation policy, and benchmark corpus must exist
in Git before the exact installable candidate; the candidate must bind those
identities; and the final outcome is derived from a holdout-only row-free
metric trace under the already committed benchmark gates.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import resources
from pathlib import Path, PurePosixPath
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONScalar, JSONValue
from histdatacom.synthetic.benchmark import BenchmarkCandidateV1
from histdatacom.synthetic.benchmark_corpus import (
    PREDECLARED_GATE_COMMIT,
    ReverseDegradationBenchmarkCorpusV1,
    read_benchmark_window_metric_trace,
    read_reverse_degradation_benchmark_campaign,
    read_reverse_degradation_benchmark_corpus,
)
from histdatacom.synthetic.benchmark_gates import (
    BenchmarkGateObservationV1,
    BenchmarkGateScope,
    BenchmarkGateStatus,
    BenchmarkPromotionDecisionV1,
    evaluate_benchmark_promotion_gates,
    load_default_benchmark_promotion_gate_policy,
)
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.marked_hawkes import (
    MarkedHawkesConfigV1,
    MarkedHawkesFitResultV1,
    build_marked_hawkes_benchmark_candidate,
)
from histdatacom.synthetic.release_candidate import (
    ReconstructionReleaseCandidateV1,
    read_reconstruction_release_candidate,
)
from histdatacom.synthetic.release_holdout import (
    ProtectedReleaseHoldoutManifestV1,
    ReleaseCandidateFreezeV1,
    ReleaseHoldoutAlreadyConsumedError,
    ReleaseHoldoutEvaluationOutcome,
    read_protected_release_holdout_manifest,
    read_release_candidate_freeze,
)

RELEASE_HOLDOUT_EVALUATION_POLICY_SCHEMA_VERSION = (
    "histdatacom.release-holdout-evaluation-policy.v1"
)
RECONSTRUCTION_RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-release-holdout-authorization.v1"
)
RELEASE_HOLDOUT_GATE_REPORT_SCHEMA_VERSION = (
    "histdatacom.release-holdout-gate-report.v1"
)
RECONSTRUCTION_RELEASE_HOLDOUT_RECEIPT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-release-holdout-receipt.v1"
)
RECONSTRUCTION_RELEASE_HOLDOUT_RETIREMENT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-release-holdout-retirement.v1"
)
DEFAULT_RELEASE_HOLDOUT_EVALUATION_POLICY_ASSET = (
    "assets/release_holdout_evaluation_policy_v1.json"
)
MAX_RELEASE_HOLDOUT_EVALUATION_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_RELEASE_HOLDOUT_EVALUATION_ITEMS = 4096

_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_OBJECT_ID = re.compile(r"[0-9a-f]{40,64}")
_HARD_METRIC_DEFAULTS: Mapping[str, float] = {
    "event_count_relative_error": 1.0,
    "interarrival_hist_l1": 1.0,
    "path_realized_variation_relative_error": 1.0,
    "spread_tail_relative_error": 1.0,
    "update_transition_l1": 1.0,
    "immutable_anchor_violation_count": 1.0,
    "unsupported_context_emission_count": 1.0,
    "triangle_residual_p99_pips": 0.0,
}
_COUNT_COMPARISON_METRICS = frozenset(
    {
        "immutable_anchor_violation_count",
        "unsupported_context_emission_count",
    }
)
_UNCERTAINTY_METRICS = (
    "event_count_relative_error",
    "interarrival_hist_l1",
    "path_realized_variation_relative_error",
    "spread_tail_relative_error",
    "update_transition_l1",
    "triangle_residual_p99_pips",
)


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutEvaluationPolicyV1:
    """Pre-result policy binding the exact benchmark gate semantics."""

    policy_name: str
    policy_version: str
    issue_number: int
    benchmark_gate_policy_id: str
    benchmark_gate_policy_commit: str
    required_split_kind: str
    minimum_ensemble_member_count: int
    frozen_fit_required: bool
    holdout_only_metrics_required: bool
    source_replay_required: bool
    manifest_policy_and_corpus_git_commit_required: bool
    candidate_selected_without_holdout_results: bool
    holdout_selection_role: bool
    frozen_before_release_holdout_results: bool
    policy_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_EVALUATION_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_HOLDOUT_EVALUATION_POLICY_SCHEMA_VERSION,
        )
        for name in ("policy_name", "policy_version"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if isinstance(self.issue_number, bool) or self.issue_number != 512:
            raise ValueError("release-holdout evaluation policy issue differs")
        benchmark = load_default_benchmark_promotion_gate_policy()
        if self.benchmark_gate_policy_id != benchmark.policy_id:
            raise ValueError("release-holdout benchmark gate policy differs")
        if self.benchmark_gate_policy_commit != PREDECLARED_GATE_COMMIT:
            raise ValueError("release-holdout benchmark gate commit differs")
        if self.required_split_kind != "final_holdout":
            raise ValueError("release-holdout evaluation split differs")
        if (
            isinstance(self.minimum_ensemble_member_count, bool)
            or not 2 <= self.minimum_ensemble_member_count <= 8
        ):
            raise ValueError("release-holdout ensemble minimum is invalid")
        required_true = (
            "frozen_fit_required",
            "holdout_only_metrics_required",
            "source_replay_required",
            "manifest_policy_and_corpus_git_commit_required",
            "candidate_selected_without_holdout_results",
            "frozen_before_release_holdout_results",
        )
        if any(getattr(self, name) is not True for name in required_true):
            raise ValueError("release-holdout evaluation policy weakened")
        if self.holdout_selection_role is not False:
            raise ValueError("release holdout cannot select a candidate")
        expected = _stable_id(
            "release-holdout-evaluation-policy", self.payload()
        )
        if self.policy_id and self.policy_id != expected:
            raise ValueError(
                "release-holdout evaluation policy identity differs"
            )
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy_name": self.policy_name,
            "policy_version": self.policy_version,
            "issue_number": self.issue_number,
            "benchmark_gate_policy_id": self.benchmark_gate_policy_id,
            "benchmark_gate_policy_commit": self.benchmark_gate_policy_commit,
            "required_split_kind": self.required_split_kind,
            "minimum_ensemble_member_count": self.minimum_ensemble_member_count,
            "frozen_fit_required": self.frozen_fit_required,
            "holdout_only_metrics_required": self.holdout_only_metrics_required,
            "source_replay_required": self.source_replay_required,
            "manifest_policy_and_corpus_git_commit_required": (
                self.manifest_policy_and_corpus_git_commit_required
            ),
            "candidate_selected_without_holdout_results": (
                self.candidate_selected_without_holdout_results
            ),
            "holdout_selection_role": self.holdout_selection_role,
            "frozen_before_release_holdout_results": (
                self.frozen_before_release_holdout_results
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseHoldoutEvaluationPolicyV1:
        return cls(
            policy_name=str(data.get("policy_name", "")),
            policy_version=str(data.get("policy_version", "")),
            issue_number=_strict_int(data.get("issue_number"), "issue_number"),
            benchmark_gate_policy_id=str(
                data.get("benchmark_gate_policy_id", "")
            ),
            benchmark_gate_policy_commit=str(
                data.get("benchmark_gate_policy_commit", "")
            ),
            required_split_kind=str(data.get("required_split_kind", "")),
            minimum_ensemble_member_count=_strict_int(
                data.get("minimum_ensemble_member_count"),
                "minimum_ensemble_member_count",
            ),
            frozen_fit_required=_strict_bool(
                data.get("frozen_fit_required"), "frozen_fit_required"
            ),
            holdout_only_metrics_required=_strict_bool(
                data.get("holdout_only_metrics_required"),
                "holdout_only_metrics_required",
            ),
            source_replay_required=_strict_bool(
                data.get("source_replay_required"), "source_replay_required"
            ),
            manifest_policy_and_corpus_git_commit_required=_strict_bool(
                data.get("manifest_policy_and_corpus_git_commit_required"),
                "manifest_policy_and_corpus_git_commit_required",
            ),
            candidate_selected_without_holdout_results=_strict_bool(
                data.get("candidate_selected_without_holdout_results"),
                "candidate_selected_without_holdout_results",
            ),
            holdout_selection_role=_strict_bool(
                data.get("holdout_selection_role"), "holdout_selection_role"
            ),
            frozen_before_release_holdout_results=_strict_bool(
                data.get("frozen_before_release_holdout_results"),
                "frozen_before_release_holdout_results",
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionReleaseHoldoutAuthorizationV1:
    """Authorization bound to Git evidence and one installable candidate."""

    manifest_id: str
    manifest_ref: ArtifactRef
    graph_id: str
    graph_ref: ArtifactRef
    scientific_candidate_id: str
    release_candidate_id: str
    release_candidate_ref: ArtifactRef
    evaluation_policy_id: str
    evaluation_policy_ref: ArtifactRef
    corpus_id: str
    corpus_ref: ArtifactRef
    ensemble_member_ids: tuple[str, ...]
    evidence_git_commit_sha: str
    repository_root: str
    manifest_git_path: str
    evaluation_policy_git_path: str
    corpus_git_path: str
    authorized_at_utc: str
    authorization_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION,
        )
        for name in (
            "manifest_id",
            "graph_id",
            "scientific_candidate_id",
            "release_candidate_id",
            "evaluation_policy_id",
            "corpus_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in (
            "manifest_ref",
            "graph_ref",
            "release_candidate_ref",
            "evaluation_policy_ref",
            "corpus_ref",
        ):
            ref = getattr(self, name)
            if not isinstance(ref, ArtifactRef):
                raise TypeError(
                    "release-holdout authorization reference invalid"
                )
            _require_strong_ref(ref)
        members = _text_tuple(self.ensemble_member_ids)
        if not 2 <= len(members) <= 8:
            raise ValueError("release-holdout authorization members invalid")
        object.__setattr__(self, "ensemble_member_ids", members)
        object.__setattr__(
            self,
            "evidence_git_commit_sha",
            _git_object_id(self.evidence_git_commit_sha),
        )
        root = Path(self.repository_root).expanduser().resolve()
        object.__setattr__(self, "repository_root", str(root))
        for name in (
            "manifest_git_path",
            "evaluation_policy_git_path",
            "corpus_git_path",
        ):
            object.__setattr__(self, name, _safe_git_path(getattr(self, name)))
        object.__setattr__(
            self, "authorized_at_utc", _timestamp(self.authorized_at_utc)
        )
        expected = _stable_id(
            "reconstruction-release-holdout-authorization", self.payload()
        )
        if self.authorization_id and self.authorization_id != expected:
            raise ValueError("release-holdout authorization identity differs")
        object.__setattr__(self, "authorization_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "manifest_ref": self.manifest_ref.to_dict(),
            "graph_id": self.graph_id,
            "graph_ref": self.graph_ref.to_dict(),
            "scientific_candidate_id": self.scientific_candidate_id,
            "release_candidate_id": self.release_candidate_id,
            "release_candidate_ref": self.release_candidate_ref.to_dict(),
            "evaluation_policy_id": self.evaluation_policy_id,
            "evaluation_policy_ref": self.evaluation_policy_ref.to_dict(),
            "corpus_id": self.corpus_id,
            "corpus_ref": self.corpus_ref.to_dict(),
            "ensemble_member_ids": list(self.ensemble_member_ids),
            "evidence_git_commit_sha": self.evidence_git_commit_sha,
            "repository_root": self.repository_root,
            "manifest_git_path": self.manifest_git_path,
            "evaluation_policy_git_path": self.evaluation_policy_git_path,
            "corpus_git_path": self.corpus_git_path,
            "authorized_at_utc": self.authorized_at_utc,
            "maximum_evaluations": 1,
            "git_committed_before_candidate": True,
            "exact_frozen_fit_required": True,
            "holdout_selection_role": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "authorization_id": self.authorization_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionReleaseHoldoutAuthorizationV1:
        expected = {
            "maximum_evaluations": 1,
            "git_committed_before_candidate": True,
            "exact_frozen_fit_required": True,
            "holdout_selection_role": False,
        }
        if any(data.get(key) != value for key, value in expected.items()):
            raise ValueError("release-holdout authorization policy differs")
        return cls(
            manifest_id=str(data.get("manifest_id", "")),
            manifest_ref=ArtifactRef.from_dict(
                _mapping(data.get("manifest_ref"))
            ),
            graph_id=str(data.get("graph_id", "")),
            graph_ref=ArtifactRef.from_dict(_mapping(data.get("graph_ref"))),
            scientific_candidate_id=str(
                data.get("scientific_candidate_id", "")
            ),
            release_candidate_id=str(data.get("release_candidate_id", "")),
            release_candidate_ref=ArtifactRef.from_dict(
                _mapping(data.get("release_candidate_ref"))
            ),
            evaluation_policy_id=str(data.get("evaluation_policy_id", "")),
            evaluation_policy_ref=ArtifactRef.from_dict(
                _mapping(data.get("evaluation_policy_ref"))
            ),
            corpus_id=str(data.get("corpus_id", "")),
            corpus_ref=ArtifactRef.from_dict(_mapping(data.get("corpus_ref"))),
            ensemble_member_ids=_string_tuple(data.get("ensemble_member_ids")),
            evidence_git_commit_sha=str(
                data.get("evidence_git_commit_sha", "")
            ),
            repository_root=str(data.get("repository_root", "")),
            manifest_git_path=str(data.get("manifest_git_path", "")),
            evaluation_policy_git_path=str(
                data.get("evaluation_policy_git_path", "")
            ),
            corpus_git_path=str(data.get("corpus_git_path", "")),
            authorized_at_utc=str(data.get("authorized_at_utc", "")),
            authorization_id=str(data.get("authorization_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseHoldoutGateReportV1:
    """Row-free, holdout-only gate report for one frozen candidate."""

    authorization_id: str
    manifest_id: str
    graph_id: str
    scientific_candidate_id: str
    release_candidate_id: str
    evaluation_policy_id: str
    corpus_id: str
    campaign_id: str
    metric_trace_id: str
    benchmark_candidate_id: str
    config_id: str
    fit_id: str
    window_id_map: Mapping[str, str]
    ensemble_member_ids: tuple[str, ...]
    candidate_metrics: Mapping[str, JSONScalar]
    campaign_decision: BenchmarkPromotionDecisionV1
    candidate_decision: BenchmarkPromotionDecisionV1
    corpus_ref: ArtifactRef
    campaign_ref: ArtifactRef
    metric_trace_ref: ArtifactRef
    outcome: ReleaseHoldoutEvaluationOutcome
    report_id: str = ""
    schema_version: str = RELEASE_HOLDOUT_GATE_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RELEASE_HOLDOUT_GATE_REPORT_SCHEMA_VERSION
        )
        for name in (
            "authorization_id",
            "manifest_id",
            "graph_id",
            "scientific_candidate_id",
            "release_candidate_id",
            "evaluation_policy_id",
            "corpus_id",
            "campaign_id",
            "metric_trace_id",
            "benchmark_candidate_id",
            "config_id",
            "fit_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        mapping = {
            _required_text(key): _required_text(value)
            for key, value in sorted(self.window_id_map.items())
        }
        if not mapping or len(mapping) > MAX_RELEASE_HOLDOUT_EVALUATION_ITEMS:
            raise ValueError("release-holdout report window mapping invalid")
        object.__setattr__(self, "window_id_map", mapping)
        members = _text_tuple(self.ensemble_member_ids)
        object.__setattr__(self, "ensemble_member_ids", members)
        metrics = {
            _required_text(key): _json_scalar(value, key)
            for key, value in sorted(self.candidate_metrics.items())
        }
        if not metrics or len(metrics) > MAX_RELEASE_HOLDOUT_EVALUATION_ITEMS:
            raise ValueError("release-holdout candidate metrics invalid")
        object.__setattr__(self, "candidate_metrics", metrics)
        if (
            not isinstance(self.campaign_decision, BenchmarkPromotionDecisionV1)
            or self.campaign_decision.scope is not BenchmarkGateScope.CAMPAIGN
            or self.campaign_decision.subject_id != self.corpus_id
        ):
            raise ValueError("release-holdout campaign decision differs")
        if (
            not isinstance(
                self.candidate_decision, BenchmarkPromotionDecisionV1
            )
            or self.candidate_decision.scope is not BenchmarkGateScope.CANDIDATE
            or self.candidate_decision.subject_id != self.benchmark_candidate_id
        ):
            raise ValueError("release-holdout candidate decision differs")
        for name in ("corpus_ref", "campaign_ref", "metric_trace_ref"):
            ref = getattr(self, name)
            if not isinstance(ref, ArtifactRef):
                raise TypeError("release-holdout report reference invalid")
            _require_strong_ref(ref)
        expected_outcome = _derived_outcome(
            self.campaign_decision, self.candidate_decision
        )
        if self.outcome is not expected_outcome:
            raise ValueError("release-holdout outcome differs from gates")
        expected = _stable_id("release-holdout-gate-report", self.payload())
        if self.report_id and self.report_id != expected:
            raise ValueError("release-holdout gate report identity differs")
        object.__setattr__(self, "report_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "authorization_id": self.authorization_id,
            "manifest_id": self.manifest_id,
            "graph_id": self.graph_id,
            "scientific_candidate_id": self.scientific_candidate_id,
            "release_candidate_id": self.release_candidate_id,
            "evaluation_policy_id": self.evaluation_policy_id,
            "corpus_id": self.corpus_id,
            "campaign_id": self.campaign_id,
            "metric_trace_id": self.metric_trace_id,
            "benchmark_candidate_id": self.benchmark_candidate_id,
            "config_id": self.config_id,
            "fit_id": self.fit_id,
            "window_id_map": dict(self.window_id_map),
            "ensemble_member_ids": list(self.ensemble_member_ids),
            "candidate_metrics": dict(self.candidate_metrics),
            "campaign_decision": self.campaign_decision.to_dict(),
            "candidate_decision": self.candidate_decision.to_dict(),
            "corpus_ref": self.corpus_ref.to_dict(),
            "campaign_ref": self.campaign_ref.to_dict(),
            "metric_trace_ref": self.metric_trace_ref.to_dict(),
            "outcome": self.outcome.value,
            "evaluation_number": 1,
            "holdout_only": True,
            "event_rows_embedded": False,
            "holdout_selection_role": False,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "report_id": self.report_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReleaseHoldoutGateReportV1:
        expected = {
            "evaluation_number": 1,
            "holdout_only": True,
            "event_rows_embedded": False,
            "holdout_selection_role": False,
            "automatic_winner": False,
        }
        if any(data.get(key) != value for key, value in expected.items()):
            raise ValueError("release-holdout gate report policy differs")
        return cls(
            authorization_id=str(data.get("authorization_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            graph_id=str(data.get("graph_id", "")),
            scientific_candidate_id=str(
                data.get("scientific_candidate_id", "")
            ),
            release_candidate_id=str(data.get("release_candidate_id", "")),
            evaluation_policy_id=str(data.get("evaluation_policy_id", "")),
            corpus_id=str(data.get("corpus_id", "")),
            campaign_id=str(data.get("campaign_id", "")),
            metric_trace_id=str(data.get("metric_trace_id", "")),
            benchmark_candidate_id=str(data.get("benchmark_candidate_id", "")),
            config_id=str(data.get("config_id", "")),
            fit_id=str(data.get("fit_id", "")),
            window_id_map={
                str(key): str(value)
                for key, value in _mapping(data.get("window_id_map")).items()
            },
            ensemble_member_ids=_string_tuple(data.get("ensemble_member_ids")),
            candidate_metrics={
                str(key): _json_scalar(value, str(key))
                for key, value in _mapping(
                    data.get("candidate_metrics")
                ).items()
            },
            campaign_decision=BenchmarkPromotionDecisionV1.from_dict(
                _mapping(data.get("campaign_decision"))
            ),
            candidate_decision=BenchmarkPromotionDecisionV1.from_dict(
                _mapping(data.get("candidate_decision"))
            ),
            corpus_ref=ArtifactRef.from_dict(_mapping(data.get("corpus_ref"))),
            campaign_ref=ArtifactRef.from_dict(
                _mapping(data.get("campaign_ref"))
            ),
            metric_trace_ref=ArtifactRef.from_dict(
                _mapping(data.get("metric_trace_ref"))
            ),
            outcome=ReleaseHoldoutEvaluationOutcome(
                str(data.get("outcome", ""))
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionReleaseHoldoutReceiptV1:
    """Durable gate-derived or operational-failure one-time receipt."""

    authorization_id: str
    manifest_id: str
    graph_id: str
    scientific_candidate_id: str
    release_candidate_id: str
    outcome: ReleaseHoldoutEvaluationOutcome
    evaluated_at_utc: str
    report_id: str = ""
    report_ref: ArtifactRef | None = None
    operational_error_type: str = ""
    receipt_id: str = ""
    schema_version: str = RECONSTRUCTION_RELEASE_HOLDOUT_RECEIPT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_RELEASE_HOLDOUT_RECEIPT_SCHEMA_VERSION,
        )
        for name in (
            "authorization_id",
            "manifest_id",
            "graph_id",
            "scientific_candidate_id",
            "release_candidate_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        operational = (
            self.outcome is ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE
        )
        if operational:
            if self.report_ref is not None or self.report_id:
                raise ValueError("operational failure cannot claim a report")
            object.__setattr__(
                self,
                "operational_error_type",
                _required_text(self.operational_error_type),
            )
        else:
            if not isinstance(self.report_ref, ArtifactRef):
                raise ValueError("release-holdout receipt report is absent")
            _require_strong_ref(self.report_ref)
            object.__setattr__(
                self, "report_id", _required_text(self.report_id)
            )
            if self.operational_error_type:
                raise ValueError("gate-derived receipt has operational error")
        object.__setattr__(
            self, "evaluated_at_utc", _timestamp(self.evaluated_at_utc)
        )
        expected = _stable_id(
            "reconstruction-release-holdout-receipt", self.payload()
        )
        if self.receipt_id and self.receipt_id != expected:
            raise ValueError("release-holdout receipt identity differs")
        object.__setattr__(self, "receipt_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "authorization_id": self.authorization_id,
            "manifest_id": self.manifest_id,
            "graph_id": self.graph_id,
            "scientific_candidate_id": self.scientific_candidate_id,
            "release_candidate_id": self.release_candidate_id,
            "outcome": self.outcome.value,
            "evaluated_at_utc": self.evaluated_at_utc,
            "report_id": self.report_id,
            "report_ref": (
                self.report_ref.to_dict()
                if self.report_ref is not None
                else None
            ),
            "operational_error_type": self.operational_error_type,
            "evaluation_number": 1,
            "holdout_consumed": True,
            "retry_permitted": False,
            "holdout_selection_role": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "receipt_id": self.receipt_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionReleaseHoldoutReceiptV1:
        expected = {
            "evaluation_number": 1,
            "holdout_consumed": True,
            "retry_permitted": False,
            "holdout_selection_role": False,
        }
        if any(data.get(key) != value for key, value in expected.items()):
            raise ValueError("release-holdout receipt policy differs")
        raw_ref = data.get("report_ref")
        return cls(
            authorization_id=str(data.get("authorization_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            graph_id=str(data.get("graph_id", "")),
            scientific_candidate_id=str(
                data.get("scientific_candidate_id", "")
            ),
            release_candidate_id=str(data.get("release_candidate_id", "")),
            outcome=ReleaseHoldoutEvaluationOutcome(
                str(data.get("outcome", ""))
            ),
            evaluated_at_utc=str(data.get("evaluated_at_utc", "")),
            report_id=str(data.get("report_id", "")),
            report_ref=(
                ArtifactRef.from_dict(_mapping(raw_ref))
                if raw_ref is not None
                else None
            ),
            operational_error_type=str(data.get("operational_error_type", "")),
            receipt_id=str(data.get("receipt_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionReleaseHoldoutRetirementV1:
    """Permanent retirement marker for the exact candidate evaluation."""

    manifest_id: str
    release_candidate_id: str
    receipt_id: str
    outcome: ReleaseHoldoutEvaluationOutcome
    retired_at_utc: str
    successor_manifest_id: str = ""
    marker_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_RELEASE_HOLDOUT_RETIREMENT_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_RELEASE_HOLDOUT_RETIREMENT_SCHEMA_VERSION,
        )
        for name in ("manifest_id", "release_candidate_id", "receipt_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "retired_at_utc", _timestamp(self.retired_at_utc)
        )
        if self.outcome is not ReleaseHoldoutEvaluationOutcome.PASSED:
            object.__setattr__(
                self,
                "successor_manifest_id",
                _required_text(self.successor_manifest_id),
            )
        expected = _stable_id(
            "reconstruction-release-holdout-retirement", self.payload()
        )
        if self.marker_id and self.marker_id != expected:
            raise ValueError("release-holdout retirement identity differs")
        object.__setattr__(self, "marker_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest_id": self.manifest_id,
            "release_candidate_id": self.release_candidate_id,
            "receipt_id": self.receipt_id,
            "outcome": self.outcome.value,
            "retired_at_utc": self.retired_at_utc,
            "successor_manifest_id": self.successor_manifest_id,
            "retired": True,
            "reuse_permitted": False,
            "next_release_requires_fresh_manifest": True,
            "same_holdout_tuning_permitted": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "marker_id": self.marker_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionReleaseHoldoutRetirementV1:
        expected = {
            "retired": True,
            "reuse_permitted": False,
            "next_release_requires_fresh_manifest": True,
            "same_holdout_tuning_permitted": False,
        }
        if any(data.get(key) != value for key, value in expected.items()):
            raise ValueError("release-holdout retirement policy differs")
        return cls(
            manifest_id=str(data.get("manifest_id", "")),
            release_candidate_id=str(data.get("release_candidate_id", "")),
            receipt_id=str(data.get("receipt_id", "")),
            outcome=ReleaseHoldoutEvaluationOutcome(
                str(data.get("outcome", ""))
            ),
            retired_at_utc=str(data.get("retired_at_utc", "")),
            successor_manifest_id=str(data.get("successor_manifest_id", "")),
            marker_id=str(data.get("marker_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def load_default_release_holdout_evaluation_policy() -> (
    ReleaseHoldoutEvaluationPolicyV1
):
    """Load and identity-check the packaged pre-result evaluation policy."""
    asset = resources.files("histdatacom.synthetic").joinpath(
        DEFAULT_RELEASE_HOLDOUT_EVALUATION_POLICY_ASSET
    )
    return ReleaseHoldoutEvaluationPolicyV1.from_dict(
        _json_mapping(asset.read_text(encoding="utf-8"))
    )


def authorize_reconstruction_release_holdout(
    manifest_ref: ArtifactRef,
    graph_ref: ArtifactRef,
    release_candidate_ref: ArtifactRef,
    evaluation_policy_ref: ArtifactRef,
    corpus_ref: ArtifactRef,
    *,
    repository_root: str | Path,
    evidence_git_commit_sha: str,
    authorized_at_utc: str,
) -> ReconstructionReleaseHoldoutAuthorizationV1:
    """Authorize only Git-committed evidence bound by the exact candidate."""
    root = Path(repository_root).expanduser().resolve()
    manifest_path = _relative_git_path(root, manifest_ref.path)
    policy_path = _relative_git_path(root, evaluation_policy_ref.path)
    corpus_path = _relative_git_path(root, corpus_ref.path)
    commit = _git_object_id(evidence_git_commit_sha)
    manifest = read_protected_release_holdout_manifest(
        _verify_ref(manifest_ref)
    )
    graph = read_release_candidate_freeze(_verify_ref(graph_ref))
    candidate = read_reconstruction_release_candidate(
        _verify_ref(release_candidate_ref)
    )
    policy = read_release_holdout_evaluation_policy(
        _verify_ref(evaluation_policy_ref)
    )
    corpus = read_reverse_degradation_benchmark_corpus(_verify_ref(corpus_ref))
    members = _candidate_holdout_policy_members(candidate, policy, corpus)
    _validate_candidate_links(
        manifest,
        manifest_ref,
        graph,
        graph_ref,
        candidate,
        release_candidate_ref,
        policy,
        evaluation_policy_ref,
        corpus,
        corpus_ref,
    )
    _verify_git_evidence(
        root,
        commit,
        candidate,
        graph,
        (
            (manifest_path, manifest_ref),
            (policy_path, evaluation_policy_ref),
            (corpus_path, corpus_ref),
        ),
    )
    authorized = _timestamp(authorized_at_utc)
    if _timestamp_value(authorized) <= _timestamp_value(
        candidate.frozen_at_utc
    ):
        raise ValueError("release-holdout authorization predates candidate")
    return ReconstructionReleaseHoldoutAuthorizationV1(
        manifest_id=manifest.manifest_id,
        manifest_ref=manifest_ref,
        graph_id=graph.graph_id,
        graph_ref=graph_ref,
        scientific_candidate_id=graph.candidate_id,
        release_candidate_id=candidate.candidate_id,
        release_candidate_ref=release_candidate_ref,
        evaluation_policy_id=policy.policy_id,
        evaluation_policy_ref=evaluation_policy_ref,
        corpus_id=corpus.corpus_id,
        corpus_ref=corpus_ref,
        ensemble_member_ids=members,
        evidence_git_commit_sha=commit,
        repository_root=str(root),
        manifest_git_path=manifest_path,
        evaluation_policy_git_path=policy_path,
        corpus_git_path=corpus_path,
        authorized_at_utc=authorized,
    )


def build_release_holdout_gate_report(
    authorization: ReconstructionReleaseHoldoutAuthorizationV1,
    campaign_ref: ArtifactRef,
    metric_trace_ref: ArtifactRef,
) -> ReleaseHoldoutGateReportV1:
    """Derive the release result from exact holdout-only trace metrics."""
    manifest, graph, candidate, policy, corpus = _authorization_inputs(
        authorization
    )
    campaign = read_reverse_degradation_benchmark_campaign(
        _verify_ref(campaign_ref)
    )
    trace = read_benchmark_window_metric_trace(_verify_ref(metric_trace_ref))
    if (
        campaign.corpus_id != corpus.corpus_id
        or trace.corpus_id != corpus.corpus_id
        or trace.campaign_id != campaign.campaign_id
    ):
        raise ValueError("release-holdout campaign graph differs")
    benchmark_policy = load_default_benchmark_promotion_gate_policy()
    if (
        benchmark_policy.policy_id != policy.benchmark_gate_policy_id
        or campaign.campaign_gate_decision.policy_id
        != benchmark_policy.policy_id
        or not campaign.source_replay_verified
    ):
        raise ValueError("release-holdout campaign policy or replay differs")
    config, fit, benchmark_candidate = _exact_benchmark_candidate(
        candidate, authorization.ensemble_member_ids
    )
    benchmark_candidate_id = benchmark_candidate.candidate_id
    expected_method_name = f"marked_hawkes_{config.excitation_structure.value}"
    campaign_reports = tuple(
        item
        for item in campaign.candidate_reports
        if item.candidate_id == benchmark_candidate_id
    )
    if (
        len(campaign_reports) != 1
        or campaign_reports[0].method_name != expected_method_name
        or campaign_reports[0].role != "candidate"
        or campaign_reports[0].provisional
        or campaign_reports[0].ensemble_member_count
        != len(authorization.ensemble_member_ids)
    ):
        raise ValueError("release-holdout campaign exact candidate differs")
    window_map = _holdout_window_map(manifest, corpus)
    target_window_ids = set(window_map.values())
    corpus_windows = {item.window_id: item for item in corpus.windows}
    selected = tuple(
        item
        for item in trace.observations
        if item.candidate_id == benchmark_candidate_id
        and item.split_kind == policy.required_split_kind
    )
    if {item.window_id for item in selected} - target_window_ids:
        raise ValueError("release-holdout trace includes foreign windows")
    for observation in selected:
        window = corpus_windows[observation.window_id]
        if (
            observation.method_name != expected_method_name
            or observation.role != "candidate"
            or observation.session != window.session
            or observation.epoch_label != window.epoch_label
            or observation.context_state != window.context_state
            or observation.positioning_state != window.positioning_state
        ):
            raise ValueError("release-holdout trace candidate metadata differs")
        if not set(_HARD_METRIC_DEFAULTS) <= set(
            observation.comparison_metrics
        ):
            raise ValueError("release-holdout trace metrics are incomplete")
        for name in _HARD_METRIC_DEFAULTS:
            value = observation.comparison_metrics[name]
            if value < 0.0:
                raise ValueError("release-holdout trace metric is negative")
            if name in _COUNT_COMPARISON_METRICS and not value.is_integer():
                raise ValueError("release-holdout trace count is fractional")
    observed_cells = {
        (item.window_id, item.ensemble_member_id): item for item in selected
    }
    if len(observed_cells) != len(selected):
        raise ValueError("release-holdout trace duplicates candidate cells")
    expected_cells = {
        (window_id, member_id)
        for window_id in target_window_ids
        for member_id in authorization.ensemble_member_ids
    }
    if set(observed_cells) - expected_cells:
        raise ValueError("release-holdout trace contains foreign members")
    missing_cell_count = len(expected_cells - set(observed_cells))
    values: dict[str, list[float]] = {}
    for observation in selected:
        for name, value in observation.comparison_metrics.items():
            values.setdefault(name, []).append(value)
    maxima = {
        name: max(values.get(name, [default]))
        for name, default in _HARD_METRIC_DEFAULTS.items()
    }
    metrics: dict[str, JSONScalar] = {
        "immutable_anchor_violation_count": int(
            maxima["immutable_anchor_violation_count"]
        ),
        "max_event_count_relative_error": maxima["event_count_relative_error"],
        "candidate_failure_count": missing_cell_count,
        "max_interarrival_hist_l1": maxima["interarrival_hist_l1"],
        "max_path_realized_variation_relative_error": maxima[
            "path_realized_variation_relative_error"
        ],
        "refusal_rate_reported": True,
        "max_spread_tail_relative_error": maxima["spread_tail_relative_error"],
        "triangle_residual_p99_pips": maxima["triangle_residual_p99_pips"],
        "uncertainty_interval_count": sum(
            bool(values.get(name)) for name in _UNCERTAINTY_METRICS
        ),
        "unsupported_context_emission_count": int(
            maxima["unsupported_context_emission_count"]
        ),
        "max_update_transition_l1": maxima["update_transition_l1"],
    }
    observations = tuple(
        BenchmarkGateObservationV1(
            scope=BenchmarkGateScope.CANDIDATE,
            subject_id=benchmark_candidate_id,
            metric_name=name,
            value=value,
            evidence_ids=(trace.trace_id, campaign.campaign_id),
        )
        for name, value in sorted(metrics.items())
    )
    candidate_decision = evaluate_benchmark_promotion_gates(
        benchmark_policy,
        observations,
        scope=BenchmarkGateScope.CANDIDATE,
        subject_id=benchmark_candidate_id,
    )
    return ReleaseHoldoutGateReportV1(
        authorization_id=authorization.authorization_id,
        manifest_id=manifest.manifest_id,
        graph_id=graph.graph_id,
        scientific_candidate_id=graph.candidate_id,
        release_candidate_id=candidate.candidate_id,
        evaluation_policy_id=policy.policy_id,
        corpus_id=corpus.corpus_id,
        campaign_id=campaign.campaign_id,
        metric_trace_id=trace.trace_id,
        benchmark_candidate_id=benchmark_candidate_id,
        config_id=config.config_id,
        fit_id=fit.fit_id,
        window_id_map=window_map,
        ensemble_member_ids=authorization.ensemble_member_ids,
        candidate_metrics=metrics,
        campaign_decision=campaign.campaign_gate_decision,
        candidate_decision=candidate_decision,
        corpus_ref=authorization.corpus_ref,
        campaign_ref=campaign_ref,
        metric_trace_ref=metric_trace_ref,
        outcome=_derived_outcome(
            campaign.campaign_gate_decision, candidate_decision
        ),
    )


def execute_reconstruction_release_holdout_once(
    authorization: ReconstructionReleaseHoldoutAuthorizationV1,
    state_directory: str | Path,
    evaluator: Callable[
        [
            ProtectedReleaseHoldoutManifestV1,
            ReleaseCandidateFreezeV1,
            ReconstructionReleaseCandidateV1,
            ReverseDegradationBenchmarkCorpusV1,
            ReleaseHoldoutEvaluationPolicyV1,
        ],
        ArtifactRef,
    ],
    *,
    evaluated_at_utc: str | None = None,
) -> tuple[ReconstructionReleaseHoldoutReceiptV1, ArtifactRef]:
    """Reserve before evaluation and accept only a rebuilt gate report."""
    manifest, graph, candidate, policy, corpus = _authorization_inputs(
        authorization
    )
    root = Path(state_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(manifest.manifest_id.encode("utf-8")).hexdigest()
    state_path = root / f"release-holdout-access-{key}.json"
    reservation = _json_line(
        {
            "authorization_id": authorization.authorization_id,
            "manifest_id": manifest.manifest_id,
            "release_candidate_id": candidate.candidate_id,
            "state": "opened-and-consumed",
        }
    )
    _reserve_once(state_path, reservation)
    try:
        report_ref = evaluator(manifest, graph, candidate, corpus, policy)
        if not isinstance(report_ref, ArtifactRef):
            raise TypeError("release-holdout evaluator returned another type")
        report = read_release_holdout_gate_report(_verify_ref(report_ref))
        rebuilt = build_release_holdout_gate_report(
            authorization, report.campaign_ref, report.metric_trace_ref
        )
        if report != rebuilt:
            raise ValueError("release-holdout gate report is not reproducible")
        campaign = read_reverse_degradation_benchmark_campaign(
            report.campaign_ref.path
        )
        if _timestamp_value(campaign.started_at_utc) <= _timestamp_value(
            authorization.authorized_at_utc
        ):
            raise ValueError("release-holdout campaign predates authorization")
        completed = _timestamp_value(campaign.completed_at_utc)
        if completed < _timestamp_value(campaign.started_at_utc):
            raise ValueError(
                "release-holdout campaign completion predates start"
            )
        evaluated = _timestamp(
            evaluated_at_utc or datetime.now(timezone.utc).isoformat()
        )
        if _timestamp_value(evaluated) < completed:
            raise ValueError("release-holdout receipt predates campaign")
        receipt = ReconstructionReleaseHoldoutReceiptV1(
            authorization_id=authorization.authorization_id,
            manifest_id=manifest.manifest_id,
            graph_id=graph.graph_id,
            scientific_candidate_id=graph.candidate_id,
            release_candidate_id=candidate.candidate_id,
            outcome=report.outcome,
            evaluated_at_utc=evaluated,
            report_id=report.report_id,
            report_ref=report_ref,
        )
    except Exception as error:  # noqa: BLE001 - holdout remains consumed
        evaluated = _timestamp(
            evaluated_at_utc or datetime.now(timezone.utc).isoformat()
        )
        receipt = ReconstructionReleaseHoldoutReceiptV1(
            authorization_id=authorization.authorization_id,
            manifest_id=manifest.manifest_id,
            graph_id=graph.graph_id,
            scientific_candidate_id=graph.candidate_id,
            release_candidate_id=candidate.candidate_id,
            outcome=ReleaseHoldoutEvaluationOutcome.OPERATIONAL_FAILURE,
            evaluated_at_utc=evaluated,
            operational_error_type=type(error).__name__,
        )
    receipt_ref = write_reconstruction_release_holdout_receipt(receipt, root)
    _atomic_replace(
        state_path,
        _json_line(
            {
                "authorization_id": authorization.authorization_id,
                "manifest_id": manifest.manifest_id,
                "release_candidate_id": candidate.candidate_id,
                "receipt_ref": receipt_ref.to_dict(),
                "state": "retirement-required",
            }
        ),
    )
    return receipt, receipt_ref


def retire_reconstruction_release_holdout(
    manifest: ProtectedReleaseHoldoutManifestV1,
    receipt: ReconstructionReleaseHoldoutReceiptV1,
    *,
    retired_at_utc: str,
    successor_manifest_id: str = "",
) -> ReconstructionReleaseHoldoutRetirementV1:
    """Retire the consumed holdout; every non-pass requires a successor."""
    if receipt.manifest_id != manifest.manifest_id:
        raise ValueError("release-holdout retirement receipt is stale")
    if receipt.report_ref is not None:
        _verify_ref(receipt.report_ref)
    return ReconstructionReleaseHoldoutRetirementV1(
        manifest_id=manifest.manifest_id,
        release_candidate_id=receipt.release_candidate_id,
        receipt_id=receipt.receipt_id,
        outcome=receipt.outcome,
        retired_at_utc=retired_at_utc,
        successor_manifest_id=successor_manifest_id,
    )


def write_release_holdout_evaluation_policy(
    policy: ReleaseHoldoutEvaluationPolicyV1, output_directory: str | Path
) -> ArtifactRef:
    return _write_contract(
        policy.to_json(),
        output_directory,
        prefix="release-holdout-evaluation-policy",
        kind="release_holdout_evaluation_policy_v1",
        metadata={"policy_id": policy.policy_id},
    )


def read_release_holdout_evaluation_policy(
    path: str | Path,
) -> ReleaseHoldoutEvaluationPolicyV1:
    return ReleaseHoldoutEvaluationPolicyV1.from_dict(
        _read_contract(path, "release-holdout-evaluation-policy")
    )


def write_reconstruction_release_holdout_authorization(
    authorization: ReconstructionReleaseHoldoutAuthorizationV1,
    output_directory: str | Path,
) -> ArtifactRef:
    return _write_contract(
        authorization.to_json(),
        output_directory,
        prefix="reconstruction-release-holdout-authorization",
        kind="reconstruction_release_holdout_authorization_v1",
        metadata={
            "authorization_id": authorization.authorization_id,
            "release_candidate_id": authorization.release_candidate_id,
        },
    )


def read_reconstruction_release_holdout_authorization(
    path: str | Path,
) -> ReconstructionReleaseHoldoutAuthorizationV1:
    return ReconstructionReleaseHoldoutAuthorizationV1.from_dict(
        _read_contract(path, "reconstruction-release-holdout-authorization")
    )


def write_release_holdout_gate_report(
    report: ReleaseHoldoutGateReportV1, output_directory: str | Path
) -> ArtifactRef:
    return _write_contract(
        report.to_json(),
        output_directory,
        prefix="release-holdout-gate-report",
        kind="release_holdout_gate_report_v1",
        metadata={
            "report_id": report.report_id,
            "release_candidate_id": report.release_candidate_id,
            "outcome": report.outcome.value,
        },
    )


def read_release_holdout_gate_report(
    path: str | Path,
) -> ReleaseHoldoutGateReportV1:
    return ReleaseHoldoutGateReportV1.from_dict(
        _read_contract(path, "release-holdout-gate-report")
    )


def write_reconstruction_release_holdout_receipt(
    receipt: ReconstructionReleaseHoldoutReceiptV1,
    output_directory: str | Path,
) -> ArtifactRef:
    return _write_contract(
        receipt.to_json(),
        output_directory,
        prefix="reconstruction-release-holdout-receipt",
        kind="reconstruction_release_holdout_receipt_v1",
        metadata={
            "receipt_id": receipt.receipt_id,
            "release_candidate_id": receipt.release_candidate_id,
            "outcome": receipt.outcome.value,
        },
    )


def read_reconstruction_release_holdout_receipt(
    path: str | Path,
) -> ReconstructionReleaseHoldoutReceiptV1:
    return ReconstructionReleaseHoldoutReceiptV1.from_dict(
        _read_contract(path, "reconstruction-release-holdout-receipt")
    )


def write_reconstruction_release_holdout_retirement(
    marker: ReconstructionReleaseHoldoutRetirementV1,
    output_directory: str | Path,
) -> ArtifactRef:
    return _write_contract(
        marker.to_json(),
        output_directory,
        prefix="reconstruction-release-holdout-retirement",
        kind="reconstruction_release_holdout_retirement_v1",
        metadata={
            "marker_id": marker.marker_id,
            "release_candidate_id": marker.release_candidate_id,
        },
    )


def read_reconstruction_release_holdout_retirement(
    path: str | Path,
) -> ReconstructionReleaseHoldoutRetirementV1:
    return ReconstructionReleaseHoldoutRetirementV1.from_dict(
        _read_contract(path, "reconstruction-release-holdout-retirement")
    )


def _authorization_inputs(
    authorization: ReconstructionReleaseHoldoutAuthorizationV1,
) -> tuple[
    ProtectedReleaseHoldoutManifestV1,
    ReleaseCandidateFreezeV1,
    ReconstructionReleaseCandidateV1,
    ReleaseHoldoutEvaluationPolicyV1,
    ReverseDegradationBenchmarkCorpusV1,
]:
    if not isinstance(
        authorization, ReconstructionReleaseHoldoutAuthorizationV1
    ):
        raise TypeError("release-holdout authorization must use v1")
    manifest = read_protected_release_holdout_manifest(
        _verify_ref(authorization.manifest_ref)
    )
    graph = read_release_candidate_freeze(_verify_ref(authorization.graph_ref))
    candidate = read_reconstruction_release_candidate(
        _verify_ref(authorization.release_candidate_ref)
    )
    policy = read_release_holdout_evaluation_policy(
        _verify_ref(authorization.evaluation_policy_ref)
    )
    corpus = read_reverse_degradation_benchmark_corpus(
        _verify_ref(authorization.corpus_ref)
    )
    members = _candidate_holdout_policy_members(candidate, policy, corpus)
    if members != authorization.ensemble_member_ids:
        raise ValueError("release-holdout authorization member set differs")
    _validate_candidate_links(
        manifest,
        authorization.manifest_ref,
        graph,
        authorization.graph_ref,
        candidate,
        authorization.release_candidate_ref,
        policy,
        authorization.evaluation_policy_ref,
        corpus,
        authorization.corpus_ref,
    )
    _verify_git_evidence(
        Path(authorization.repository_root),
        authorization.evidence_git_commit_sha,
        candidate,
        graph,
        (
            (authorization.manifest_git_path, authorization.manifest_ref),
            (
                authorization.evaluation_policy_git_path,
                authorization.evaluation_policy_ref,
            ),
            (authorization.corpus_git_path, authorization.corpus_ref),
        ),
    )
    expected = {
        "manifest_id": manifest.manifest_id,
        "graph_id": graph.graph_id,
        "scientific_candidate_id": graph.candidate_id,
        "release_candidate_id": candidate.candidate_id,
        "evaluation_policy_id": policy.policy_id,
        "corpus_id": corpus.corpus_id,
    }
    if any(
        getattr(authorization, name) != value
        for name, value in expected.items()
    ):
        raise ValueError("release-holdout authorization binding differs")
    return manifest, graph, candidate, policy, corpus


def _validate_candidate_links(
    manifest: ProtectedReleaseHoldoutManifestV1,
    manifest_ref: ArtifactRef,
    graph: ReleaseCandidateFreezeV1,
    graph_ref: ArtifactRef,
    candidate: ReconstructionReleaseCandidateV1,
    candidate_ref: ArtifactRef,
    policy: ReleaseHoldoutEvaluationPolicyV1,
    policy_ref: ArtifactRef,
    corpus: ReverseDegradationBenchmarkCorpusV1,
    corpus_ref: ArtifactRef,
) -> None:
    del candidate_ref, policy_ref
    if graph.manifest_id != manifest.manifest_id:
        raise ValueError("release candidate graph uses another holdout")
    if graph.selection_dossier_id != manifest.selection_dossier_id:
        raise ValueError("release candidate selection dossier is stale")
    dependencies = {item.name: item for item in candidate.dependencies}
    exact = {
        "benchmark_corpus": (corpus.corpus_id, corpus_ref),
        "candidate_graph": (graph.graph_id, graph_ref),
        "protected_release_holdout": (manifest.manifest_id, manifest_ref),
    }
    for name, (artifact_id, ref) in exact.items():
        dependency = dependencies[name]
        if dependency.artifact_id != artifact_id or not _same_ref(
            dependency.artifact_ref, ref
        ):
            raise ValueError(f"release candidate {name} binding differs")
    selection = dependencies["product_selection_dossier"]
    if selection.artifact_id != graph.selection_dossier_id or not _same_ref(
        selection.artifact_ref, manifest.selection_dossier_ref
    ):
        raise ValueError(
            "release candidate product selection differs from graph"
        )
    if (
        corpus.gate_policy_id != policy.benchmark_gate_policy_id
        or corpus.gate_policy_commit != policy.benchmark_gate_policy_commit
    ):
        raise ValueError("release-holdout corpus gate policy differs")
    registry_ref = dependencies["certification_policy"].artifact_ref
    registry = _json_mapping(Path(_verify_ref(registry_ref)).read_text())
    entries = _mapping(registry.get("entries"))
    certification = _mapping(entries.get("certification_policy"))
    payload = _mapping(certification.get("payload"))
    expected = {
        "release_holdout_evaluation_policy_id": policy.policy_id,
        "release_holdout_corpus_id": corpus.corpus_id,
        "benchmark_gate_policy_id": policy.benchmark_gate_policy_id,
        "benchmark_gate_policy_commit": policy.benchmark_gate_policy_commit,
    }
    if any(payload.get(name) != value for name, value in expected.items()):
        raise ValueError("candidate release-holdout policy binding differs")


def _candidate_holdout_policy_members(
    candidate: ReconstructionReleaseCandidateV1,
    policy: ReleaseHoldoutEvaluationPolicyV1,
    corpus: ReverseDegradationBenchmarkCorpusV1,
) -> tuple[str, ...]:
    registry_ref = candidate.dependency("certification_policy").artifact_ref
    registry = _json_mapping(Path(_verify_ref(registry_ref)).read_text())
    payload = _mapping(
        _mapping(
            _mapping(registry.get("entries")).get("certification_policy")
        ).get("payload")
    )
    members = _text_tuple(
        _string_tuple(payload.get("release_holdout_ensemble_member_ids"))
    )
    if len(members) < policy.minimum_ensemble_member_count:
        raise ValueError("candidate release-holdout ensemble is underpowered")
    if members != _text_tuple(corpus.profile.ensemble_member_ids):
        raise ValueError("candidate and corpus release-holdout members differ")
    return members


def _verify_git_evidence(
    root: Path,
    commit: str,
    candidate: ReconstructionReleaseCandidateV1,
    graph: ReleaseCandidateFreezeV1,
    evidence: Sequence[tuple[str, ArtifactRef]],
) -> None:
    if _git(root, "config", "--get", "remote.origin.url") != (
        candidate.git_identity.repository_url
    ):
        raise ValueError("release-holdout repository identity differs")
    if not _git_success(
        root,
        "merge-base",
        "--is-ancestor",
        commit,
        candidate.git_identity.commit_sha,
    ):
        raise ValueError("release-holdout evidence commit is not an ancestor")
    committed_at = _timestamp(_git(root, "show", "-s", "--format=%cI", commit))
    if _timestamp_value(graph.frozen_at_utc) <= _timestamp_value(committed_at):
        raise ValueError("candidate graph predates committed holdout evidence")
    for git_path, ref in evidence:
        payload = _git_bytes(root, "show", f"{commit}:{git_path}")
        if (
            len(payload) != ref.size_bytes
            or hashlib.sha256(payload).hexdigest() != ref.sha256
        ):
            raise ValueError(f"committed holdout evidence differs: {git_path}")


def _exact_benchmark_candidate(
    candidate: ReconstructionReleaseCandidateV1,
    members: Sequence[str],
) -> tuple[MarkedHawkesConfigV1, MarkedHawkesFitResultV1, BenchmarkCandidateV1]:
    config_ref = candidate.dependency("selected_engine_config").artifact_ref
    fit_ref = candidate.dependency("selected_engine_fit").artifact_ref
    config = MarkedHawkesConfigV1.from_json(
        Path(_verify_ref(config_ref)).read_text(encoding="utf-8")
    )
    fit = MarkedHawkesFitResultV1.from_json(
        Path(_verify_ref(fit_ref)).read_text(encoding="utf-8")
    )
    benchmark_candidate = build_marked_hawkes_benchmark_candidate(
        config, fit, ensemble_member_ids=members
    )
    return config, fit, benchmark_candidate


def _holdout_window_map(
    manifest: ProtectedReleaseHoldoutManifestV1,
    corpus: ReverseDegradationBenchmarkCorpusV1,
) -> dict[str, str]:
    corpus_windows = tuple(
        item for item in corpus.windows if item.split_kind == "final_holdout"
    )
    by_interval = {
        (item.period, item.start_ns, item.end_ns): item
        for item in corpus_windows
    }
    if len(by_interval) != len(corpus_windows):
        raise ValueError("release-holdout corpus intervals duplicate")
    mapping: dict[str, str] = {}
    sources = {item.partition_id: item for item in corpus.sources}
    for window in manifest.windows:
        corpus_window = by_interval.get(
            (window.period, window.start_ns, window.end_ns)
        )
        if corpus_window is None:
            raise ValueError("release-holdout corpus window is missing")
        counts = {
            key.lower(): value
            for key, value in window.symbol_event_counts.items()
        }
        corpus_counts = {
            key.lower(): value
            for key, value in corpus_window.symbol_event_counts.items()
        }
        if counts != corpus_counts:
            raise ValueError("release-holdout corpus event counts differ")
        if window.session_stratum != corpus_window.session:
            raise ValueError("release-holdout corpus session differs")
        if window.epoch_stratum != corpus_window.epoch_label:
            raise ValueError("release-holdout corpus epoch differs")
        selected_sources = tuple(
            sources[item] for item in corpus_window.source_partition_ids
        )
        hashes = {
            key.lower(): value
            for key, value in corpus_window.symbol_partition_sha256.items()
        }
        if {
            key.lower(): value for key, value in window.source_hashes.items()
        } != hashes:
            raise ValueError("release-holdout corpus window hashes differ")
        partition_ids = tuple(
            sorted(
                f"{item.partition_id}#window:"
                f"{corpus_window.start_ns}:{corpus_window.end_ns}"
                for item in selected_sources
            )
        )
        if window.source_partition_ids != partition_ids:
            raise ValueError("release-holdout corpus window partitions differ")
        mapping[window.window_id] = corpus_window.window_id
    if len(mapping) != len(corpus_windows):
        raise ValueError(
            "release-holdout corpus contains foreign final windows"
        )
    return dict(sorted(mapping.items()))


def _derived_outcome(
    campaign: BenchmarkPromotionDecisionV1,
    candidate: BenchmarkPromotionDecisionV1,
) -> ReleaseHoldoutEvaluationOutcome:
    decisions = (campaign, candidate)
    if all(item.promotion_eligible for item in decisions):
        return ReleaseHoldoutEvaluationOutcome.PASSED
    if any(
        check.blocking and check.status is BenchmarkGateStatus.MISSING
        for decision in decisions
        for check in decision.checks
    ):
        return ReleaseHoldoutEvaluationOutcome.INSUFFICIENT_EVIDENCE
    return ReleaseHoldoutEvaluationOutcome.FAILED


def _relative_git_path(root: Path, path: str) -> str:
    target = Path(path).expanduser().resolve()
    try:
        relative = target.relative_to(root)
    except ValueError as error:
        raise ValueError(
            "holdout evidence is outside the repository"
        ) from error
    return _safe_git_path(relative.as_posix())


def _safe_git_path(value: str) -> str:
    normalized = str(PurePosixPath(_required_text(value)))
    path = PurePosixPath(normalized)
    if path.is_absolute() or ".." in path.parts or normalized in {".", ""}:
        raise ValueError("release-holdout Git path is unsafe")
    return normalized


def _same_ref(left: ArtifactRef, right: ArtifactRef) -> bool:
    return (
        left.kind == right.kind
        and Path(left.path).expanduser().resolve()
        == Path(right.path).expanduser().resolve()
        and left.size_bytes == right.size_bytes
        and left.sha256 == right.sha256
    )


def _verify_ref(ref: ArtifactRef) -> Path:
    _require_strong_ref(ref)
    path = Path(ref.path).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"release-holdout artifact is missing: {path}")
    if path.stat().st_size != ref.size_bytes:
        raise ValueError(f"release-holdout artifact size differs: {path}")
    if _file_sha256(path) != ref.sha256:
        raise ValueError(f"release-holdout artifact hash differs: {path}")
    return path


def _require_strong_ref(ref: ArtifactRef) -> None:
    _required_text(ref.kind)
    if not Path(_required_text(ref.path)).expanduser().is_absolute():
        raise ValueError("release-holdout artifact path is relative")
    if isinstance(ref.size_bytes, bool) or not isinstance(ref.size_bytes, int):
        raise TypeError("release-holdout artifact size is absent")
    if ref.size_bytes < 0:
        raise ValueError("release-holdout artifact size is negative")
    _sha256(ref.sha256)


def _write_contract(
    text: str,
    output_directory: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    payload = (text + "\n").encode("utf-8")
    if len(payload) > MAX_RELEASE_HOLDOUT_EVALUATION_ARTIFACT_BYTES:
        raise ValueError("release-holdout evaluation artifact exceeds limit")
    digest = hashlib.sha256(payload).hexdigest()
    directory = Path(output_directory).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}-{digest}.json"
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError("release-holdout evaluation artifact collision")
    else:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=len(payload),
        sha256=digest,
        metadata=dict(metadata),
    )


def _read_contract(path: str | Path, prefix: str) -> Mapping[str, Any]:
    target = Path(path).expanduser()
    payload = target.read_bytes()
    if len(payload) > MAX_RELEASE_HOLDOUT_EVALUATION_ARTIFACT_BYTES:
        raise ValueError("release-holdout evaluation artifact exceeds limit")
    digest = hashlib.sha256(payload).hexdigest()
    if target.name != f"{prefix}-{digest}.json":
        raise ValueError("release-holdout artifact is not content addressed")
    return _mapping(json.loads(payload))


def _reserve_once(path: Path, payload: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as error:
        raise ReleaseHoldoutAlreadyConsumedError(
            "release holdout was already opened or reserved"
        ) from error
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _atomic_replace(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _json_line(payload: Mapping[str, JSONValue]) -> bytes:
    return str(canonical_contract_json(payload)).encode("utf-8") + b"\n"


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode:
        raise ValueError(
            "release-holdout Git inspection failed: " + completed.stderr.strip()
        )
    return completed.stdout.strip()


def _git_bytes(root: Path, *arguments: str) -> bytes:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        raise ValueError(
            "release-holdout Git evidence is missing: "
            + completed.stderr.decode("utf-8", errors="replace").strip()
        )
    return completed.stdout


def _git_success(root: Path, *arguments: str) -> bool:
    return (
        subprocess.run(
            ("git", "-C", str(root), *arguments),
            capture_output=True,
            check=False,
        ).returncode
        == 0
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _timestamp(value: str) -> str:
    parsed = _timestamp_value(value)
    return parsed.isoformat().replace("+00:00", "Z")


def _timestamp_value(value: str) -> datetime:
    normalized = _required_text(value)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("release-holdout timestamp is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("release-holdout timestamp must include timezone")
    return parsed.astimezone(timezone.utc)


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError("release-holdout JSON is invalid") from error
    return _mapping(value)


def _json_scalar(value: Any, name: str) -> JSONScalar:
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    raise ValueError(f"release-holdout {name} is not scalar")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("release-holdout object is invalid")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("release-holdout sequence is invalid")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _text_tuple(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({_required_text(value) for value in values}))
    if not normalized or len(normalized) > MAX_RELEASE_HOLDOUT_EVALUATION_ITEMS:
        raise ValueError("release-holdout text set is invalid")
    return normalized


def _required_text(value: Any) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("release-holdout text is required")
    return normalized


def _sha256(value: str) -> str:
    normalized = _required_text(value).lower()
    if _SHA256.fullmatch(normalized) is None:
        raise ValueError("release-holdout SHA-256 is invalid")
    return normalized


def _git_object_id(value: str) -> str:
    normalized = _required_text(value).lower()
    if _GIT_OBJECT_ID.fullmatch(normalized) is None:
        raise ValueError("release-holdout Git object ID is invalid")
    return normalized


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"release-holdout {name} is invalid")
    return value


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"release-holdout {name} is invalid")
    return value


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported release-holdout schema: {actual!r}")


__all__ = [
    "RECONSTRUCTION_RELEASE_HOLDOUT_AUTHORIZATION_SCHEMA_VERSION",
    "RECONSTRUCTION_RELEASE_HOLDOUT_RECEIPT_SCHEMA_VERSION",
    "RECONSTRUCTION_RELEASE_HOLDOUT_RETIREMENT_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_EVALUATION_POLICY_SCHEMA_VERSION",
    "RELEASE_HOLDOUT_GATE_REPORT_SCHEMA_VERSION",
    "ReconstructionReleaseHoldoutAuthorizationV1",
    "ReconstructionReleaseHoldoutReceiptV1",
    "ReconstructionReleaseHoldoutRetirementV1",
    "ReleaseHoldoutEvaluationPolicyV1",
    "ReleaseHoldoutGateReportV1",
    "authorize_reconstruction_release_holdout",
    "build_release_holdout_gate_report",
    "execute_reconstruction_release_holdout_once",
    "load_default_release_holdout_evaluation_policy",
    "read_reconstruction_release_holdout_authorization",
    "read_reconstruction_release_holdout_receipt",
    "read_reconstruction_release_holdout_retirement",
    "read_release_holdout_evaluation_policy",
    "read_release_holdout_gate_report",
    "retire_reconstruction_release_holdout",
    "write_reconstruction_release_holdout_authorization",
    "write_reconstruction_release_holdout_receipt",
    "write_reconstruction_release_holdout_retirement",
    "write_release_holdout_evaluation_policy",
    "write_release_holdout_gate_report",
]
