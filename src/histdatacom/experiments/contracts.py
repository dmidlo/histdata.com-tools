"""Public experiment interoperability; private instances remain external."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, cast

from ._wire import (
    Artifact,
    Record,
    canonical_json,
    digest,
    identifier,
    identity,
    load_json,
    ordered,
    sha256,
    text,
)


class EvidenceKind(str, Enum):
    SYNTHETIC_FIXTURE = "synthetic_fixture_not_scientific_qualification"
    DECLARED_EXTERNAL = "declared_external_unverified"


@dataclass(frozen=True, slots=True)
class ArtifactReferenceV1(Record):
    """Exact content reference, not evidence of authenticity or availability."""

    schema: str
    native_id: str
    sha256: str
    byte_length: int
    evidence_kind: EvidenceKind

    def _validate(self) -> None:
        text(self.schema)
        text(self.native_id)
        digest(self.sha256)
        if not 0 < self.byte_length < 2**63:
            raise ValueError("reference byte bound")


@dataclass(frozen=True, slots=True)
class RetainedFixtureV1(Artifact):
    """Retained canonical fixture bytes; native schemas use their real reader.

    Generic fixture specifications are deliberately NOT native source proofs.
    External references cannot be promoted to verified evidence by this class.
    """

    KIND: ClassVar[str] = "fixture"
    reference: ArtifactReferenceV1
    payload_json: str

    def _validate(self) -> None:
        if self.reference.evidence_kind is not EvidenceKind.SYNTHETIC_FIXTURE:
            raise ValueError("only explicitly synthetic evidence is retained")
        payload = load_json(self.payload_json)
        # Account for the parsed payload together with the outer envelope.
        canonical_json({"reference": self.reference, "payload": payload})
        if (
            sha256(self.payload_json) != self.reference.sha256
            or len(self.payload_json) != self.reference.byte_length
        ):
            raise ValueError("retained fixture byte identity mismatch")
        if self.reference.schema == "histdatacom.experiment-fixture-spec.v1":
            if type(payload) is not dict or set(payload) != {"name", "value"}:
                raise ValueError("invalid explicit fixture specification")
            if self.reference.native_id != identity("fixture-spec", payload):
                raise ValueError("fixture specification identity mismatch")
        elif self.reference.schema == "histdatacom.synthetic-event-stream.v1":
            # Optional native integration is lazy; the public wire API itself
            # is stdlib-only and never opens datasets or runs a generator.
            from histdatacom.synthetic.contracts import SyntheticEventStreamV1

            if type(payload) is not dict:
                raise ValueError("expected native stream object")
            stream = SyntheticEventStreamV1.from_dict(payload)
            if canonical_json(stream.to_dict()) != self.payload_json:
                raise ValueError(
                    "noncanonical or unknown native fixture fields"
                )
            if self.reference.native_id != stream.stream_id:
                raise ValueError("native stream fixture identity mismatch")
        else:
            raise ValueError("unsupported retained fixture schema")


def fixture_specification(name: str, value: object) -> RetainedFixtureV1:
    """Create a declared test configuration, never a trained-model receipt."""
    text(name)
    payload = {"name": name, "value": value}
    encoded = canonical_json(payload)
    return RetainedFixtureV1(
        ArtifactReferenceV1(
            "histdatacom.experiment-fixture-spec.v1",
            identity("fixture-spec", payload),
            sha256(encoded),
            len(encoded),
            EvidenceKind.SYNTHETIC_FIXTURE,
        ),
        encoded,
    )


def retain_native_fixture(payload_json: str) -> RetainedFixtureV1:
    """Replay an actual native stream reader and retain its exact fixture bytes."""
    from histdatacom.synthetic.contracts import SyntheticEventStreamV1

    value = load_json(payload_json)
    if type(value) is not dict:
        raise ValueError("expected native stream object")
    stream = SyntheticEventStreamV1.from_dict(value)
    return RetainedFixtureV1(
        ArtifactReferenceV1(
            "histdatacom.synthetic-event-stream.v1",
            stream.stream_id,
            sha256(payload_json),
            len(payload_json),
            EvidenceKind.SYNTHETIC_FIXTURE,
        ),
        payload_json,
    )


class ComponentField(str, Enum):
    DATASET_PRODUCTS = "dataset_products"
    DATASET_LINEAGE = "dataset_lineage"
    SPLIT_POLICY = "split_policy"
    SPLIT_ARTIFACT = "split_artifact"
    FEATURE_REGISTRY = "feature_registry"
    FEATURE_PROJECTION = "feature_projection"
    PREPROCESSING = "preprocessing"
    LABEL = "label"
    UNCERTAINTY = "uncertainty"
    TRANSPORT = "transport"
    MODEL_ARCHITECTURE = "model_architecture"
    MODEL_CONFIGURATION = "model_configuration"
    MODEL_WEIGHTS = "model_weights"
    CALIBRATION = "calibration"
    DECISION_POLICY = "decision_policy"
    TRANSACTION_COST = "transaction_cost"
    FINANCING = "financing"
    MARGIN = "margin"
    EXECUTION = "execution"
    ACCOUNT_POLICY = "account_policy"


_REQUIRED = frozenset(
    {
        ComponentField.DATASET_PRODUCTS,
        ComponentField.DATASET_LINEAGE,
        ComponentField.SPLIT_POLICY,
        ComponentField.SPLIT_ARTIFACT,
        ComponentField.FEATURE_REGISTRY,
        ComponentField.FEATURE_PROJECTION,
        ComponentField.PREPROCESSING,
        ComponentField.LABEL,
        ComponentField.MODEL_ARCHITECTURE,
        ComponentField.MODEL_CONFIGURATION,
    }
)


@dataclass(frozen=True, slots=True)
class ScientificComponentV1(Record):
    field: ComponentField
    references: tuple[ArtifactReferenceV1, ...]
    not_applicable_reason: str | None = None

    def _validate(self) -> None:
        if not self.references:
            if self.field in _REQUIRED or self.not_applicable_reason is None:
                raise ValueError("missing scientific component")
            text(self.not_applicable_reason)
        elif self.not_applicable_reason is not None:
            raise ValueError("component cannot be present and not applicable")
        keys = tuple(canonical_json(ref) for ref in self.references)
        ordered(keys)


class ReplayClass(str, Enum):
    EXACT = "declared_exact_replay"
    NUMERICAL = "declared_numerical_replay_policy"
    NONDETERMINISTIC = "declared_nondeterministic"


@dataclass(frozen=True, slots=True)
class RandomnessV1(Record):
    semantic_namespace: str
    seeds: tuple[int, ...]
    replay_class: ReplayClass
    replay_policy: ArtifactReferenceV1

    def _validate(self) -> None:
        text(self.semantic_namespace)
        if len(self.seeds) > 256 or any(seed < 0 for seed in self.seeds):
            raise ValueError("invalid bounded seed inventory")
        if self.seeds != tuple(sorted(set(self.seeds))):
            raise ValueError("seeds must be sorted unique")


@dataclass(frozen=True, slots=True)
class EnvironmentV1(Record):
    repository_commit: str
    repository_tree: str
    runtime: ArtifactReferenceV1
    lock: ArtifactReferenceV1
    sbom: ArtifactReferenceV1

    def _validate(self) -> None:
        for revision in (self.repository_commit, self.repository_tree):
            if len(revision) not in (40, 64):
                raise ValueError("expected exact Git commit/tree")
            digest(revision.zfill(64))


class MetricDirection(str, Enum):
    MINIMIZE = "minimize"
    MAXIMIZE = "maximize"
    DESCRIPTIVE = "descriptive"


@dataclass(frozen=True, slots=True)
class MetricDefinitionV1(Artifact):
    KIND: ClassVar[str] = "metric"
    name: str
    semantic_version: str
    formula: str
    direction: MetricDirection
    units: str
    weighting: str
    missing_censored: str
    aggregation: str
    uncertainty_method: str

    def _validate(self) -> None:
        for value in (
            self.name,
            self.semantic_version,
            self.formula,
            self.units,
            self.weighting,
            self.missing_censored,
            self.aggregation,
            self.uncertainty_method,
        ):
            text(value)
        parts = self.semantic_version.split(".")
        if len(parts) != 3 or any(
            not p.isdigit() or str(int(p)) != p for p in parts
        ):
            raise ValueError(
                "metric semantic version must be canonical SemVer core"
            )


@dataclass(frozen=True, slots=True)
class MetricRegistryV1(Artifact):
    KIND: ClassVar[str] = "metric-registry"
    version: str
    metrics: tuple[MetricDefinitionV1, ...]

    def _validate(self) -> None:
        text(self.version)
        ordered(tuple(metric.name for metric in self.metrics), nonempty=True)


@dataclass(frozen=True, slots=True)
class ScientificInputsV1(Record):
    hypothesis_id: str
    fit_policy: ArtifactReferenceV1
    components: tuple[ScientificComponentV1, ...]
    environment: EnvironmentV1
    randomness: RandomnessV1
    metric_registry_id: str
    reporting_strata: tuple[str, ...]
    research_cycle_id: str
    protected_evaluation_roots: tuple[str, ...]
    evaluates_trading_actions: bool
    search_plan_id: str

    def _validate(self) -> None:
        text(self.hypothesis_id)
        text(self.research_cycle_id)
        identifier(self.search_plan_id, "search-plan")
        identifier(self.metric_registry_id, "metric-registry")
        ordered(self.reporting_strata, nonempty=True)
        ordered(self.protected_evaluation_roots)
        names = tuple(component.field.value for component in self.components)
        if names != tuple(sorted(field.value for field in ComponentField)):
            raise ValueError(
                "complete sorted scientific component inventory required"
            )
        if self.evaluates_trading_actions:
            for component in self.components:
                if (
                    component.field
                    in {
                        ComponentField.DECISION_POLICY,
                        ComponentField.TRANSACTION_COST,
                        ComponentField.FINANCING,
                        ComponentField.MARGIN,
                        ComponentField.EXECUTION,
                        ComponentField.ACCOUNT_POLICY,
                    }
                    and not component.references
                ):
                    raise ValueError(
                        "trading evaluation requires all action/cost/risk policies"
                    )

    def references(self) -> tuple[ArtifactReferenceV1, ...]:
        return (
            self.fit_policy,
            self.environment.runtime,
            self.environment.lock,
            self.environment.sbom,
            self.randomness.replay_policy,
            *(
                ref
                for component in self.components
                for ref in component.references
            ),
        )


@dataclass(frozen=True, slots=True)
class ExperimentBundleV1(Artifact):
    """Scientific identity is separate from full-envelope/display identity.

    Attempts, results, statuses and promotion links are append-only registry
    records. Changing those never rewrites this immutable input declaration.
    """

    KIND: ClassVar[str] = "bundle"
    scientific_inputs: ScientificInputsV1
    display_title: str
    descriptive_branch: str

    def _validate(self) -> None:
        text(self.display_title)
        text(self.descriptive_branch)

    @property
    def experiment_id(self) -> str:
        return identity(
            "scientific", {"version": 1, "inputs": self.scientific_inputs}
        )


class AttemptStatus(str, Enum):
    RUNNING = "running"
    FAILED = "failed"
    COMPLETED = "completed"


@dataclass(frozen=True, slots=True)
class ResourceEnvelopeV1(Record):
    wall_seconds: int
    memory_bytes: int
    workers: int

    def _validate(self) -> None:
        if (
            self.wall_seconds <= 0
            or self.memory_bytes <= 0
            or not 0 < self.workers <= 4096
        ):
            raise ValueError("invalid declared resource envelope")


@dataclass(frozen=True, slots=True)
class ExperimentAttemptV1(Artifact):
    KIND: ClassVar[str] = "attempt"
    experiment_id: str
    bundle_id: str
    attempt_label: str
    status: AttemptStatus
    started_at_ns: int
    ended_at_ns: int | None
    runtime_identity: ArtifactReferenceV1
    worker_identity: str
    resources: ResourceEnvelopeV1
    logs: tuple[ArtifactReferenceV1, ...]
    checkpoints: tuple[ArtifactReferenceV1, ...]
    produced_artifacts: tuple[ArtifactReferenceV1, ...]
    retry_of_attempt_id: str | None
    reason: str | None
    retry_reason: str | None = None
    previous_state_id: str | None = None

    def _validate(self) -> None:
        identifier(self.experiment_id, "scientific")
        identifier(self.bundle_id, "bundle")
        text(self.attempt_label)
        text(self.worker_identity)
        if self.started_at_ns < 0 or (
            self.ended_at_ns is not None
            and self.ended_at_ns < self.started_at_ns
        ):
            raise ValueError("invalid attempt clock order")
        if self.status is AttemptStatus.RUNNING:
            if self.ended_at_ns is not None or self.reason is not None:
                raise ValueError("running attempt has terminal fields")
        elif self.ended_at_ns is None:
            raise ValueError("terminal attempt needs end clock")
        if self.status is AttemptStatus.FAILED:
            if self.reason is None:
                raise ValueError("failure reason required")
        if self.reason is not None:
            text(self.reason)
        if self.retry_of_attempt_id is not None:
            identifier(self.retry_of_attempt_id, "attempt")
            if self.retry_reason is None:
                raise ValueError("retry rationale required")
            text(self.retry_reason)
        elif self.retry_reason is not None:
            raise ValueError("retry rationale without predecessor attempt")
        if self.previous_state_id is not None:
            identifier(self.previous_state_id, "attempt")
            if self.status is AttemptStatus.RUNNING:
                raise ValueError("running attempt cannot replace prior state")
        for values in (self.logs, self.checkpoints, self.produced_artifacts):
            ordered(tuple(canonical_json(value) for value in values))

    @property
    def attempt_id(self) -> str:
        """Stable logical attempt; artifact_id identifies its immutable state."""
        return identity(
            "run",
            {
                "experiment_id": self.experiment_id,
                "attempt_label": self.attempt_label,
            },
        )


class ResultState(str, Enum):
    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class MetricPayloadV1(Record):
    state: ResultState
    value: float | None
    support_units: int
    uncertainty_low: float | None
    uncertainty_high: float | None
    unavailable_reason: str | None

    def _validate(self) -> None:
        if self.support_units < 0:
            raise ValueError("negative metric support")
        if self.state is ResultState.AVAILABLE:
            if (
                self.value is None
                or self.support_units == 0
                or self.unavailable_reason is not None
            ):
                raise ValueError("available metric needs value and support")
        elif self.value is not None or self.unavailable_reason is None:
            raise ValueError(
                "unavailable metric must state reason and omit value"
            )
        if self.unavailable_reason is not None:
            text(self.unavailable_reason)
        if (self.uncertainty_low is None) != (self.uncertainty_high is None):
            raise ValueError("incomplete uncertainty interval")
        if self.uncertainty_low is not None:
            if (
                self.state is not ResultState.AVAILABLE
                or self.uncertainty_low > cast(float, self.uncertainty_high)
            ):
                raise ValueError("invalid uncertainty interval")


@dataclass(frozen=True, slots=True)
class ExperimentResultV1(Artifact):
    KIND: ClassVar[str] = "result"
    experiment_id: str
    metric_id: str
    stratum_id: str
    payload: MetricPayloadV1

    def _validate(self) -> None:
        identifier(self.experiment_id, "scientific")
        identifier(self.metric_id, "metric")
        text(self.stratum_id)

    @property
    def result_id(self) -> str:
        return identity(
            "metric-result",
            {
                "experiment_id": self.experiment_id,
                "metric_id": self.metric_id,
                "stratum_id": self.stratum_id,
                "output_payload_hash": sha256(canonical_json(self.payload)),
            },
        )


@dataclass(frozen=True, slots=True)
class ResultReceiptV1(Artifact):
    """Producer attribution stays separate from replay-invariant result identity."""

    KIND: ClassVar[str] = "result-receipt"
    result_id: str
    attempt_id: str
    artifact: ArtifactReferenceV1

    def _validate(self) -> None:
        identifier(self.result_id, "metric-result")
        identifier(self.attempt_id, "attempt")


def result_reference(
    result: ExperimentResultV1, kind: EvidenceKind
) -> ArtifactReferenceV1:
    encoded = result.to_json()
    return ArtifactReferenceV1(
        result.schema_version(),
        result.result_id,
        sha256(encoded),
        len(encoded),
        kind,
    )
