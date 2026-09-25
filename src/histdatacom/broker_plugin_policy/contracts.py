"""Declared provider-rights evidence, never automated legal interpretation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import ClassVar

from histdatacom.broker_plugins.contracts import (
    _check_extension_keys,
    _check_json,
    _semver,
    _text,
)

from ._wire import Artifact, Record, canonical_json, digest, load_json, ordered

POLICY_NONCLAIM = (
    "declared provider-policy enforcement is not legal advice, provider "
    "authorization verification, or scientific qualification"
)


class BrokerPolicyDataClass(str, Enum):
    PRIVATE_ACCOUNT = "credentials_or_private_account_metadata"
    RAW_PAYLOAD = "raw_provider_payloads"
    NORMALIZED_QUOTES = "normalized_canonical_quotes"
    CONTENT_HASHES = "content_hashes"
    HEALTH = "bounded_health_metadata"
    FINGERPRINTS = "derived_features_or_statistical_fingerprints"
    BROKER_SYNTHETIC = "broker_conditioned_synthetic_products"


class BrokerPolicyOperation(str, Enum):
    INVOKE = "invoke"
    CAPTURE = "capture"
    MATERIAL_USE = "material_use"
    DERIVE = "derive"
    RETAIN_LOCAL = "retain_local"
    REDISTRIBUTE = "redistribute"
    PUBLISH = "publish"


class BrokerPolicyStatus(str, Enum):
    ALLOWED = "allowed"
    DENIED = "denied"
    UNKNOWN = "unknown"


class BrokerPolicyEvidenceKind(str, Enum):
    DECLARED = "operator_reviewed_declaration_not_legal_verification"
    SYNTHETIC = "synthetic_conformance_evidence_not_provider_authorization"


class BrokerPolicyConstraintMode(str, Enum):
    ANY = "explicitly_unrestricted"
    ONLY = "only_declared_values"
    UNKNOWN = "unknown"


class BrokerPolicyRetention(str, Enum):
    UNKNOWN = "unknown"
    UNBOUNDED = "explicitly_unbounded"
    FINITE = "finite_maximum"
    NOT_APPLICABLE = "not_applicable"


def _ns(value: int) -> None:
    if value < 0:
        raise ValueError("policy clock must be nonnegative int64")


def _interval(start: int, end: int | None) -> None:
    _ns(start)
    if end is not None and end <= start:
        raise ValueError("policy interval must be nonempty half-open")


def _texts(values: tuple[str, ...], *, nonempty: bool = False) -> None:
    ordered(values, nonempty=nonempty)
    for value in values:
        _text(value, "policy text", 4096)


@dataclass(frozen=True, slots=True)
class BrokerPolicyReferenceV1(Record):
    kind: str
    native_id: str
    sha256: str
    byte_length: int

    def _validate(self) -> None:
        _text(self.kind, "reference kind")
        _text(self.native_id, "reference identity", 4096)
        digest(self.sha256)
        if self.byte_length <= 0:
            raise ValueError(
                "policy reference requires positive declared bytes"
            )


@dataclass(frozen=True, slots=True)
class BrokerPolicyBindingV1(Artifact):
    KIND: ClassVar[str] = "binding"
    provider_id: str
    native_binding_kind: str
    native_binding_json: str

    def _validate(self) -> None:
        _text(self.provider_id, "provider")
        _text(self.native_binding_kind, "binding kind")
        value = load_json(self.native_binding_json)
        if type(value) is not dict or not value:
            raise ValueError(
                "native binding must be a nonempty canonical object"
            )
        _check_json(value)
        _check_extension_keys(value)


@dataclass(frozen=True, slots=True)
class BrokerPolicyEvidenceV1(Artifact):
    KIND: ClassVar[str] = "terms-evidence"
    reference: BrokerPolicyReferenceV1
    provider_id: str
    terms_identity: str
    terms_version: str
    terms_date: str
    issuer: str
    reviewed_at_ns: int
    evidence_kind: BrokerPolicyEvidenceKind
    locator: str

    def _validate(self) -> None:
        for value in (
            self.provider_id,
            self.terms_identity,
            self.terms_version,
            self.issuer,
            self.locator,
        ):
            _text(value, "policy evidence", 4096)
        try:
            if (
                date.fromisoformat(self.terms_date).isoformat()
                != self.terms_date
            ):
                raise ValueError
        except ValueError as exc:
            raise ValueError("terms date requires canonical ISO date") from exc
        _ns(self.reviewed_at_ns)


@dataclass(frozen=True, slots=True)
class BrokerPolicyConstraintV1(Record):
    name: str
    mode: BrokerPolicyConstraintMode
    values: tuple[str, ...] = ()

    def _validate(self) -> None:
        if self.name not in (
            "commercial_use",
            "geography",
            "account_class",
            "feed_type",
            "eligibility",
        ):
            raise ValueError("unknown provider constraint")
        _texts(self.values)
        if bool(self.values) != (self.mode is BrokerPolicyConstraintMode.ONLY):
            raise ValueError(
                "only constraints require explicit nonempty values"
            )
        if self.name == "commercial_use" and not set(self.values) <= {
            "commercial",
            "noncommercial",
        }:
            raise ValueError("unknown commercial-use condition")


@dataclass(frozen=True, slots=True)
class BrokerPolicyAttributionV1(Record):
    attribution_id: str
    text: str

    def _validate(self) -> None:
        _text(self.attribution_id, "attribution identity")
        _text(self.text, "attribution text", 4096)


@dataclass(frozen=True, slots=True)
class BrokerPolicyRuleV1(Record):
    operation: BrokerPolicyOperation
    data_class: BrokerPolicyDataClass
    status: BrokerPolicyStatus
    evidence_ids: tuple[str, ...] = ()
    retention: BrokerPolicyRetention = BrokerPolicyRetention.NOT_APPLICABLE
    maximum_retention_ns: int | None = None
    required_attribution_ids: tuple[str, ...] = ()

    def _validate(self) -> None:
        _texts(self.evidence_ids)
        _texts(self.required_attribution_ids)
        if self.status is BrokerPolicyStatus.ALLOWED and not self.evidence_ids:
            raise ValueError("allowed rights require exact retained evidence")
        if self.operation is BrokerPolicyOperation.RETAIN_LOCAL:
            if self.retention is BrokerPolicyRetention.NOT_APPLICABLE:
                raise ValueError(
                    "local retention must explicitly state duration semantics"
                )
        elif self.retention is not BrokerPolicyRetention.NOT_APPLICABLE:
            raise ValueError(
                "retention duration applies only to local retention"
            )
        if self.retention is BrokerPolicyRetention.FINITE:
            if (
                self.maximum_retention_ns is None
                or self.maximum_retention_ns <= 0
            ):
                raise ValueError(
                    "finite retention requires a positive duration"
                )
        elif self.maximum_retention_ns is not None:
            raise ValueError("unexpected retention duration")


@dataclass(frozen=True, slots=True)
class BrokerProviderPolicyV1(Artifact):
    KIND: ClassVar[str] = "manifest"
    version: str
    binding: BrokerPolicyBindingV1
    host_software_license: str
    plugin_software_license: str | None
    evidence_ids: tuple[str, ...]
    rules: tuple[BrokerPolicyRuleV1, ...]
    constraints: tuple[BrokerPolicyConstraintV1, ...]
    attributions: tuple[BrokerPolicyAttributionV1, ...]
    declared_at_ns: int
    effective_from_ns: int
    expires_at_ns: int | None
    predecessor_id: str | None = None
    nonclaim: str = POLICY_NONCLAIM

    def _validate(self) -> None:
        _semver(self.version)
        if self.host_software_license != "MIT":
            raise ValueError(
                "host software license is MIT, not provider rights"
            )
        if self.plugin_software_license is not None:
            _text(self.plugin_software_license, "plugin software license", 4096)
        _texts(self.evidence_ids, nonempty=True)
        expected = {
            (operation, data_class)
            for operation in BrokerPolicyOperation
            for data_class in BrokerPolicyDataClass
        }
        actual = tuple((r.operation, r.data_class) for r in self.rules)
        if (
            len(actual) != len(expected)
            or set(actual) != expected
            or actual != tuple(sorted(actual))
        ):
            raise ValueError(
                "policy requires exact sorted complete49-cell rights matrix"
            )
        if any(
            not set(r.evidence_ids) <= set(self.evidence_ids)
            for r in self.rules
        ):
            raise ValueError("rule evidence is not in the manifest inventory")
        if tuple(c.name for c in self.constraints) != (
            "account_class",
            "commercial_use",
            "eligibility",
            "feed_type",
            "geography",
        ):
            raise ValueError(
                "policy requires all five sorted eligibility constraints"
            )
        _texts(tuple(a.attribution_id for a in self.attributions))
        available = {a.attribution_id for a in self.attributions}
        if any(
            not set(r.required_attribution_ids) <= available for r in self.rules
        ):
            raise ValueError("rule lacks retained attribution text")
        _ns(self.declared_at_ns)
        _interval(self.effective_from_ns, self.expires_at_ns)
        if self.predecessor_id is not None:
            _text(self.predecessor_id, "predecessor", 4096)
        if self.nonclaim != POLICY_NONCLAIM:
            raise ValueError(
                "policy cannot claim legal/scientific qualification"
            )


@dataclass(frozen=True, slots=True)
class BrokerPolicyAcknowledgementV1(Artifact):
    KIND: ClassVar[str] = "acknowledgement"
    policy_id: str
    evidence_ids: tuple[str, ...]
    operator_reference: BrokerPolicyReferenceV1
    acknowledged_at_ns: int
    expires_at_ns: int | None

    def _validate(self) -> None:
        _text(self.policy_id, "acknowledged policy", 4096)
        _texts(self.evidence_ids, nonempty=True)
        _interval(self.acknowledged_at_ns, self.expires_at_ns)


@dataclass(frozen=True, slots=True)
class BrokerPolicyRevocationV1(Artifact):
    KIND: ClassVar[str] = "revocation"
    target_id: str
    recorded_at_ns: int
    effective_at_ns: int
    evidence_ids: tuple[str, ...]
    reason: str

    def _validate(self) -> None:
        _text(self.target_id, "revoked identity", 4096)
        _ns(self.recorded_at_ns)
        _ns(self.effective_at_ns)
        _texts(self.evidence_ids, nonempty=True)
        if self.reason not in (
            "withdrawn",
            "terms_changed",
            "review_invalidated",
            "configuration_changed",
        ):
            raise ValueError("unknown policy revocation reason")


@dataclass(frozen=True, slots=True)
class BrokerPolicyExecutionV1(Record):
    commercial_use: bool
    geography: str | None
    account_class: str | None
    feed_type: str | None
    eligibility_assertions: tuple[str, ...] = ()
    attribution_ids: tuple[str, ...] = ()

    def _validate(self) -> None:
        for value in (self.geography, self.account_class, self.feed_type):
            if value is not None:
                _text(value, "execution constraint", 4096)
        _texts(self.eligibility_assertions)
        _texts(self.attribution_ids)


@dataclass(frozen=True, slots=True)
class BrokerPolicyContextV1(Artifact):
    KIND: ClassVar[str] = "review-context"
    policies: tuple[BrokerProviderPolicyV1, ...]
    evidence: tuple[BrokerPolicyEvidenceV1, ...]
    acknowledgements: tuple[BrokerPolicyAcknowledgementV1, ...]
    revocations: tuple[BrokerPolicyRevocationV1, ...]
    selected_policy_ids: tuple[str, ...]
    execution: BrokerPolicyExecutionV1

    def _validate(self) -> None:
        for records in (
            self.policies,
            self.evidence,
            self.acknowledgements,
            self.revocations,
        ):
            if len(records) > 128:
                raise ValueError(
                    "policy review inventory exceeds128 records per family"
                )
            _texts(tuple(r.artifact_id for r in records))
        _texts(self.selected_policy_ids, nonempty=True)
        if len(self.selected_policy_ids) > 16:
            raise ValueError("at most16 provider bindings per closed operation")
        from .decisions import validate_review_context

        validate_review_context(self)


@dataclass(frozen=True, slots=True)
class BrokerPolicySubjectV1(Artifact):
    """Metadata-only projection; only closed native resolvers qualify its use."""

    KIND: ClassVar[str] = "subject"
    native_ref: BrokerPolicyReferenceV1
    bindings: tuple[BrokerPolicyBindingV1, ...]
    data_classes: tuple[BrokerPolicyDataClass, ...]
    evidence_kind: BrokerPolicyEvidenceKind = BrokerPolicyEvidenceKind.DECLARED

    def _validate(self) -> None:
        if not 1 <= len(self.bindings) <= 16:
            raise ValueError(
                "subject requires1..16 exact native provider bindings"
            )
        _texts(tuple(b.artifact_id for b in self.bindings), nonempty=True)
        if not self.data_classes or self.data_classes != tuple(
            sorted(set(self.data_classes))
        ):
            raise ValueError(
                "subject requires exact sorted nonempty class closure"
            )


@dataclass(frozen=True, slots=True)
class BrokerPolicyRequestV1(Artifact):
    KIND: ClassVar[str] = "operation-request"
    subject: BrokerPolicySubjectV1
    operation: BrokerPolicyOperation
    decision_at_ns: int
    intended_retention_deadline_ns: int | None = None
    recipient_scope: str | None = None

    def _validate(self) -> None:
        _ns(self.decision_at_ns)
        if self.intended_retention_deadline_ns is not None:
            if (
                self.operation is not BrokerPolicyOperation.RETAIN_LOCAL
                or self.intended_retention_deadline_ns <= self.decision_at_ns
            ):
                raise ValueError(
                    "retention deadline requires a future local-retention interval"
                )
        if self.recipient_scope is not None:
            _text(self.recipient_scope, "recipient scope", 4096)
            if self.operation not in (
                BrokerPolicyOperation.REDISTRIBUTE,
                BrokerPolicyOperation.PUBLISH,
            ):
                raise ValueError(
                    "recipient scope applies only to dissemination"
                )


@dataclass(frozen=True, slots=True)
class BrokerPolicyCellDecisionV1(Record):
    binding_id: str
    policy_id: str | None
    data_class: BrokerPolicyDataClass
    status: BrokerPolicyStatus
    reasons: tuple[str, ...]
    evidence_ids: tuple[str, ...]
    acknowledgement_ids: tuple[str, ...]
    required_attribution_ids: tuple[str, ...]
    valid_until_ns: int | None

    def _validate(self) -> None:
        _text(self.binding_id, "decision binding", 4096)
        if self.policy_id is not None:
            _text(self.policy_id, "decision policy", 4096)
        for values in (
            self.reasons,
            self.evidence_ids,
            self.acknowledgement_ids,
            self.required_attribution_ids,
        ):
            _texts(values)
        if (self.status is BrokerPolicyStatus.ALLOWED) != (not self.reasons):
            raise ValueError("only allowed decisions have no refusal reasons")
        if self.valid_until_ns is not None:
            _ns(self.valid_until_ns)


@dataclass(frozen=True, slots=True)
class BrokerPolicyDecisionV1(Artifact):
    KIND: ClassVar[str] = "decision"
    context: BrokerPolicyContextV1
    request: BrokerPolicyRequestV1
    cells: tuple[BrokerPolicyCellDecisionV1, ...]
    status: BrokerPolicyStatus
    valid_until_ns: int | None
    nonclaim: str = POLICY_NONCLAIM

    def _validate(self) -> None:
        from .decisions import _decision_values

        if self.nonclaim != POLICY_NONCLAIM:
            raise ValueError("decision cannot claim legal authority")
        expected = _decision_values(self.context, self.request)
        if (self.cells, self.status, self.valid_until_ns) != expected:
            raise ValueError(
                "policy decision differs from exact declared-evidence replay"
            )

    @property
    def allowed(self) -> bool:
        return self.status is BrokerPolicyStatus.ALLOWED


def policy_reference(artifact: Artifact) -> BrokerPolicyReferenceV1:
    from ._wire import sha256

    value = artifact.to_json()
    return BrokerPolicyReferenceV1(
        artifact.schema_version(),
        artifact.artifact_id,
        sha256(value),
        len(value),
    )


def canonical_policy_json(value: object) -> str:
    return canonical_json(value)
