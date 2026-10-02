"""Bounded retained native execution evidence; physical clocks are preserved."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar

from ._wire import Artifact, parse_conformance_json
from .contracts import (
    BrokerConformanceReason,
    BrokerConformanceStatus,
    atom,
    identity,
    ordered,
)


@dataclass(frozen=True, slots=True)
class BrokerConformanceEvidenceV1(Artifact):
    plan_id: str
    case_id: str
    candidate_id: str
    started_at_ns: int
    stopped_at_ns: int
    inventory_json: str
    invocation_json: str
    permission_manifest_json: str
    permission_context_json: str
    provider_context_json: str
    native_family: str
    native_json: str = ""
    permission_execution_json: str = ""
    health_json: str = ""
    provenance_json: str = ""
    refusal_family: str = "none"
    refusal_reason: str = "none"
    refusal_decision_json: str = ""
    probe_json: str = ""
    event_failure: str = "none"
    capability_plan_json: str = ""
    KIND: ClassVar[str] = "evidence"

    def _validate(self) -> None:
        identity(self.plan_id, "broker-conformance-plan")
        identity(self.candidate_id, "broker-plugin-candidate")
        atom(self.case_id)
        if not 0 <= self.started_at_ns <= self.stopped_at_ns < 2**63:
            raise ValueError("invalid execution clocks")
        if self.native_family not in (
            "trusted",
            "trusted_probe",
            "host_admission",
            "isolated",
            "none",
        ):
            raise ValueError("unknown native conformance family")
        if self.refusal_family not in (
            "none",
            "permission",
            "policy",
            "security",
            "capability",
            "lifecycle",
            "execution",
        ):
            raise ValueError("unknown native refusal family")
        atom(self.refusal_reason)
        if self.event_failure not in {
            "none",
            "event_type",
            "event_contract",
            "invalid_spread",
            "session",
            "sequence",
            "clock_identity",
            "monotonic_regression",
            "utc_regression",
        }:
            raise ValueError("unknown host event failure")
        for name in (
            "inventory_json",
            "invocation_json",
            "permission_manifest_json",
            "permission_context_json",
            "provider_context_json",
            "native_json",
            "permission_execution_json",
            "health_json",
            "provenance_json",
            "refusal_decision_json",
            "probe_json",
            "capability_plan_json",
        ):
            value = getattr(self, name)
            if value:
                parse_conformance_json(value)
        if not self.inventory_json or (
            not self.invocation_json
            and not (
                self.native_family == "none"
                and self.refusal_family == "capability"
                and self.capability_plan_json
            )
        ):
            raise ValueError("native execution binding absent")
        if self.native_family == "none" and self.invocation_json:
            raise ValueError("metadata-only refusal cannot carry an invocation")


@dataclass(frozen=True, slots=True)
class BrokerConformanceProbeV1(Artifact):
    """Host-owned progress through the actual public installed SDK gate."""

    candidate_id: str
    stage: str
    metadata_json: str = ""
    binding_json: str = ""
    session_json: str = ""
    events_json: tuple[str, ...] = ()
    KIND: ClassVar[str] = "probe"

    def _validate(self) -> None:
        identity(self.candidate_id, "broker-plugin-candidate")
        if self.stage not in {
            "factory",
            "metadata",
            "configuration",
            "open",
            "instruments",
            "subscribe",
            "events",
            "unsubscribe",
            "close",
            "complete",
        }:
            raise ValueError("unknown conformance progress stage")
        if len(self.events_json) > 128:
            raise ValueError("conformance progress bound")
        for text in (
            self.metadata_json,
            self.binding_json,
            self.session_json,
            *self.events_json,
        ):
            if text:
                parse_conformance_json(text)


@dataclass(frozen=True, slots=True)
class BrokerConformanceRunnerOutcomeV1(Artifact):
    """A nonpassing interrupted/setup outcome, never positive execution proof."""

    plan_id: str
    case_id: str
    status: BrokerConformanceStatus
    reason: BrokerConformanceReason
    evidence_ids: tuple[str, ...] = ()
    KIND: ClassVar[str] = "runner-outcome"

    def _validate(self) -> None:
        identity(self.plan_id, "broker-conformance-plan")
        atom(self.case_id)
        if (
            self.status is BrokerConformanceStatus.PASS
            or self.reason is BrokerConformanceReason.VERIFIED
        ):
            raise ValueError("runner outcome cannot certify execution")
        ordered(self.evidence_ids)
        if len(self.evidence_ids) > 2:
            raise ValueError("runner evidence inventory bound")
        for value in self.evidence_ids:
            identity(value, "broker-conformance-evidence")
