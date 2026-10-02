"""Declarative exact-candidate plans and closed, independently assessed results."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar

from histdatacom.broker_plugins import BrokerConfigurationSchemaV1

from ._wire import Artifact, canonical_conformance_json, parse_conformance_json

BROKER_CONFORMANCE_VERSION = "2.0.0"
CONFORMANCE_NONCLAIM = "software_contract_only_not_provider_authority_quality_or_scientific_qualification"


def atom(value: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_.-]{0,127}", value):
        raise ValueError("invalid conformance atom")


def identity(value: str, prefix: str | None = None) -> None:
    pattern = r"[a-z][a-z0-9-]*" if prefix is None else re.escape(prefix)
    if not re.fullmatch(pattern + r":sha256:[a-f0-9]{64}", value):
        raise ValueError("invalid conformance identity")


def ordered(values: tuple[str, ...]) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError("duplicate or unordered conformance inventory")


class BrokerConformanceProfile(str, Enum):
    TRUSTED = "trusted_contract_v1"
    ISOLATED = "isolated_contract_v1"
    DUAL = "dual_contract_v1"


class BrokerConformanceSubject(str, Enum):
    CANDIDATE = "installed_candidate"
    REFERENCE = "host_reference_selftest"


class BrokerConformanceStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"
    ERROR = "error"
    UNSUPPORTED = "unsupported"
    NOT_RUN = "not_run"


class BrokerConformanceReason(str, Enum):
    VERIFIED = "independently_verified"
    CONTRACT = "contract_mismatch"
    DRIVER = "scenario_driver_unavailable"
    PLATFORM = "execution_backend_unsupported"
    CAPABILITY = "capability_unsupported"
    TIMEOUT = "runner_deadline"
    CANCELLED = "run_cancelled"
    EXECUTION = "unexpected_execution_failure"
    EVIDENCE = "missing_or_invalid_evidence"
    IDENTITY = "installed_identity_changed"
    AUTHORIZATION = "operator_authorization_required"
    PROVENANCE = "provenance_boundary_unavailable"


@dataclass(frozen=True, slots=True)
class BrokerConformanceScenarioV1(Artifact):
    scenario_id: str
    configuration_json: str
    KIND: ClassVar[str] = "scenario"

    def _validate(self) -> None:
        atom(self.scenario_id)
        value = parse_conformance_json(self.configuration_json)
        if len(value) > 32 or any(
            type(item) not in (str, int, float, bool) for item in value.values()
        ):
            raise ValueError("conformance configuration must be primitive")
        if canonical_conformance_json(value) != self.configuration_json:
            raise ValueError("noncanonical scenario configuration")


@dataclass(frozen=True, slots=True)
class BrokerConformanceDriverV1(Artifact):
    """Setup only: no callable, expected verdict, or replacement factory."""

    candidate_id: str
    permission_manifest_id: str
    provider_id: str
    configuration_schema_json: str
    symbols: tuple[str, ...]
    scenarios: tuple[BrokerConformanceScenarioV1, ...]
    fixture_abi: str = "canonical-fx-v1"
    KIND: ClassVar[str] = "driver"

    def _validate(self) -> None:
        identity(self.candidate_id, "broker-plugin-candidate")
        identity(self.permission_manifest_id, "broker-permission-manifest")
        atom(self.provider_id)
        schema = BrokerConfigurationSchemaV1.from_json(
            self.configuration_schema_json
        )
        if (
            schema.to_json() != self.configuration_schema_json
            or self.fixture_abi != "canonical-fx-v1"
        ):
            raise ValueError("unknown conformance fixture ABI")
        ordered(self.symbols)
        if self.symbols != ("EURUSD",):
            raise ValueError("SDK-v1 fixture profile requires declared EURUSD")
        ordered(tuple(item.scenario_id for item in self.scenarios))
        if not self.scenarios or len(self.scenarios) > 128:
            raise ValueError("conformance scenario inventory bound")
        for scenario in self.scenarios:
            schema.validate_configuration(
                parse_conformance_json(scenario.configuration_json)
            )


@dataclass(frozen=True, slots=True)
class BrokerConformanceCaseV1(Artifact):
    case_id: str
    capability: str
    scenario_id: str
    profiles: tuple[str, ...]
    execution_backend: str = "selected_profile_runtime"
    KIND: ClassVar[str] = "case"

    def _validate(self) -> None:
        if self.execution_backend not in {
            "selected_profile_runtime",
            "host_admission",
            "trusted_probe",
            "paired_runtime",
            "reference_host_fault",
        }:
            raise ValueError("unknown required conformance backend")
        for value in (self.case_id, self.capability, self.scenario_id):
            atom(value)
        ordered(self.profiles)
        if not self.profiles or set(self.profiles) - {
            item.value for item in BrokerConformanceProfile
        }:
            raise ValueError("unknown conformance execution profile")


@dataclass(frozen=True, slots=True)
class BrokerConformanceCatalogV1(Artifact):
    cases: tuple[BrokerConformanceCaseV1, ...]
    version: str = BROKER_CONFORMANCE_VERSION
    KIND: ClassVar[str] = "catalog"

    def _validate(self) -> None:
        ordered(tuple(item.case_id for item in self.cases))
        if not self.cases or self.version != BROKER_CONFORMANCE_VERSION:
            raise ValueError("invalid conformance catalog")


@dataclass(frozen=True, slots=True)
class BrokerConformancePlanV1(Artifact):
    driver: BrokerConformanceDriverV1
    catalog_id: str
    profile: BrokerConformanceProfile
    capabilities: tuple[str, ...]
    case_ids: tuple[str, ...]
    subject: BrokerConformanceSubject = BrokerConformanceSubject.CANDIDATE
    case_timeout_ms: int = 300_000
    KIND: ClassVar[str] = "plan"

    def _validate(self) -> None:
        identity(self.catalog_id, "broker-conformance-catalog")
        ordered(self.capabilities)
        ordered(self.case_ids)
        if not self.capabilities or not self.case_ids:
            raise ValueError("empty conformance scope cannot certify")
        for value in self.capabilities + self.case_ids:
            atom(value)
        if not 1000 <= self.case_timeout_ms <= 600_000:
            raise ValueError("conformance deadline bound")


@dataclass(frozen=True, slots=True)
class BrokerConformanceCaseResultV1(Artifact):
    case_id: str
    status: BrokerConformanceStatus
    reason: BrokerConformanceReason
    subject: BrokerConformanceSubject
    evidence_ids: tuple[str, ...] = ()
    execution_backend: str = "unexecuted"
    KIND: ClassVar[str] = "case-result"

    def _validate(self) -> None:
        if self.execution_backend not in {
            "unexecuted",
            "host_admission",
            "metadata_only",
            "trusted",
            "trusted_probe",
            "isolated",
            "paired_runtime",
            "reference_host_fault",
        }:
            raise ValueError("unknown actual conformance backend")
        atom(self.case_id)
        ordered(self.evidence_ids)
        for value in self.evidence_ids:
            identity(value)
        if (self.status is BrokerConformanceStatus.PASS) != (
            self.reason is BrokerConformanceReason.VERIFIED
        ):
            raise ValueError("conformance status/reason contradiction")
        if (
            self.status is BrokerConformanceStatus.PASS
            and not self.evidence_ids
        ):
            raise ValueError("passing case requires executed evidence")


@dataclass(frozen=True, slots=True)
class BrokerConformanceCapabilityV1(Artifact):
    capability: str
    required_cases: tuple[str, ...]
    passed_cases: tuple[str, ...]
    status: BrokerConformanceStatus
    KIND: ClassVar[str] = "capability"

    def _validate(self) -> None:
        atom(self.capability)
        ordered(self.required_cases)
        ordered(self.passed_cases)
        if not set(self.passed_cases) <= set(self.required_cases):
            raise ValueError("unexpected conformance case")
        if self.status is BrokerConformanceStatus.PASS and (
            not self.required_cases or self.required_cases != self.passed_cases
        ):
            raise ValueError("required conformance cases not all passed")


@dataclass(frozen=True, slots=True)
class BrokerConformanceReportV1(Artifact):
    plan: BrokerConformancePlanV1
    results: tuple[BrokerConformanceCaseResultV1, ...]
    capabilities: tuple[BrokerConformanceCapabilityV1, ...]
    nonclaim: str = CONFORMANCE_NONCLAIM
    KIND: ClassVar[str] = "report"

    def _validate(self) -> None:
        if tuple(item.case_id for item in self.results) != self.plan.case_ids:
            raise ValueError("report omitted or reordered planned case")
        if any(item.subject is not self.plan.subject for item in self.results):
            raise ValueError(
                "reference selftest cannot certify another subject"
            )
        if (
            tuple(item.capability for item in self.capabilities)
            != self.plan.capabilities
            or self.nonclaim != CONFORMANCE_NONCLAIM
        ):
            raise ValueError("conformance scope changed")
        from .assessment import assess_capabilities

        if self.capabilities != assess_capabilities(self.plan, self.results):
            raise ValueError("capability assessment contradicts actual cases")

    @property
    def certified(self) -> bool:
        return self.plan.subject is BrokerConformanceSubject.CANDIDATE and all(
            item.status is BrokerConformanceStatus.PASS
            for item in self.capabilities
        )
