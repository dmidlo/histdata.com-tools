"""Installable generated-input broker software-contract conformance kit."""

from .catalog import broker_conformance_catalog
from .contracts import (
    BROKER_CONFORMANCE_VERSION,
    CONFORMANCE_NONCLAIM,
    BrokerConformanceCapabilityV1,
    BrokerConformanceCaseResultV1,
    BrokerConformanceCaseV1,
    BrokerConformanceCatalogV1,
    BrokerConformanceDriverV1,
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
    BrokerConformanceReason,
    BrokerConformanceReportV1,
    BrokerConformanceScenarioV1,
    BrokerConformanceStatus,
    BrokerConformanceSubject,
)
from .drivers import inspect_broker_conformance_driver, plan_broker_conformance
from .evidence import BrokerConformanceEvidenceV1
from .fixtures import (
    BrokerConformanceFixtureV1,
    build_broker_conformance_fixture,
)
from .runner import run_broker_conformance
from .storage import verify_broker_conformance

__all__ = [
    "BROKER_CONFORMANCE_VERSION",
    "CONFORMANCE_NONCLAIM",
    "BrokerConformanceCapabilityV1",
    "BrokerConformanceCaseResultV1",
    "BrokerConformanceCaseV1",
    "BrokerConformanceCatalogV1",
    "BrokerConformanceDriverV1",
    "BrokerConformanceEvidenceV1",
    "BrokerConformanceFixtureV1",
    "BrokerConformancePlanV1",
    "BrokerConformanceProfile",
    "BrokerConformanceReason",
    "BrokerConformanceReportV1",
    "BrokerConformanceScenarioV1",
    "BrokerConformanceStatus",
    "BrokerConformanceSubject",
    "broker_conformance_catalog",
    "build_broker_conformance_fixture",
    "inspect_broker_conformance_driver",
    "plan_broker_conformance",
    "run_broker_conformance",
    "verify_broker_conformance",
]
