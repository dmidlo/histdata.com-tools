"""Bounded historical report metadata, never current-kit certification.

The ordinary Report reader deliberately assesses against today's frozen kit.
Compatibility replay instead supplies its exact retained catalog. This reader
does not create a current Report, execute evidence, or grant any authority.
"""

from __future__ import annotations

import hashlib
from typing import NamedTuple

from ._wire import canonical_conformance_json, parse_conformance_json
from .assessment import _assess_capabilities
from .contracts import (
    CONFORMANCE_NONCLAIM,
    BrokerConformanceCapabilityV1,
    BrokerConformanceCaseResultV1,
    BrokerConformanceCatalogV1,
    BrokerConformancePlanV1,
)


class RetainedConformanceReport(NamedTuple):
    """Validated report components without a current `certified` interface."""

    plan: BrokerConformancePlanV1
    results: tuple[BrokerConformanceCaseResultV1, ...]
    capabilities: tuple[BrokerConformanceCapabilityV1, ...]


def read_retained_conformance_report(
    text: str, catalog: BrokerConformanceCatalogV1
) -> RetainedConformanceReport:
    """Check exact V1 wire, identities and full retained-denominator assessment."""
    if type(catalog) is not BrokerConformanceCatalogV1:
        raise ValueError("exact retained conformance catalog required")
    catalog = BrokerConformanceCatalogV1.from_json(catalog.to_json())
    value = parse_conformance_json(text)
    if (
        set(value)
        != {
            "schema_version",
            "artifact_id",
            "plan",
            "results",
            "capabilities",
            "nonclaim",
        }
        or value["schema_version"] != "histdatacom.broker-conformance.report.v1"
    ):
        raise ValueError("invalid retained conformance report envelope")
    if (
        type(value["plan"]) is not dict
        or type(value["results"]) is not list
        or type(value["capabilities"]) is not list
        or value["nonclaim"] != CONFORMANCE_NONCLAIM
    ):
        raise ValueError("invalid retained conformance report fields")
    plan = BrokerConformancePlanV1.from_dict(value["plan"])
    results = tuple(
        BrokerConformanceCaseResultV1.from_dict(item)
        for item in value["results"]
    )
    capabilities = tuple(
        BrokerConformanceCapabilityV1.from_dict(item)
        for item in value["capabilities"]
    )
    if capabilities != _assess_capabilities(plan, results, catalog):
        raise ValueError("retained capability assessment contradicts cases")
    payload = {
        "schema_version": "histdatacom.broker-conformance.report.v1",
        "plan": plan.to_dict(),
        "results": [item.to_dict() for item in results],
        "capabilities": [item.to_dict() for item in capabilities],
        "nonclaim": CONFORMANCE_NONCLAIM,
    }
    expected_id = (
        "broker-conformance-report:sha256:"
        + hashlib.sha256(
            canonical_conformance_json(payload).encode("ascii")
        ).hexdigest()
    )
    if (
        canonical_conformance_json({**payload, "artifact_id": expected_id})
        != text
    ):
        raise ValueError("retained report identity or canonical bytes changed")
    return RetainedConformanceReport(plan, results, capabilities)
