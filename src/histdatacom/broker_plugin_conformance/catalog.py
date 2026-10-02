"""Frozen required case inventory; callers cannot certify a reduced test set."""

from __future__ import annotations

from .contracts import (
    BrokerConformanceCaseV1,
    BrokerConformanceCatalogV1,
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
)

_ALL = tuple(sorted(item.value for item in BrokerConformanceProfile))
_WORKER = tuple(
    sorted(
        (
            BrokerConformanceProfile.ISOLATED.value,
            BrokerConformanceProfile.DUAL.value,
        )
    )
)
HOST_ADMISSION_CASES = frozenset(
    (
        "permissions.required-denial",
        "permissions.revocation",
        "policy.invoke-denial",
        "policy.retention-denial",
        "policy.publication-denial",
    )
)
_ROWS = (
    ("discovery.metadata", "common", "finite", _ALL),
    ("discovery.sdk", "common", "finite", _ALL),
    ("permissions.declaration", "common", "finite", _ALL),
    ("permissions.required-denial", "common", "finite", _ALL),
    ("permissions.revocation", "common", "finite", _ALL),
    ("policy.invoke-denial", "common", "finite", _ALL),
    ("policy.retention-denial", "common", "finite", _WORKER),
    ("policy.publication-denial", "common", "finite", _ALL),
    ("lifecycle.finite", "common", "finite", _ALL),
    ("lifecycle.exception", "common", "exception", _ALL),
    ("lifecycle.malformed", "common", "malformed", _ALL),
    ("lifecycle.cancellation", "common", "next-block", _WORKER),
    ("lifecycle.forced-shutdown", "common", "close-block", _WORKER),
    ("lifecycle.reconnect", "common", "reconnect", _WORKER),
    ("queue.backpressure", "common", "burst", _WORKER),
    ("queue.overflow", "host-adversarial.v1", "overflow", _WORKER),
    ("secrets.redaction", "common", "secret-leak", _ALL),
    ("replay.determinism", "common", "finite", _ALL),
    ("session.freshness", "common", "finite", _ALL),
    ("provenance.replay", "common", "finite", _WORKER),
    ("provenance.mutations", "host-adversarial.v1", "finite", _WORKER),
    (
        "execution.equivalence",
        "common",
        "finite",
        (BrokerConformanceProfile.DUAL.value,),
    ),
    ("quotes.symbols", "quotes.v1", "finite", _ALL),
    ("quotes.decimals", "quotes.v1", "finite", _ALL),
    ("quotes.invalid-spread", "quotes.v1", "invalid-spread", _ALL),
    ("quotes.duplicate", "quotes.v1", "duplicate", _ALL),
    ("quotes.unchanged", "quotes.v1", "unchanged", _ALL),
    ("quotes.stale", "quotes.v1", "stale", _WORKER),
    ("quotes.sequence", "quotes.v1", "bad-sequence", _ALL),
    ("timestamps.source", "timestamps.broker-event.v1", "finite", _ALL),
    ("timestamps.receive", "timestamps.receive.v1", "finite", _ALL),
    ("timestamps.monotonic", "timestamps.receive.v1", "bad-clock", _ALL),
    ("health.honest", "health.v1", "honest-health", _WORKER),
    ("health.false-duplicate", "health.v1", "healthy-duplicate", _WORKER),
    ("health.false-gap", "health.v1", "healthy-gap", _WORKER),
    ("health.false-reorder", "health.v1", "healthy-reorder", _WORKER),
    ("health.false-delay", "health.v1", "healthy-delay", _WORKER),
    ("health.unknown-upstream", "health.v1", "finite", _WORKER),
    ("heartbeat.delivery", "heartbeat.v1", "heartbeat", _ALL),
    ("gaps.delivery", "gaps.v1", "gap", _ALL),
    ("sizes.quoted", "sizes.quoted.v1", "sizes", _ALL),
    ("sizes.permission-denial", "sizes.quoted.v1", "sizes", _ALL),
    ("raw.hashes", "raw-hashes.v1", "raw", _ALL),
    ("raw.permission-denial", "raw-hashes.v1", "raw", _ALL),
)


def broker_conformance_catalog() -> BrokerConformanceCatalogV1:
    probes = {
        "lifecycle.exception",
        "lifecycle.malformed",
        "secrets.redaction",
        "quotes.invalid-spread",
        "quotes.sequence",
        "timestamps.monotonic",
        "sizes.permission-denial",
        "raw.permission-denial",
    }
    return BrokerConformanceCatalogV1(
        tuple(
            BrokerConformanceCaseV1(
                *row,
                execution_backend=(
                    "host_admission"
                    if row[0] in HOST_ADMISSION_CASES
                    else (
                        "trusted_probe"
                        if row[0] in probes
                        else (
                            "paired_runtime"
                            if row[0]
                            in {
                                "execution.equivalence",
                                "replay.determinism",
                                "session.freshness",
                            }
                            else (
                                "reference_host_fault"
                                if row[0]
                                in {"queue.overflow", "provenance.mutations"}
                                else "selected_profile_runtime"
                            )
                        )
                    )
                ),
            )
            for row in sorted(_ROWS)
        )
    )


def required_cases(
    profile: BrokerConformanceProfile, capability: str
) -> tuple[str, ...]:
    catalog = broker_conformance_catalog()
    own = [
        case
        for case in catalog.cases
        if case.capability == capability and profile.value in case.profiles
    ]
    if not own:
        return ()
    return tuple(
        sorted(
            case.case_id
            for case in catalog.cases
            if profile.value in case.profiles
            and case.capability in ("common", capability)
        )
    )


def verify_plan(plan: BrokerConformancePlanV1) -> None:
    catalog = broker_conformance_catalog()
    expected = {
        case.case_id
        for case in catalog.cases
        if case.capability == "common" and plan.profile.value in case.profiles
    } | {
        case
        for capability in plan.capabilities
        for case in required_cases(plan.profile, capability)
    }
    if plan.catalog_id != catalog.artifact_id or plan.case_ids != tuple(
        sorted(expected)
    ):
        raise ValueError("conformance plan changed required catalog inventory")
