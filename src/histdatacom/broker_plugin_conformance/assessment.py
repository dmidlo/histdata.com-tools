"""Complete-denominator assessment, independent of candidate/driver verdicts."""

from __future__ import annotations

from .catalog import required_cases, verify_plan
from .contracts import (
    BrokerConformanceCapabilityV1,
    BrokerConformanceCaseResultV1,
    BrokerConformancePlanV1,
    BrokerConformanceReportV1,
)
from .contracts import (
    BrokerConformanceStatus as Status,
)


def assess_capabilities(
    plan: BrokerConformancePlanV1,
    results: tuple[BrokerConformanceCaseResultV1, ...],
) -> tuple[BrokerConformanceCapabilityV1, ...]:
    verify_plan(plan)
    if tuple(item.case_id for item in results) != plan.case_ids or any(
        item.subject is not plan.subject for item in results
    ):
        raise ValueError("conformance result inventory or subject differs")
    by_id = {item.case_id: item for item in results}
    values = []
    for capability in plan.capabilities:
        required = required_cases(plan.profile, capability)
        passed = tuple(
            case_id
            for case_id in required
            if by_id[case_id].status is Status.PASS
        )
        if not required:
            status = Status.UNSUPPORTED
        elif passed == required:
            status = Status.PASS
        else:
            statuses = {by_id[case_id].status for case_id in required}
            status = next(
                item
                for item in (
                    Status.FAIL,
                    Status.ERROR,
                    Status.UNSUPPORTED,
                    Status.NOT_RUN,
                )
                if item in statuses
            )
        values.append(
            BrokerConformanceCapabilityV1(capability, required, passed, status)
        )
    return tuple(values)


def assess_broker_conformance(
    plan: BrokerConformancePlanV1,
    results: tuple[BrokerConformanceCaseResultV1, ...],
) -> BrokerConformanceReportV1:
    """Assess already verified cases; persistence verification replays evidence."""
    return BrokerConformanceReportV1(
        plan, results, assess_capabilities(plan, results)
    )
