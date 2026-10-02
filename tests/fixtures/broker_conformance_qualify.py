"""Clean-wheel public kit qualification, copied outside the checkout to run.

Requires an independently installed generated fixture and its exact driver.
No test imports, host mocks, private requester or source-path injection.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from histdatacom.broker_plugin_conformance import (
    BrokerConformanceDriverV1,
    BrokerConformanceProfile,
    BrokerConformanceSubject,
    plan_broker_conformance,
    run_broker_conformance,
    verify_broker_conformance,
)
from histdatacom.broker_plugin_registry import discover_broker_plugins


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--profile",
        choices=[item.value for item in BrokerConformanceProfile],
        required=True,
    )
    parser.add_argument("--reference-host", action="store_true")
    args = parser.parse_args()
    driver = BrokerConformanceDriverV1.from_json(args.driver.read_text("ascii"))
    profile = BrokerConformanceProfile(args.profile)
    capabilities = (
        "gaps.v1",
        "heartbeat.v1",
        "quotes.v1",
        "raw-hashes.v1",
        "sizes.quoted.v1",
        "timestamps.broker-event.v1",
        "timestamps.receive.v1",
    )
    if profile is not BrokerConformanceProfile.TRUSTED:
        capabilities = tuple(sorted((*capabilities, "health.v1")))
    if args.reference_host:
        capabilities = ("host-adversarial.v1",)
    plan = plan_broker_conformance(
        discover_broker_plugins(),
        driver,
        profile=profile,
        capabilities=capabilities,
        subject=(
            BrokerConformanceSubject.REFERENCE
            if args.reference_host
            else BrokerConformanceSubject.CANDIDATE
        ),
    )
    report = run_broker_conformance(plan, args.output)
    restored = verify_broker_conformance(args.output)
    assert restored.to_json() == report.to_json()
    assert len(report.results) == len(plan.case_ids)
    passed = all(
        capability.status.value == "pass" for capability in report.capabilities
    )
    assert passed, [
        (item.case_id, item.status.value, item.reason.value)
        for item in report.results
        if item.status.value != "pass"
    ]
    assert report.certified is not args.reference_host
    print(
        sys.version.split()[0],
        report.artifact_id,
        len(report.results),
        "verified-software-contract-only",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
