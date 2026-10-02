"""Installable generated-only broker conformance command line."""

from __future__ import annotations

import argparse
from pathlib import Path

from histdatacom.broker_plugin_registry import discover_broker_plugins

from .catalog import broker_conformance_catalog
from .contracts import (
    BrokerConformanceDriverV1,
    BrokerConformancePlanV1,
    BrokerConformanceProfile,
    BrokerConformanceSubject,
)
from .drivers import plan_broker_conformance
from .fixtures import FIXTURE_FAULTS, build_broker_conformance_fixture
from .runner import run_broker_conformance
from .storage import read_text, verify_broker_conformance, write_artifact


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Synthetic software-contract diagnostics; no provider authority or scientific certification."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("catalog")
    fixture = commands.add_parser("fixture")
    fixture.add_argument("--output", required=True, type=Path)
    fixture.add_argument("--fault", choices=FIXTURE_FAULTS, default="none")
    plan = commands.add_parser("plan")
    plan.add_argument("--driver", required=True, type=Path)
    plan.add_argument(
        "--profile",
        required=True,
        choices=[item.value for item in BrokerConformanceProfile],
    )
    plan.add_argument("--capability", action="append", required=True)
    plan.add_argument(
        "--subject",
        choices=[item.value for item in BrokerConformanceSubject],
        default=BrokerConformanceSubject.CANDIDATE.value,
    )
    plan.add_argument("--output", required=True, type=Path)
    run = commands.add_parser("run")
    run.add_argument("--plan", required=True, type=Path)
    run.add_argument("--output", required=True, type=Path)
    run.add_argument(
        "--authorize-generated-execution", action="store_true", required=True
    )
    verify = commands.add_parser("verify")
    verify.add_argument("directory", type=Path)
    args = parser.parse_args(argv)
    if args.command == "catalog":
        print(broker_conformance_catalog().to_json())
        return 0
    if args.command == "fixture":
        built = build_broker_conformance_fixture(args.output, fault=args.fault)
        write_artifact(args.output / "driver.json", built.driver)
        print(str(built.wheel))
        return 0
    if args.command == "plan":
        result = plan_broker_conformance(
            discover_broker_plugins(),
            BrokerConformanceDriverV1.from_json(read_text(args.driver)),
            profile=BrokerConformanceProfile(args.profile),
            capabilities=tuple(sorted(set(args.capability))),
            subject=BrokerConformanceSubject(args.subject),
        )
        write_artifact(args.output, result)
        print(result.artifact_id)
        return 0
    if args.command == "run":
        report = run_broker_conformance(
            BrokerConformancePlanV1.from_json(read_text(args.plan)), args.output
        )
    else:
        report = verify_broker_conformance(args.directory)
    print(report.to_json())
    return 0 if report.certified else 1


if __name__ == "__main__":
    raise SystemExit(main())
