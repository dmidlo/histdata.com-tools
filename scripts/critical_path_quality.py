#!/usr/bin/env python3
"""Build commit-bound branch, property, and mutation quality reports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from histdatacom.synthetic.critical_path_quality import (
    CriticalPathGateCheckV1,
    CriticalPathGateReportV1,
    file_sha256,
    utc_now,
    write_critical_path_gate_report,
)

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class CoverageFloor:
    """Measured branch floor for one critical implementation family."""

    family: str
    path: str
    minimum: float
    rationale: str


@dataclass(frozen=True, slots=True)
class PropertyInvariant:
    """Named invariant and its direct pytest evidence nodes."""

    invariant_id: str
    test_nodeids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MutationSpec:
    """One exact source mutation and the tests expected to kill it."""

    mutation_id: str
    invariant_id: str
    source_path: str
    original: str
    replacement: str
    test_nodeids: tuple[str, ...]
    focused: bool = False


# Floors are rounded below the 2026-08-21 focused baseline.  Large orchestration
# modules receive slightly lower floors than compact leaf contracts, but no
# critical family can borrow coverage from an unrelated module.
COVERAGE_FLOORS = (
    CoverageFloor(
        "experiment_source_identity",
        "src/histdatacom/reconstruction_experiment.py",
        58.0,
        "content identity and source binding across local materialization roots",
    ),
    CoverageFloor(
        "schema_restoration",
        "src/histdatacom/reconstruction_schema.py",
        77.0,
        "version restoration and fail-closed compatibility translation",
    ),
    CoverageFloor(
        "observation_cardinality",
        "src/histdatacom/synthetic/observation.py",
        64.0,
        "anchor-safe observation and exact cardinality accounting",
    ),
    CoverageFloor(
        "adaptive_partition",
        "src/histdatacom/synthetic/partition_invariance.py",
        62.0,
        "half-open source ownership, history, and semantic seed invariance",
    ),
    CoverageFloor(
        "support_plan",
        "src/histdatacom/synthetic/reconstruction_plan.py",
        54.0,
        "adaptive support planning and bounded workflow construction",
    ),
    CoverageFloor(
        "support_index",
        "src/histdatacom/synthetic/support_verification.py",
        50.0,
        "independent support replay and gap-free final index construction",
    ),
    CoverageFloor(
        "bounded_alignment",
        "src/histdatacom/synthetic/alignment_qualification.py",
        65.0,
        "prior-only quote selection and age-policy qualification",
    ),
    CoverageFloor(
        "carving_anchor_lineage",
        "src/histdatacom/synthetic/carving.py",
        58.0,
        "hard-rule precedence, immutable anchors, and lineage retention",
    ),
    CoverageFloor(
        "cross_currency",
        "src/histdatacom/synthetic/cross_currency.py",
        72.0,
        "bid/ask-side triangle algebra and atomic cross-series validation",
    ),
    CoverageFloor(
        "atomic_persistence",
        "src/histdatacom/synthetic/persistence.py",
        61.0,
        "committed-only discovery and byte/logical replay verification",
    ),
    CoverageFloor(
        "request_receipt_resume",
        "src/histdatacom/orchestration/reconstruction.py",
        65.0,
        "retry-safe requests, receipts, cancellation, and runtime failures",
    ),
    CoverageFloor(
        "campaign_publication",
        "src/histdatacom/reconstruction.py",
        54.0,
        "unique campaign products and complete-index publication",
    ),
    CoverageFloor(
        "certification_aggregation",
        "src/histdatacom/synthetic/certification_campaign.py",
        55.0,
        "hash-verified measurement extraction into certification",
    ),
    CoverageFloor(
        "certification_evidence",
        "src/histdatacom/synthetic/certification.py",
        65.0,
        "falsifiable evidence evaluation and fail-closed promotion state",
    ),
)


PROPERTY_INVARIANTS = (
    PropertyInvariant(
        "support_intervals_contiguous_exactly_once",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_partitions_are_contiguous_and_own_rows_exactly_once",
            "tests/unit/test_critical_path_properties.py::test_generated_partition_gaps_fail_closed",
        ),
    ),
    PropertyInvariant(
        "half_open_partition_no_loss_or_duplication",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_partitions_are_contiguous_and_own_rows_exactly_once",
        ),
    ),
    PropertyInvariant(
        "observed_anchors_immutable",
        (
            "tests/unit/test_synthetic_persistence.py::test_anchor_value_drift_refuses_even_when_observed_id_is_unchanged",
        ),
    ),
    PropertyInvariant(
        "future_events_excluded_from_prior_support",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_future_alignment_tuples_fail_closed",
            "tests/unit/test_alignment_qualification.py::test_future_event_and_silent_widening_fail_closed",
        ),
    ),
    PropertyInvariant(
        "worker_retry_order_path_semantic_invariance",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_seed_identity_excludes_execution_tuning",
            "tests/unit/test_synthetic_observation.py::test_input_order_does_not_change_delivery_result",
            "tests/unit/test_orchestration_reconstruction.py::test_worker_loss_after_receipt_reuses_artifact_without_handler",
        ),
    ),
    PropertyInvariant(
        "source_and_config_hash_invalidation",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_semantic_changes_invalidate_identity",
            "tests/unit/test_release_candidate.py::test_content_addressed_reader_rejects_tampering",
        ),
    ),
    PropertyInvariant(
        "scientific_and_resource_limits_fail_closed",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_resource_boundaries_are_inclusive_and_overflow_fails",
            "tests/unit/test_critical_path_properties.py::test_spectral_radius_boundary_fails_closed",
            "tests/unit/test_orchestration_reconstruction.py::test_request_rejects_inline_rows_hidden_in_artifact_metadata",
            "tests/unit/test_resource_envelopes.py::test_forecast_refuses_extrapolation_beyond_measured_limit",
        ),
    ),
    PropertyInvariant(
        "cross_currency_bid_ask_sides_and_anchor_lineage",
        (
            "tests/unit/test_synthetic_cross_currency.py::test_triangle_residual_uses_independent_executable_bid_ask_sides",
            "tests/unit/test_synthetic_cross_currency.py::test_triangle_reconciliation_is_deterministic_and_preserves_anchors",
        ),
    ),
    PropertyInvariant(
        "accepted_rejected_counts_reconcile",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_rejection_counts_reconcile",
        ),
    ),
    PropertyInvariant(
        "campaign_coordinate_has_one_product",
        (
            "tests/unit/test_reconstruction_plan.py::test_public_plan_spec_supports_exact_paired_window_bounds",
        ),
    ),
    PropertyInvariant(
        "incomplete_product_index_cannot_publish",
        (
            "tests/unit/test_reconstruction_plan.py::test_public_plan_spec_supports_exact_paired_window_bounds",
        ),
    ),
    PropertyInvariant(
        "atomic_discovery_excludes_scratch",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_scratch_manifests_are_never_discoverable",
            "tests/unit/test_synthetic_persistence.py::test_staging_is_invisible_and_commit_is_atomic_and_idempotent",
        ),
    ),
)


MUTATIONS = (
    MutationSpec(
        "stability_boundary_lt_instead_of_lte",
        "scientific_and_resource_limits_fail_closed",
        "src/histdatacom/synthetic/marked_hawkes.py",
        "if radius < 0.0 or radius >= 1.0:",
        "if radius < 0.0 or radius > 1.0:",
        (
            "tests/unit/test_critical_path_properties.py::test_spectral_radius_boundary_fails_closed",
        ),
        True,
    ),
    MutationSpec(
        "resource_boundary_rejects_equal_limit",
        "scientific_and_resource_limits_fail_closed",
        "src/histdatacom/synthetic/streaming.py",
        "if actual > limit:\n                violations.append(",
        "if actual >= limit:\n                violations.append(",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_resource_boundaries_are_inclusive_and_overflow_fails",
        ),
        True,
    ),
    MutationSpec(
        "removed_release_artifact_hash_comparison",
        "source_and_config_hash_invalidation",
        "src/histdatacom/synthetic/release_candidate.py",
        'if _file_sha256(path) != ref.sha256:\n        raise ValueError(f"release candidate artifact hash differs: {path}")',
        'if False and _file_sha256(path) != ref.sha256:\n        raise ValueError(f"release candidate artifact hash differs: {path}")',
        (
            "tests/unit/test_release_candidate.py::test_nested_artifact_hash_tampering_fails_verification",
        ),
        True,
    ),
    MutationSpec(
        "omitted_support_interval_contiguity",
        "support_intervals_contiguous_exactly_once",
        "src/histdatacom/synthetic/partition_invariance.py",
        'if left.end_ns != right.start_ns:\n                raise ValueError("adaptive partition is not contiguous")',
        'if False and left.end_ns != right.start_ns:\n                raise ValueError("adaptive partition is not contiguous")',
        (
            "tests/unit/test_critical_path_properties.py::test_generated_partition_gaps_fail_closed",
        ),
        True,
    ),
    MutationSpec(
        "accepted_future_alignment_timestamp",
        "future_events_excluded_from_prior_support",
        "src/histdatacom/synthetic/alignment_qualification.py",
        "if age < 0 or probe_time - times[symbol] != age:",
        "if probe_time - times[symbol] != age:",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_future_alignment_tuples_fail_closed",
        ),
        True,
    ),
    MutationSpec(
        "swapped_triangle_denominator_side",
        "cross_currency_bid_ask_sides_and_anchor_lineage",
        "src/histdatacom/synthetic/cross_currency.py",
        "implied_bid = numerator.bid / denominator.ask\n        implied_ask = numerator.ask / denominator.bid",
        "implied_bid = numerator.bid / denominator.bid\n        implied_ask = numerator.ask / denominator.ask",
        (
            "tests/unit/test_synthetic_cross_currency.py::test_triangle_residual_uses_independent_executable_bid_ask_sides",
        ),
    ),
    MutationSpec(
        "disabled_anchor_comparison",
        "observed_anchors_immutable",
        "src/histdatacom/synthetic/persistence.py",
        "if outputs != anchors:\n        raise ReconstructionPersistenceError(",
        "if False and outputs != anchors:\n        raise ReconstructionPersistenceError(",
        (
            "tests/unit/test_synthetic_persistence.py::test_anchor_value_drift_refuses_even_when_observed_id_is_unchanged",
        ),
        True,
    ),
    MutationSpec(
        "changed_alignment_age_boundary",
        "future_events_excluded_from_prior_support",
        "src/histdatacom/synthetic/cross_currency.py",
        "<= nearest_prior_max_age_ns\n                for member in relationship.symbols",
        "< nearest_prior_max_age_ns\n                for member in relationship.symbols",
        (
            "tests/unit/test_synthetic_cross_currency.py::test_bounded_nearest_prior_reconciles_an_asynchronous_triangle",
        ),
    ),
    MutationSpec(
        "ignored_missing_campaign_product",
        "incomplete_product_index_cannot_publish",
        "src/histdatacom/reconstruction.py",
        'if index.status != "complete" or index.missing_product_count:',
        'if False and (index.status != "complete" or index.missing_product_count):',
        (
            "tests/unit/test_reconstruction_plan.py::test_public_plan_spec_supports_exact_paired_window_bounds",
        ),
        True,
    ),
    MutationSpec(
        "bypassed_product_verification",
        "campaign_coordinate_has_one_product",
        "src/histdatacom/reconstruction.py",
        "verify_reconstruction_publication(path)\n                if verify_products\n                else load_reconstruction_manifest(path)",
        "load_reconstruction_manifest(path)\n                if verify_products\n                else load_reconstruction_manifest(path)",
        (
            "tests/unit/test_reconstruction_plan.py::test_public_plan_spec_supports_exact_paired_window_bounds",
        ),
    ),
    MutationSpec(
        "runtime_refusal_treated_as_completion",
        "worker_retry_order_path_semantic_invariance",
        "src/histdatacom/orchestration/reconstruction.py",
        "if outcome.status is ReconstructionStageStatus.REFUSED:\n            refused = state.interrupted(",
        "if outcome.status is ReconstructionStageStatus.COMPLETED:\n            refused = state.interrupted(",
        (
            "tests/unit/test_orchestration_reconstruction.py::test_many_stage_refusal_reasons_persist_as_bounded_summary",
        ),
        True,
    ),
    MutationSpec(
        "accepted_rejected_reconciliation_inverted",
        "accepted_rejected_counts_reconcile",
        "src/histdatacom/synthetic/streaming.py",
        "if self.accepted_count + self.rejected_count != self.candidate_count:",
        "if self.accepted_count + self.rejected_count == self.candidate_count:",
        (
            "tests/unit/test_critical_path_properties.py::test_generated_rejection_counts_reconcile",
        ),
    ),
)


def _git_commit_sha(explicit: str | None) -> str:
    if explicit:
        return explicit.strip().lower()
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip().lower()


def _coverage_file(files: dict[str, Any], expected: str) -> tuple[str, dict[str, Any]]:
    normalized = expected.replace("\\", "/")
    matches = [
        (name, value)
        for name, value in files.items()
        if name.replace("\\", "/").endswith(normalized)
    ]
    if len(matches) != 1:
        raise ValueError(f"coverage file match is not unique: {expected}")
    return matches[0]


def _coverage_report(args: argparse.Namespace) -> int:
    coverage_path = Path(args.coverage_json).expanduser().resolve()
    payload = json.loads(coverage_path.read_text(encoding="utf-8"))
    meta = payload.get("meta")
    if not isinstance(meta, dict) or meta.get("branch_coverage") is not True:
        raise ValueError("coverage JSON was not collected with branch coverage")
    files = payload.get("files")
    if not isinstance(files, dict):
        raise TypeError("coverage JSON does not contain per-file data")
    checks = []
    source_hashes = {coverage_path.name: file_sha256(coverage_path)}
    for floor in COVERAGE_FLOORS:
        _observed_name, data = _coverage_file(files, floor.path)
        summary = data.get("summary")
        if not isinstance(summary, dict):
            raise TypeError(f"coverage summary is absent: {floor.path}")
        branches = int(summary.get("num_branches", 0))
        missing = int(summary.get("missing_branches", 0))
        if branches <= 0 or missing < 0 or missing > branches:
            raise ValueError(f"branch coverage is invalid: {floor.path}")
        covered = branches - missing
        percentage = 100.0 * covered / branches
        passed = percentage + 1e-12 >= floor.minimum
        checks.append(
            CriticalPathGateCheckV1(
                check_id=floor.family,
                status="passed" if passed else "failed",
                evidence={
                    "module": floor.path,
                    "coverage_key": floor.path,
                    "coverage_version": str(meta.get("version", "")),
                    "covered_branches": covered,
                    "total_branches": branches,
                    "branch_percent": round(percentage, 3),
                    "minimum_branch_percent": floor.minimum,
                    "rationale": floor.rationale,
                },
            )
        )
        source_hashes[floor.path] = file_sha256(ROOT / floor.path)
    report = CriticalPathGateReportV1(
        gate_name="critical_branch_coverage",
        git_commit_sha=_git_commit_sha(args.git_commit_sha),
        command=f"coverage {coverage_path.name}",
        profile="release-critical-modules",
        source_hashes=source_hashes,
        checks=tuple(checks),
        created_at_utc=utc_now(),
        passed=all(item.passed for item in checks),
    )
    ref = write_critical_path_gate_report(report, args.output)
    print(json.dumps(ref.to_dict(), indent=2, sort_keys=True))
    return 0 if report.passed else 1


def _run_pytest(
    nodeids: tuple[str, ...], timeout: int
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "pytest", "-q", *nodeids],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _property_report(args: argparse.Namespace) -> int:
    nodeids = tuple(
        dict.fromkeys(
            nodeid
            for invariant in PROPERTY_INVARIANTS
            for nodeid in invariant.test_nodeids
        )
    )
    started = time.monotonic()
    result = _run_pytest(nodeids, args.timeout_seconds)
    duration = round(time.monotonic() - started, 3)
    output = (result.stdout + result.stderr).encode("utf-8", errors="replace")
    passed = result.returncode == 0
    shared = {
        "pytest_returncode": result.returncode,
        "pytest_output_sha256": hashlib.sha256(output).hexdigest(),
        "duration_seconds": duration,
    }
    checks = tuple(
        CriticalPathGateCheckV1(
            check_id=item.invariant_id,
            status="passed" if passed else "failed",
            evidence={**shared, "test_nodeids": list(item.test_nodeids)},
        )
        for item in PROPERTY_INVARIANTS
    )
    source_paths = sorted({nodeid.split("::", 1)[0] for nodeid in nodeids})
    report = CriticalPathGateReportV1(
        gate_name="critical_property_invariants",
        git_commit_sha=_git_commit_sha(args.git_commit_sha),
        command="pytest -q " + " ".join(nodeids),
        profile="bounded-critical-invariants",
        source_hashes={path: file_sha256(ROOT / path) for path in source_paths},
        checks=checks,
        created_at_utc=utc_now(),
        passed=passed,
    )
    ref = write_critical_path_gate_report(report, args.output)
    print(json.dumps(ref.to_dict(), indent=2, sort_keys=True))
    if not passed:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
    return 0 if passed else 1


def _mutated_test(spec: MutationSpec, timeout: int) -> tuple[str, int, float, str]:
    source = ROOT / spec.source_path
    text = source.read_text(encoding="utf-8")
    occurrence_count = text.count(spec.original)
    if occurrence_count != 1:
        raise ValueError(
            f"mutation {spec.mutation_id} expected one source occurrence, "
            f"found {occurrence_count}"
        )
    mutated = text.replace(spec.original, spec.replacement, 1)
    with tempfile.TemporaryDirectory(prefix="histdatacom-mutant-") as raw:
        temporary = Path(raw)
        package_root = temporary / "src" / "histdatacom"
        shutil.copytree(ROOT / "src" / "histdatacom", package_root)
        target = temporary / "src" / spec.source_path.removeprefix("src/")
        target.write_text(mutated, encoding="utf-8")
        environment = dict(os.environ)
        prior_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = str(temporary / "src") + (
            os.pathsep + prior_pythonpath if prior_pythonpath else ""
        )
        started = time.monotonic()
        try:
            result = subprocess.run(
                [sys.executable, "-m", "pytest", "-q", *spec.test_nodeids],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
            duration = round(time.monotonic() - started, 3)
            output = (result.stdout + result.stderr).encode("utf-8", errors="replace")
            status = (
                "killed"
                if result.returncode == 1
                else "survived" if result.returncode == 0 else "invalid"
            )
            return (
                status,
                result.returncode,
                duration,
                hashlib.sha256(output).hexdigest(),
            )
        except subprocess.TimeoutExpired as err:
            output = (
                (err.stdout or b"")
                if isinstance(err.stdout, bytes)
                else (err.stdout or "").encode()
            )
            return (
                "timeout",
                124,
                round(time.monotonic() - started, 3),
                hashlib.sha256(output).hexdigest(),
            )


def _mutation_report(args: argparse.Namespace) -> int:
    selected = tuple(
        item for item in MUTATIONS if args.profile == "release" or item.focused
    )
    checks = []
    for spec in selected:
        status, returncode, duration, output_hash = _mutated_test(
            spec, args.timeout_seconds
        )
        checks.append(
            CriticalPathGateCheckV1(
                check_id=spec.mutation_id,
                status="passed" if status == "killed" else "failed",
                evidence={
                    "invariant_id": spec.invariant_id,
                    "mutation_status": status,
                    "source_path": spec.source_path,
                    "source_sha256": file_sha256(ROOT / spec.source_path),
                    "operator": f"{spec.original!r} -> {spec.replacement!r}",
                    "test_nodeids": list(spec.test_nodeids),
                    "pytest_returncode": returncode,
                    "pytest_output_sha256": output_hash,
                    "duration_seconds": duration,
                    "reviewed_justification": "",
                },
            )
        )
        print(f"{spec.mutation_id}: {status}", flush=True)
    passed = all(item.passed for item in checks)
    source_paths = sorted({item.source_path for item in selected})
    report = CriticalPathGateReportV1(
        gate_name="critical_mutation_testing",
        git_commit_sha=_git_commit_sha(args.git_commit_sha),
        command=f"critical_path_quality.py mutations --profile {args.profile}",
        profile=args.profile,
        source_hashes={path: file_sha256(ROOT / path) for path in source_paths},
        checks=tuple(checks),
        created_at_utc=utc_now(),
        passed=passed,
    )
    ref = write_critical_path_gate_report(report, args.output)
    print(json.dumps(ref.to_dict(), indent=2, sort_keys=True))
    return 0 if passed else 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command_name", required=True)

    coverage = subparsers.add_parser("coverage")
    coverage.add_argument("--coverage-json", required=True)
    coverage.add_argument("--output", required=True)
    coverage.add_argument("--git-commit-sha")
    coverage.set_defaults(handler=_coverage_report)

    properties = subparsers.add_parser("properties")
    properties.add_argument("--output", required=True)
    properties.add_argument("--git-commit-sha")
    properties.add_argument("--timeout-seconds", type=int, default=900)
    properties.set_defaults(handler=_property_report)

    mutations = subparsers.add_parser("mutations")
    mutations.add_argument("--output", required=True)
    mutations.add_argument("--git-commit-sha")
    mutations.add_argument(
        "--profile", choices=("focused", "release"), default="focused"
    )
    mutations.add_argument("--timeout-seconds", type=int, default=300)
    mutations.set_defaults(handler=_mutation_report)
    return parser


def main() -> int:
    """Run one bounded quality evidence command."""
    args = _parser().parse_args()
    if getattr(args, "timeout_seconds", 1) <= 0:
        raise ValueError("timeout_seconds must be positive")
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
