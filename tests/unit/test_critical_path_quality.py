"""Tests for commit-bound critical-path quality evidence."""

from __future__ import annotations

import ast
import hashlib
import json
from argparse import Namespace
from pathlib import Path

import pytest

from histdatacom.synthetic.critical_path_quality import (
    CRITICAL_PATH_GATE_REPORT_ARTIFACT_KINDS,
    CriticalPathGateCheckV1,
    CriticalPathGateReportV1,
    read_critical_path_gate_report,
    write_critical_path_gate_report,
)
from scripts import critical_path_quality as runner

_COMMIT = hashlib.sha256(b"critical-path-commit").hexdigest()


def _report(*, passed: bool = True) -> CriticalPathGateReportV1:
    return CriticalPathGateReportV1(
        gate_name="critical_branch_coverage",
        git_commit_sha=_COMMIT,
        command="coverage critical.json",
        profile="test",
        source_hashes={
            "coverage.json": hashlib.sha256(b"coverage").hexdigest()
        },
        checks=(
            CriticalPathGateCheckV1(
                check_id="critical-module",
                status="passed" if passed else "failed",
                evidence={
                    "covered_branches": 9,
                    "total_branches": 10,
                    "minimum_branch_percent": 80.0,
                },
            ),
        ),
        created_at_utc="2026-08-21T12:00:00Z",
        passed=passed,
    )


def test_report_round_trips_as_strong_release_gate_evidence(
    tmp_path: Path,
) -> None:
    report = _report()
    ref = write_critical_path_gate_report(report, tmp_path / "report.json")

    assert read_critical_path_gate_report(ref.path) == report
    assert (
        ref.kind == CRITICAL_PATH_GATE_REPORT_ARTIFACT_KINDS[report.gate_name]
    )
    assert ref.metadata == {
        "gate_name": report.gate_name,
        "git_commit_sha": _COMMIT,
        "passed": True,
        "report_id": report.report_id,
        "check_count": 1,
        "profile": "test",
    }

    payload = json.loads(Path(ref.path).read_text(encoding="utf-8"))
    payload["checks"][0]["evidence"]["covered_branches"] = 8
    Path(ref.path).write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="identity differs"):
        read_critical_path_gate_report(ref.path)


def test_failed_report_is_retained_but_not_marked_as_passing(
    tmp_path: Path,
) -> None:
    report = _report(passed=False)
    ref = write_critical_path_gate_report(report, tmp_path / "failed.json")

    assert not read_critical_path_gate_report(ref.path).passed
    assert ref.metadata["passed"] is False


def test_coverage_runner_enforces_branch_percentage_not_global_average(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "critical.py"
    source.write_text("if True:\n    pass\n", encoding="utf-8")
    floor = runner.CoverageFloor(
        family="critical-test",
        path=str(source),
        minimum=80.0,
        rationale="test floor",
    )
    monkeypatch.setattr(runner, "COVERAGE_FLOORS", (floor,))
    coverage = tmp_path / "coverage.json"
    coverage.write_text(
        json.dumps(
            {
                "meta": {"version": "7.15.2", "branch_coverage": True},
                "files": {
                    str(source): {
                        "summary": {
                            "num_branches": 10,
                            "missing_branches": 2,
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "critical-coverage.json"

    result = runner._coverage_report(
        Namespace(
            coverage_json=str(coverage),
            output=str(output),
            git_commit_sha=_COMMIT,
        )
    )

    assert result == 0
    report = read_critical_path_gate_report(output)
    assert report.checks[0].evidence["branch_percent"] == 80.0
    assert report.checks[0].evidence["minimum_branch_percent"] == 80.0


def test_mutation_specs_are_exact_and_focused_profile_is_bounded() -> None:
    focused = tuple(item for item in runner.MUTATIONS if item.focused)

    assert 1 <= len(focused) < len(runner.MUTATIONS)
    assert len({item.mutation_id for item in runner.MUTATIONS}) == len(
        runner.MUTATIONS
    )
    for mutation in runner.MUTATIONS:
        source = runner.ROOT / mutation.source_path
        assert source.read_text(encoding="utf-8").count(mutation.original) == 1
        assert mutation.original != mutation.replacement
        assert mutation.test_nodeids


def test_campaign_mutations_bind_distinct_concrete_invariant_witnesses() -> (
    None
):
    properties = {
        item.invariant_id: item.test_nodeids
        for item in runner.PROPERTY_INVARIANTS
    }
    mutations = {item.mutation_id: item for item in runner.MUTATIONS}
    publication = mutations["ignored_missing_campaign_product"]
    product = mutations["bypassed_product_verification"]
    publication_nodes = tuple(
        "tests/unit/test_campaign_mutation_guards.py::" + name
        for name in (
            "test_publication_refuses_incomplete_fresh_verification",
            "test_publication_refuses_incomplete_index",
            "test_publication_refuses_missing_retained_product",
            "test_complete_publication_inputs_reach_artifact_binding",
        )
    )
    product_nodes = (
        "tests/unit/test_campaign_mutation_native.py::test_native_products_are_verified_before_index_emission",
    )
    assert publication.invariant_id == "incomplete_product_index_cannot_publish"
    assert (
        product.invariant_id == "native_products_verified_before_index_emission"
    )
    assert publication.test_nodeids == publication_nodes
    assert product.test_nodeids == product_nodes
    assert properties[publication.invariant_id] == publication_nodes
    assert properties[product.invariant_id] == product_nodes
    assert publication.focused is True
    assert product.focused is False
    coordinate_nodes = properties["campaign_coordinate_has_one_product"]
    assert coordinate_nodes == (
        "tests/unit/test_campaign_verification_oracle.py::test_duplicate_member_coordinate_is_not_extra_support",
    )
    for nodeid in (*publication_nodes, *product_nodes, *coordinate_nodes):
        path, name = nodeid.split("::")
        tree = ast.parse((runner.ROOT / path).read_text(encoding="utf-8"))
        assert any(
            isinstance(node, ast.FunctionDef) and node.name == name
            for node in tree.body
        ), nodeid
    for mutation in (publication, product):
        source = (runner.ROOT / mutation.source_path).read_text(
            encoding="utf-8"
        )
        assert source.count(mutation.original) == 1
        ast.parse(source.replace(mutation.original, mutation.replacement, 1))


def test_property_manifest_covers_every_required_invariant_and_existing_node() -> (
    None
):
    required = {
        "support_intervals_contiguous_exactly_once",
        "half_open_partition_no_loss_or_duplication",
        "observed_anchors_immutable",
        "future_events_excluded_from_prior_support",
        "worker_retry_order_path_semantic_invariance",
        "source_and_config_hash_invalidation",
        "scientific_and_resource_limits_fail_closed",
        "cross_currency_bid_ask_sides_and_anchor_lineage",
        "accepted_rejected_counts_reconcile",
        "campaign_coordinate_has_one_product",
        "native_products_verified_before_index_emission",
        "incomplete_product_index_cannot_publish",
        "atomic_discovery_excludes_scratch",
    }

    assert {
        item.invariant_id for item in runner.PROPERTY_INVARIANTS
    } == required
    for invariant in runner.PROPERTY_INVARIANTS:
        for nodeid in invariant.test_nodeids:
            assert (runner.ROOT / nodeid.split("::", 1)[0]).is_file()
