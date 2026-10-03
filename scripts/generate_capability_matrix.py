"""Render scoped capability claims; never execute or qualify their evidence."""

from __future__ import annotations

import argparse
import html
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from histdatacom.synthetic.capability_matrix import (
    CAPABILITY_CATALOG,
    MAX_MATRIX_BYTES,
    CapabilityMatrixPolicyV1,
    CapabilityMatrixV1,
    render_capability_matrix_markdown,
    structural_matrix_blockers,
)

ARTIFACT_DIRECTORY = Path("release-evidence/capability-matrix")
MATRIX_PATH = ARTIFACT_DIRECTORY / "current-dev-v1.json"
POLICY_PATH = ARTIFACT_DIRECTORY / "current-dev-policy-v1.json"
START = "<!-- capability-matrix:start -->"
END = "<!-- capability-matrix:end -->"


def _read(path: Path) -> str:
    with path.open("rb") as handle:
        raw = handle.read(MAX_MATRIX_BYTES + 1)
    if len(raw) > MAX_MATRIX_BYTES:
        raise ValueError("capability documentation input exceeds byte bound")
    return raw.decode("ascii")


def read_inputs(
    root: Path,
) -> tuple[CapabilityMatrixV1, CapabilityMatrixPolicyV1]:
    matrix_text = _read(root / MATRIX_PATH)
    policy_text = _read(root / POLICY_PATH)
    matrix = CapabilityMatrixV1.from_json(matrix_text)
    policy = CapabilityMatrixPolicyV1.from_json(policy_text)
    if matrix.to_json() != matrix_text or policy.to_json() != policy_text:
        raise ValueError("capability inputs must use exact canonical JSON")
    # This checks identity and declared scope, not scientific evidence.
    structural_matrix_blockers(matrix, policy)
    return matrix, policy


def _escape(value: str) -> str:
    return html.escape(value).replace("|", "&#124;").replace("`", "&#96;")


def _summary(
    matrix: CapabilityMatrixV1, policy: CapabilityMatrixPolicyV1
) -> str:
    blockers = structural_matrix_blockers(matrix, policy)
    counts = Counter(row.state.value for row in matrix.rows)
    states = ", ".join(
        f"{name}: {count}" for name, count in sorted(counts.items())
    )
    return (
        f"Snapshot `{matrix.release_id}` as of {matrix.as_of_utc}. "
        f"Dataset identity: `{matrix.dataset_id or 'absent'}`. "
        f"Frozen policy `{policy.policy_id}` requires {len(policy.requirements)} "
        f"rows; structural blockers: {len(blockers)}.\n\n"
        f"Recorded states across all {len(matrix.rows)} rows: {states}. "
        "These counts describe claims, not independent qualification.\n"
    )


def _current_state_notice() -> str:
    return (
        "The complete 2002-to-cutoff Temporal campaign and certified "
        "provider-neutral dataset are **not established by this snapshot**; "
        "no materialized dataset identity is supplied. The reconstruction "
        "substrate implements contracts and software, but complete execution, "
        "deep verification and era-stratified audit remain required. "
        "Open #522, #523 and #524 are not satisfied by this matrix.\n\n"
        "Later calendar, forecasting, orderflow, broker/multi-feed, schema "
        "replay, reproducibility, durability, feature/training-corpus, graph "
        "and serving programs remain separate scoped requirements. Their "
        "deferred rows do not block the v2.5 claim unless a new frozen policy "
        "explicitly includes them. Held #639 semantic-RNG and #622/#764 "
        "native-science candidates are not part of this dev-baseline snapshot.\n"
    )


def render_readme(
    matrix: CapabilityMatrixV1, policy: CapabilityMatrixPolicyV1
) -> str:
    rows = {row.requirement_id: row for row in matrix.rows}
    lines = [
        START,
        "## Current capability and evidence status",
        "",
        _current_state_notice().rstrip(),
        "",
        _summary(matrix, policy).rstrip(),
        "",
        "| Capability | Recorded state | Evidence scope | Blocking issues |",
        "|---|---|---|---|",
    ]
    for definition in CAPABILITY_CATALOG:
        if definition.parent_id is not None:
            continue
        row = rows[definition.requirement_id]
        values = (
            definition.title,
            row.state.value,
            row.evidence_scope.value,
            ", ".join(f"#{issue}" for issue in row.blocking_issues)
            or "none declared",
        )
        lines.append(
            "| " + " | ".join(_escape(value) for value in values) + " |"
        )
    lines.extend(
        (
            "",
            (
                "The [full 93-row matrix](docs/capability-matrix.md) retains each "
                "subrequirement, exact evidence identity, scope and limitation. "
                "It is generated from the [machine artifact](release-evidence/"
                "capability-matrix/current-dev-v1.json), not from issue closure, "
                "fixture presence or a report-exists check. No current row claims "
                "independently admitted `executed_passed` evidence."
            ),
            END,
        )
    )
    return "\n".join(lines)


def render_docs(
    matrix: CapabilityMatrixV1, policy: CapabilityMatrixPolicyV1
) -> str:
    table = render_capability_matrix_markdown(matrix).split("\n", 1)[1]
    return (
        "# Capability and certification matrix\n\n"
        "Generated by `scripts/generate_capability_matrix.py`; edit the "
        "versioned machine inputs, not this page.\n\n"
        + _current_state_notice()
        + "\n"
        + _summary(matrix, policy)
        + "\n## Reading the states\n\n"
        "`not_implemented` identifies a missing required surface. "
        "`implemented_unexecuted` is code-only for the stated claim, not a "
        "claim that no developer test ever ran. `executed_failed` retains a "
        "failed execution. `executed_insufficient_evidence` retains real "
        "execution whose scope does not establish the required claim. "
        "`executed_passed` requires exact independent verification in its "
        "declared scope. `deferred_blocked` keeps explicit successor or "
        "dependency gaps. `waived_with_limitation` is valid only under an "
        "explicit frozen policy clause; this snapshot permits no waivers.\n\n"
        "Evidence scopes `software`, `bounded` and `complete` are not "
        "interchangeable. A software test or generated-source replay is not "
        "an empirical provider, full-corpus or release qualification. "
        "A content hash locates bytes; it does not confer authority.\n\n"
        "The full-v2.5 policy requires all 24 critical rows at complete "
        "scope. Package/release promotion requires actual promotion and "
        "publication evidence. Prepromotion readiness is a separate "
        "explicitly narrowed claim and cannot establish full certification. "
        "This status artifact does not publish a package.\n\n"
        "## Provenance and limitations\n\n"
        "The implementation baseline is the immutable dev commit recorded "
        "in the release identity. Per-row implementation commits identify "
        "declared source-attribution metadata, not necessarily introduction "
        "commits. This snapshot's anchors were inspected in Git; fresh "
        "certification verifies current source bytes through its "
        "`implementation_id`, not the truth of an arbitrary Git label. "
        "Historical synthetic receipts have their original content IDs "
        "and limitations; they are not relabeled as fresh baseline runs. "
        "Some raw evidence resides outside the repository or is no longer "
        "retained. Such claims remain non-passing; a verifier must never "
        "substitute these retrieval references for the actual evidence.\n\n"
        "See [matrix-bound certification contracts]"
        "(reconstruction-certification-contracts.md) for the explicit public "
        "client/CLI route, concrete evidence profiles, fresh replay and "
        "blocked-versus-limited outcomes. Historical V2 scalar aggregation "
        "is not that verification boundary.\n\n"
        "Structural reads and this generator do not execute tests, import "
        "historical data or verify scientific evidence. Certification must "
        "run the versioned concrete evidence consumer and refuse unsupported "
        "verification. Parent/dependency rows cannot inherit a pass from "
        "one successful child. The source matrix and policy live under "
        "`release-evidence/capability-matrix/`.\n\n"
        "Regenerate with `python scripts/generate_capability_matrix.py "
        "--write`; check drift with `--check`. The generator checks both "
        "canonical inputs and all generated README/documentation bytes.\n\n"
        "## All requirement claims\n" + table
    )


def expected_outputs(root: Path) -> dict[Path, str]:
    matrix, policy = read_inputs(root)
    readme = (root / "README.md").read_text(encoding="utf-8")
    if readme.count(START) != 1 or readme.count(END) != 1:
        raise ValueError("README requires exactly one capability marker pair")
    start, end = readme.index(START), readme.index(END)
    if end < start:
        raise ValueError("README capability markers are reversed")
    return {
        root / "README.md": readme[:start]
        + render_readme(matrix, policy)
        + readme[end + len(END) :],
        root / "docs/capability-matrix.md": render_docs(matrix, policy),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    actions.add_argument("--write", action="store_true")
    actions.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = expected_outputs(ROOT)
    drift = []
    for path, content in expected.items():
        if args.write:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        elif not path.is_file() or path.read_text(encoding="utf-8") != content:
            drift.append(str(path.relative_to(ROOT)))
    if drift:
        print("Capability documentation drift: " + ", ".join(drift))
        return 1
    print("93 scoped capability claims; README and documentation agree")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
