"""Bounded manifest discovery, explicitly ineligible for qualification.

This module never opens Parquet or reconstructs event streams. A manifest's
content identity is structural information, not evidence of a valid product.
Inputs are cooperative local artifact trees; this is not a hostile-filesystem
snapshot protocol. Changed descriptors/bytes and final symlinks are refused.
"""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path
from typing import cast

from histdatacom.orchestration.reconstruction import (
    artifact_ref_for_file,
    verify_artifact_ref,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.persistence import (
    RECONSTRUCTION_PRODUCT_DIRECTORY,
    load_reconstruction_manifest,
)
from histdatacom.synthetic.reconstruction_plan import (
    read_reconstruction_plan_execution_manifest,
    read_synthetic_infill_plan,
)

CAMPAIGN_INVENTORY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-product-inventory.v1"
)
MAX_INVENTORY_PRODUCTS = 5000
MAX_INVENTORY_TREE_ENTRIES = 100_000
MAX_INVENTORY_DIRECTORY_DEPTH = 16
MAX_INVENTORY_BYTES = 16 * 1024 * 1024


def bounded_campaign_manifest_paths(
    output_root: str | Path,
) -> tuple[Path, ...]:
    """Find bounded committed-layout candidates, without claiming validity."""
    root = Path(output_root).expanduser() / RECONSTRUCTION_PRODUCT_DIRECTORY
    if root.is_symlink():
        raise ValueError("campaign product tree cannot be a symlink")
    if not root.exists():
        return ()
    pending = [(root, 0)]
    paths: list[Path] = []
    seen = 0
    while pending:
        directory, depth = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                seen += 1
                if seen > MAX_INVENTORY_TREE_ENTRIES:
                    raise ValueError("campaign inventory tree exceeds bounds")
                if entry.is_symlink():
                    raise ValueError(
                        "campaign inventory refuses symlink entries"
                    )
                path = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    if depth >= MAX_INVENTORY_DIRECTORY_DEPTH:
                        raise ValueError(
                            "campaign inventory depth exceeds bounds"
                        )
                    pending.append((path, depth + 1))
                elif (
                    entry.name == "manifest.json"
                    and path.parent.parent.name == "commits"
                ):
                    if not entry.is_file(follow_symlinks=False):
                        raise ValueError(
                            "campaign manifest must be a regular file"
                        )
                    paths.append(path)
                    if len(paths) > MAX_INVENTORY_PRODUCTS:
                        raise ValueError(
                            "campaign inventory product count exceeds bounds"
                        )
    return tuple(sorted(paths))


def _manifest_bytes(path: Path) -> bytes:
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_NONBLOCK"):
        raise ValueError(
            "campaign inventory requires POSIX no-follow file reads"
        )
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not (
            0 < before.st_size <= MAX_INVENTORY_BYTES
        ):
            raise ValueError("campaign manifest size/type exceeds bounds")
        encoded = stream.read(MAX_INVENTORY_BYTES + 1)
        after = os.fstat(stream.fileno())
    current = path.lstat()

    def facts(value: os.stat_result) -> tuple[int, ...]:
        return (
            value.st_dev,
            value.st_ino,
            value.st_mode,
            value.st_size,
            value.st_mtime_ns,
            value.st_ctime_ns,
        )

    if (
        facts(before) != facts(after)
        or facts(after) != facts(current)
        or len(encoded) != before.st_size
    ):
        raise ValueError("campaign manifest changed during inventory")
    return encoded


def inventory_campaign_products(
    plan_set_path: str | Path,
    *,
    output_directory: str | Path,
) -> ArtifactRef:
    """Write manifest-only observations, never a campaign product index.

    The finite v1 inventory refuses more than 5000 discovered candidates or
    16 MiB of serialized metadata. Use smaller plan sets for exploration;
    refusal never silently truncates the candidate denominator.
    """
    from histdatacom.reconstruction import (
        _write_campaign_json,
        read_reconstruction_plan_set,
    )

    plan_path = Path(plan_set_path).expanduser().resolve()
    plan_set = read_reconstruction_plan_set(plan_path)
    plan_ref = artifact_ref_for_file(
        plan_path,
        kind="reconstruction_plan_set_v1",
        metadata={"plan_set_id": plan_set.plan_set_id},
    )
    roots: set[str] = set()
    required: set[tuple[str, str, str]] = set()
    dependencies: list[ArtifactRef] = [plan_ref]
    for shard in plan_set.shards:
        verify_artifact_ref(shard.plan_ref)
        plan = read_synthetic_infill_plan(shard.plan_ref.path)
        if plan.plan_id != shard.plan_id:
            raise ValueError("campaign inventory plan identity differs")
        execution_ref = plan.artifact_graph["execution_manifest"]
        verify_artifact_ref(execution_ref)
        execution = read_reconstruction_plan_execution_manifest(
            execution_ref.path
        )
        dependencies.extend((shard.plan_ref, execution_ref))
        roots.add(execution.output_root)
        for request in plan.workflow_requests:
            for task in request.tasks:
                required.add(
                    (
                        plan.run.run_id,
                        task.window.window_id,
                        task.window.ensemble_member_id,
                    )
                )
                if len(required) > MAX_INVENTORY_PRODUCTS:
                    raise ValueError("campaign inventory plan exceeds bounds")
    candidates: list[JSONValue] = []
    seen_paths: set[Path] = set()
    # Reserve the small fixed envelope before retaining candidate metadata.
    # One candidate is bounded by its manifest snapshot; do not accumulate
    # thousands of individually large strings before checking the wire limit.
    remaining_candidate_bytes = MAX_INVENTORY_BYTES - 65536
    for root in sorted(roots):
        for path in bounded_campaign_manifest_paths(root):
            if path in seen_paths:
                continue
            seen_paths.add(path)
            if len(seen_paths) > MAX_INVENTORY_PRODUCTS:
                raise ValueError(
                    "campaign inventory product count exceeds bounds"
                )
            before = _manifest_bytes(path)
            manifest = load_reconstruction_manifest(path)
            if before != _manifest_bytes(path):
                raise ValueError("campaign manifest changed during inventory")
            coordinate = (
                manifest.run_id,
                manifest.window_id,
                manifest.ensemble_member_id,
            )
            manifest_ref = artifact_ref_for_file(
                path,
                kind="unverified_reconstruction_manifest",
                metadata={"verification_level": "manifest_only"},
            )
            if manifest_ref.sha256 != hashlib.sha256(before).hexdigest():
                raise ValueError("campaign manifest changed during inventory")
            dependencies.append(manifest_ref)
            candidate: dict[str, JSONValue] = {
                "manifest_ref": manifest_ref.to_dict(),
                "manifest_id": manifest.manifest_id,
                "run_id": coordinate[0],
                "window_id": coordinate[1],
                "ensemble_member_id": coordinate[2],
                "coordinate_scope": (
                    "planned" if coordinate in required else "out_of_plan"
                ),
                "verification_level": "manifest_only",
                "product_bytes_verified": False,
            }
            candidate_bytes = (
                len(canonical_contract_json(candidate).encode("utf-8")) + 1
            )
            if candidate_bytes > remaining_candidate_bytes:
                raise ValueError(
                    "prospective campaign inventory metadata exceeds bounds"
                )
            remaining_candidate_bytes -= candidate_bytes
            candidates.append(candidate)
    payload: dict[str, JSONValue] = {
        "schema_version": CAMPAIGN_INVENTORY_SCHEMA_VERSION,
        "verification_level": "manifest_only",
        "status": "unverified",
        "publication_eligible": False,
        "certification_eligible": False,
        "product_bytes_verified": False,
        "plan_set_ref": plan_ref.to_dict(),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "nonclaim": "Manifest discovery is not product, support, or quality verification.",
    }
    encoded = canonical_contract_json(payload).encode("utf-8")
    if len(encoded) > MAX_INVENTORY_BYTES:
        raise ValueError("campaign inventory metadata exceeds bounds")
    payload["inventory_id"] = (
        "campaign-product-inventory:sha256:"
        + hashlib.sha256(encoded).hexdigest()
    )
    for dependency in dependencies:
        verify_artifact_ref(dependency)
    target = Path(output_directory).expanduser().resolve() / (
        "campaign-product-inventory-"
        + cast(str, payload["inventory_id"]).rsplit(":", 1)[-1]
        + ".json"
    )
    _write_campaign_json(target, payload)
    return artifact_ref_for_file(
        target,
        kind="campaign_product_inventory_v1",
        metadata={
            "status": "unverified",
            "publication_eligible": False,
            "certification_eligible": False,
        },
    )
