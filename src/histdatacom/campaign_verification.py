"""Fresh campaign integrity and final native validation, without promotion.

This module does not rerun a scientific qualification campaign or authenticate
the historical completeness of caller-created source data. It independently
checks the complete declared plan rectangle and actual retained product bytes.
No receipt object, cached boolean or caller-selected verification mode can
replace this execution at publication/certification boundaries.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import stat
from bisect import bisect_right
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Any, cast

from histdatacom.campaign_index_contracts import (
    MAX_OUT_OF_PLAN_PRODUCTS,
    CampaignArtifactRefV1,
    CampaignDeepVerificationV1,
    CampaignProductVerificationV1,
    CampaignStructuralVerificationV1,
    canonical,
)
from histdatacom.runtime_contracts import ArtifactRef

# Control documents retain the existing native bound. Execution is serial by
# shard/window; event allocation additionally obeys the retained plan limits.
MAX_CONTROL_BYTES = 64 * 1024 * 1024
MAX_TRACKED_FILES = 262144
MAX_DISCOVERED_PRODUCTS = 4096 * 5000
MAX_CONTROL_DEPTH = 64
MAX_CONTROL_NODES = 4 * 1024 * 1024
MAX_CAMPAIGN_COORDINATES = 262144
MAX_RETAINED_PLAN_BYTES = 256 * 1024 * 1024
_COUNT_FIELDS = (
    "support_window_count",
    "verified_product_count",
    "missing_product_count",
    "empty_window_count",
    "refused_window_count",
    "observed_event_count",
    "synthetic_event_count",
)


def _wire(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _same(actual: Any, expected: Any, message: str) -> None:
    if _wire(actual) != _wire(expected):
        raise ValueError(message)


def _digest(value: Any) -> str:
    return hashlib.sha256(_wire(value).encode("ascii")).hexdigest()


def _verify_delivery_output_hash(streams: Any, expected_hash: str) -> None:
    from histdatacom.synthetic.delivery import (
        reconstruction_streams_content_sha256,
    )

    # Delivery commits flattened ordered event rows. The independently replayed
    # cross-currency report instead commits symbol-grouped stream records; its
    # validation ID and full quality-evidence commitment are checked separately.
    _same(
        reconstruction_streams_content_sha256(streams),
        expected_hash,
        "product actual delivery output hash differs",
    )


def _verify_quality_evidence_hash(quality: Any, report: Any) -> None:
    from histdatacom.synthetic import persistence

    quality_evidence = {
        "final_validation": report.to_dict(),
        "benchmark_artifact_ids": list(quality.benchmark_artifact_ids),
        "benchmark_evidence": dict(quality.benchmark_evidence),
        **{
            name: list(getattr(quality, name))
            for name in (
                "point_in_time_evidence_projection_ids",
                "point_in_time_evidence_decision_ids",
                "cross_series_constraint_bundle_ids",
                "cross_series_constraint_window_ids",
                "cross_series_constraint_decision_ids",
                "projection_burden_report_ids",
                "projection_burden_receipt_ids",
            )
        },
        "projection_burden_status": quality.projection_burden_status,
    }
    if (
        persistence._content_sha256(quality_evidence)
        != quality.cross_instrument_quality_sha256
    ):
        raise ValueError(
            "product final validation evidence commitment differs; "
            "legacy noncanonical evidence-list commitments require explicit "
            "republication from verified inputs (no automatic migration)"
        )


def _number(value: Any) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("campaign conditioning requires a finite number")
    return float(value)


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate native campaign control key")
        result[key] = value
    return result


def _integer(text: str) -> int:
    if len(text) > 80:
        raise ValueError("native campaign integer exceeds bounded work")
    return int(text)


def _constant(_text: str) -> None:
    raise ValueError("nonfinite native campaign JSON value")


def _float(text: str) -> float:
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("nonfinite native campaign JSON value")
    return value


def _json(data: bytes) -> dict[str, Any]:
    if len(data) > MAX_CONTROL_BYTES:
        raise ValueError("native campaign control byte bound exceeded")
    text = data.decode("utf-8")
    depth = nodes = 0
    quoted = escaped = atom = False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
            continue
        if char == '"':
            nodes += 1
            quoted = True
            atom = False
        elif char in "[{":
            depth += 1
            nodes += 1
            atom = False
        elif char in "]}":
            depth -= 1
            atom = False
        elif char in " ,:\r\n\t":
            atom = False
        elif not atom:
            nodes += 1
            atom = True
        if not 0 <= depth <= MAX_CONTROL_DEPTH or nodes > MAX_CONTROL_NODES:
            raise ValueError("native campaign JSON structural bound exceeded")
    value = json.loads(
        text,
        object_pairs_hook=_pairs,
        parse_int=_integer,
        parse_float=_float,
        parse_constant=_constant,
    )
    if type(value) is not dict:
        raise ValueError("native campaign control must be an object")
    return value


def _path(value: str | Path) -> Path:
    if type(value) not in (str, Path, type(Path())):
        raise TypeError("campaign path requires text or concrete Path")
    raw = os.fspath(value)
    if not raw or len(raw) > 4096 or "\0" in raw:
        raise ValueError("campaign path exceeds bounds")
    return Path(os.path.abspath(os.path.expanduser(raw)))


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _regular_ancestors(path: Path) -> None:
    for parent in reversed(path.parents):
        if not stat.S_ISDIR(parent.lstat().st_mode):
            raise ValueError(
                "campaign artifact ancestor is not a real directory"
            )


def _require_platform() -> None:
    if os.name != "posix" or not all(
        hasattr(os, flag) for flag in ("O_NONBLOCK", "O_NOFOLLOW")
    ):
        raise ValueError(
            "campaign verification requires POSIX no-follow/nonblocking file support"
        )


@dataclass(frozen=True)
class _Snapshot:
    path: Path
    identity: tuple[int, ...]
    sha256: str


class _Guard:
    def __init__(self) -> None:
        self.files: dict[Path, _Snapshot] = {}
        self.graph_seen: set[Path] = set()

    def read(
        self,
        path: str | Path,
        *,
        ref: ArtifactRef | None = None,
        control: bool = False,
    ) -> bytes:
        _require_platform()
        target = _path(path)
        _regular_ancestors(target)
        before = target.lstat()
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(
                "campaign evidence must be a regular no-follow file"
            )
        if control and before.st_size > MAX_CONTROL_BYTES:
            raise ValueError("campaign control byte bound exceeded")
        if len(self.files) >= MAX_TRACKED_FILES and target not in self.files:
            raise ValueError(
                "campaign verification file-inventory bound exceeded"
            )
        if ref is not None:
            admitted = CampaignArtifactRefV1.from_artifact_ref(ref)
            if (
                _path(admitted.path) != target
                or admitted.size_bytes != before.st_size
            ):
                raise ValueError("campaign artifact size/path differs")
        flags = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW
        descriptor = os.open(target, flags)
        digest = hashlib.sha256()
        chunks: list[bytes] = []
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or _identity(
                opened
            ) != _identity(before):
                raise ValueError("campaign evidence changed while opening")
            size = 0
            while chunk := os.read(descriptor, 1024 * 1024):
                size += len(chunk)
                if size > before.st_size:
                    raise ValueError("campaign evidence grew while reading")
                digest.update(chunk)
                if control:
                    chunks.append(chunk)
            if size != before.st_size or _identity(
                os.fstat(descriptor)
            ) != _identity(before):
                raise ValueError("campaign evidence changed while reading")
        finally:
            os.close(descriptor)
        if _identity(target.lstat()) != _identity(before):
            raise ValueError("campaign evidence replaced while reading")
        actual = digest.hexdigest()
        if ref is not None and actual != ref.sha256:
            raise ValueError("campaign artifact SHA-256 differs")
        snapshot = _Snapshot(target, _identity(before), actual)
        if target in self.files and self.files[target] != snapshot:
            raise ValueError("campaign evidence changed during verification")
        self.files[target] = snapshot
        return b"".join(chunks)

    def document(
        self, path: str | Path, ref: ArtifactRef | None = None
    ) -> dict[str, Any]:
        return _json(self.read(path, ref=ref, control=True))

    def native(
        self, path: str | Path, cls: Any, ref: ArtifactRef | None = None
    ) -> Any:
        payload = self.document(path, ref)
        result = cls.from_dict(payload)
        if type(result) is not cls:
            raise TypeError(
                "native campaign reader returned an unexpected type"
            )
        _same(
            result.to_dict(), payload, "native campaign canonical fields differ"
        )
        return result

    def ref(
        self,
        path: str | Path,
        kind: str,
        metadata: dict[str, Any] | None = None,
    ) -> CampaignArtifactRefV1:
        target = _path(path)
        self.read(target)
        snapshot = self.files[target]
        return CampaignArtifactRefV1(
            kind,
            str(target),
            snapshot.identity[3],
            snapshot.sha256,
            canonical(metadata or {}),
        )

    def graph(self, payload: Any, *, relative_root: Path | None = None) -> None:
        """Verify every declared nested strong ref, including native sources."""
        stack = [payload]
        while stack:
            value = stack.pop()
            if type(value) is list:
                stack.extend(value)
            elif type(value) is dict:
                if {"kind", "path", "sha256", "size_bytes"} <= set(value):
                    if set(value) != {
                        "kind",
                        "path",
                        "sha256",
                        "size_bytes",
                        "metadata",
                    }:
                        raise ValueError(
                            "native artifact reference fields differ"
                        )
                    ref = ArtifactRef.from_dict(value)
                    _same(
                        ref.to_dict(),
                        value,
                        "native artifact reference coercion",
                    )
                    if not Path(ref.path).is_absolute():
                        if (
                            relative_root is None
                            or ".." in Path(ref.path).parts
                        ):
                            raise ValueError(
                                "native relative artifact lacks its product root"
                            )
                        ref = ArtifactRef(
                            ref.kind,
                            str(relative_root / ref.path),
                            ref.size_bytes,
                            ref.sha256,
                            dict(ref.metadata),
                        )
                    self.read(ref.path, ref=ref)
                    target = _path(ref.path)
                    if (
                        target.suffix == ".json"
                        and target not in self.graph_seen
                    ):
                        self.graph_seen.add(target)
                        stack.append(self.document(target, ref))
                else:
                    stack.extend(value.values())

    def finish(self) -> str:
        digest = hashlib.sha256(b"histdatacom.campaign-inputs.v1\n")
        for path in sorted(self.files):
            snapshot = self.files[path]
            self.read(path)
            digest.update(
                canonical(
                    {
                        "path": str(path),
                        "sha256": snapshot.sha256,
                        "size_bytes": snapshot.identity[3],
                    }
                ).encode("ascii")
                + b"\n"
            )
        return digest.hexdigest()


def _shards(index: Any, guard: _Guard) -> Iterator[Any]:
    from histdatacom import reconstruction as native

    for ref in index.shard_refs:
        shard = guard.native(
            ref.path, native.ReconstructionCampaignProductShardV1, ref
        )
        required = {
            "product_shard_id": shard.product_shard_id,
            "plan_set_id": shard.plan_set_id,
            "plan_id": shard.plan_id,
            "shard_id": shard.shard_id,
            "requested_start_ns": shard.requested_start_ns,
            "requested_end_ns": shard.requested_end_ns,
            "projection_burden_receipt_count": len(
                shard.projection_burden_receipt_ids
            ),
            "status": shard.status,
            **{name: getattr(shard, name) for name in _COUNT_FIELDS},
        }
        for key, actual in required.items():
            _same(
                ref.metadata.get(key),
                actual,
                f"campaign shard {key} metadata differs",
            )
        if (
            shard.plan_set_id != index.plan_set_id
            or shard.support_artifact_id != index.support_artifact_id
        ):
            raise ValueError("campaign shard parent binding differs")
        yield shard


def _structural(path: str | Path, guard: _Guard) -> tuple[Any, dict[str, Any]]:
    from histdatacom import reconstruction as native

    index = guard.native(path, native.ReconstructionCampaignProductIndexV1)
    totals = dict.fromkeys(_COUNT_FIELDS, 0)
    digest = hashlib.sha256(b"histdatacom.campaign-shard-rows.v1\n")
    seen: set[str] = set()
    for shard in _shards(index, guard):
        if shard.shard_id in seen:
            raise ValueError("duplicate campaign shard identity")
        seen.add(shard.shard_id)
        for name in totals:
            totals[name] += getattr(shard, name)
        for row in shard.entries:
            digest.update(_wire(row.to_dict()).encode("ascii") + b"\n")
    for name, value in totals.items():
        _same(getattr(index, name), value, f"campaign actual {name} differs")
    fields = {
        "index_ref": guard.ref(
            path, "reconstruction_campaign_product_index_v1"
        ),
        "index_id": index.product_index_id,
        "plan_set_id": index.plan_set_id,
        "support_artifact_id": index.support_artifact_id,
        "status": (
            "incomplete" if totals["missing_product_count"] else "complete"
        ),
        "shard_count": len(seen),
        "shard_rows_sha256": digest.hexdigest(),
        **totals,
    }
    fields["product_count"] = fields.pop("verified_product_count")
    return index, fields


def inspect_campaign_product_index(
    index_path: str | Path,
) -> CampaignStructuralVerificationV1:
    """Inspect actual index/shard identities and counts, not product bytes."""
    guard = _Guard()
    _, fields = _structural(index_path, guard)
    return CampaignStructuralVerificationV1(
        **fields,
        verified_inputs_sha256=guard.finish(),
    )


def _plans(index: Any, guard: _Guard) -> tuple[Any, dict[str, tuple[Any, Any]]]:
    from histdatacom import reconstruction as native
    from histdatacom.synthetic.reconstruction_plan import SyntheticInfillPlanV1

    plan_set = guard.native(
        index.plan_set_ref.path,
        native.ReconstructionPlanSetV1,
        index.plan_set_ref,
    )
    _same(
        plan_set.plan_set_id,
        index.plan_set_id,
        "campaign plan-set identity differs",
    )
    _same(
        (plan_set.requested_start_ns, plan_set.requested_end_ns),
        (index.requested_start_ns, index.requested_end_ns),
        "campaign plan-set interval differs",
    )
    result: dict[str, tuple[Any, Any]] = {}
    run_ids: set[str] = set()
    total_plan_bytes = 0
    for shard in plan_set.shards:
        if type(shard.plan_ref.size_bytes) is not int:
            raise ValueError("campaign plan lacks exact byte size")
        total_plan_bytes += shard.plan_ref.size_bytes
        if total_plan_bytes > MAX_RETAINED_PLAN_BYTES:
            raise ValueError("campaign retained-plan aggregate bound exceeded")
        plan = guard.native(
            shard.plan_ref.path, SyntheticInfillPlanV1, shard.plan_ref
        )
        if plan.run.run_id in run_ids:
            raise ValueError("campaign plan shards duplicate a run")
        run_ids.add(plan.run.run_id)
        if (plan.plan_id, plan.requested_start_ns, plan.requested_end_ns) != (
            shard.plan_id,
            shard.requested_start_ns,
            shard.requested_end_ns,
        ):
            raise ValueError("campaign plan shard content differs")
        _same(
            shard.resource_summary,
            plan.resources.to_dict(),
            "campaign plan shard resource summary differs",
        )
        if (
            not shard.executable
            or shard.preflight_status != plan.status
            or shard.refusal_count != len(plan.refusals)
            or shard.empty_window_count != plan.resources.empty_window_count
        ):
            raise ValueError("campaign plan shard outcomes differ")
        guard.graph(plan.to_dict())
        result[shard.shard_id] = (shard, plan)
    _validate_support(index, plan_set, guard, result)
    return plan_set, result


def _validate_support(
    index: Any, plan_set: Any, guard: _Guard, plans: Any
) -> None:
    from histdatacom import reconstruction as native

    support_ref = index.support_map_ref
    if support_ref.kind == "reconstruction_plan_support_map_v1":
        support = guard.native(
            support_ref.path, native.ReconstructionPlanSupportMapV1, support_ref
        )
        expected = native._build_reconstruction_plan_support_map(plan_set)
        _same(
            support.to_dict(),
            expected.to_dict(),
            "campaign support differs from actual plans",
        )
    else:
        support = guard.native(
            support_ref.path,
            native.ReconstructionPlanSupportMapIndexV2,
            support_ref,
        )
        if len(support.shard_refs) != len(plan_set.shards):
            raise ValueError("campaign support shard denominator differs")
        for ref, shard in zip(support.shard_refs, plan_set.shards, strict=True):
            actual = guard.native(
                ref.path, native.ReconstructionPlanSupportMapV1, ref
            )
            expected = native._build_reconstruction_plan_support_map_shard(
                plan_set, shard
            )
            _same(
                actual.to_dict(),
                expected.to_dict(),
                "campaign support shard differs from actual plan",
            )
            for key, value in {
                "support_map_id": actual.support_map_id,
                "plan_set_id": actual.plan_set_id,
                "requested_start_ns": actual.requested_start_ns,
                "requested_end_ns": actual.requested_end_ns,
                "window_count": len(actual.windows),
                "executable_window_count": actual.executable_window_count,
                "refused_window_count": actual.refused_window_count,
                "empty_window_count": actual.empty_window_count,
                "status": actual.status,
            }.items():
                _same(
                    ref.metadata.get(key),
                    value,
                    f"campaign support metadata {key} differs",
                )
    _same(
        support.plan_set_id,
        plan_set.plan_set_id,
        "support plan-set binding differs",
    )
    actual_id = (
        getattr(support, "support_map_id", None) or support.support_map_index_id
    )
    _same(actual_id, index.support_artifact_id, "support artifact ID differs")
    engines = support.selected_proposal_engine_ids
    if not engines:
        from histdatacom.synthetic.generation import (
            EMPIRICAL_MOTIF_GENERATOR_ID,
        )
        from histdatacom.synthetic.reconstruction_plan import (
            ReconstructionPlanConfigurationV1,
        )

        # V1 support retained no engine roster; its actual native dispatcher
        # admits exactly empirical motifs. Do not reinterpret empty V2 rosters.
        for _, plan in plans.values():
            ref = plan.artifact_graph["configuration"]
            config = guard.native(
                ref.path, ReconstructionPlanConfigurationV1, ref
            )
            if config.configuration_id != plan.configuration_id:
                raise ValueError(
                    "legacy campaign configuration identity differs"
                )
        engines = (EMPIRICAL_MOTIF_GENERATOR_ID,)
    _same(
        engines,
        index.selected_proposal_engine_ids,
        "campaign engine roster differs",
    )


def _discover(
    roots: tuple[str, ...], guard: _Guard
) -> Iterator[tuple[Path, Any]]:
    from histdatacom.synthetic.persistence import (
        RECONSTRUCTION_MANIFEST_ARTIFACT_KIND,
        _reconstruction_product_root_from_path,
        verify_reconstruction_publication,
    )

    count = 0
    seen: set[Path] = set()
    for root in roots:
        for path in iter_campaign_manifest_paths(root):
            if path in seen:
                continue
            seen.add(path)
            count += 1
            if count > MAX_DISCOVERED_PRODUCTS:
                raise ValueError("campaign discovery product bound exceeded")
            payload = guard.document(path)
            guard.graph(
                payload,
                relative_root=_reconstruction_product_root_from_path(
                    path.parent
                ),
            )
            # Partitions are relative, while V3 observed source refs are nested.
            for partition in payload.get("partitions", ()):
                relative = partition["relative_path"]
                part = Path(relative)
                if part.is_absolute() or ".." in part.parts:
                    raise ValueError("campaign partition escapes its product")
                guard.read(
                    path.parent / part,
                    ref=ArtifactRef(
                        "reconstruction_partition",
                        str(path.parent / part),
                        partition["size_bytes"],
                        partition["byte_sha256"],
                    ),
                )
            manifest = verify_reconstruction_publication(path)
            _same(
                manifest.to_dict(),
                payload,
                "native product manifest canonical fields differ",
            )
            guard.ref(path, RECONSTRUCTION_MANIFEST_ARTIFACT_KIND)
            yield path, manifest


def _terminal_support_intervals(
    plan_set: Any, plans: Any
) -> dict[str, tuple[tuple[int, int], ...]]:
    """Derive run-wide terminal scopes from the same native support graph.

    Empty/refused support has no member roster. It excludes products for every
    member of that run, not merely a synthetic ``None`` member coordinate.
    """
    from histdatacom import reconstruction as native

    result: dict[str, tuple[tuple[int, int], ...]] = {}
    count = 0
    for descriptor, plan in plans.values():
        support = native._build_reconstruction_plan_support_map_shard(
            plan_set, descriptor
        )
        intervals = []
        for window in support.windows:
            if window.status not in {"empty", "refused"}:
                continue
            if count >= MAX_CAMPAIGN_COORDINATES:
                raise ValueError("campaign terminal interval bound exceeded")
            count += 1
            intervals.append((window.start_ns, window.end_ns))
        ordered = tuple(sorted(intervals))
        if any(left[1] > right[0] for left, right in pairwise(ordered)):
            raise ValueError("campaign terminal support intervals overlap")
        if ordered:
            result[plan.run.run_id] = ordered
    return result


def _refuse_terminal_event_times(
    run_id: str,
    terminal_intervals: dict[str, tuple[tuple[int, int], ...]],
    event_times: Iterable[int],
) -> None:
    """Pure half-open interval check; not a product-validation authority."""
    intervals = terminal_intervals.get(run_id, ())
    if not intervals:
        return
    for event_time in event_times:
        position = (
            bisect_right(intervals, event_time, key=lambda item: item[0]) - 1
        )
        if position >= 0 and event_time < intervals[position][1]:
            raise ValueError(
                "campaign product has events in same-run empty/refused support"
            )


def _refuse_terminal_product(
    path: Path,
    manifest: Any,
    terminal_intervals: dict[str, tuple[tuple[int, int], ...]],
) -> None:
    if manifest.run_id not in terminal_intervals:
        return
    from histdatacom.synthetic.persistence import read_reconstruction_streams

    # Native V3 replay verifies logical hashes/counts, not its declared logical
    # min/max fields. Read the actual reconstructed rows: a false range/window
    # label cannot hide a terminal event, and a gap between events is not one.
    streams = read_reconstruction_streams(path)
    _refuse_terminal_event_times(
        manifest.run_id,
        terminal_intervals,
        (event.event_time_ns for stream in streams for event in stream.events),
    )


def _verify_shard(
    shard: Any,
    descriptor: Any,
    plan: Any,
    index: Any,
    products: Any,
    guard: _Guard,
    product_digest: Any,
    seen_coordinates: set[Any],
    plan_set: Any,
) -> None:
    from histdatacom import reconstruction as native

    expected_support = native._build_reconstruction_plan_support_map_shard(
        plan_set, descriptor
    )
    support_by_id = {item.support_id: item for item in expected_support.windows}
    tasks = {
        (
            task.window.core_start_ns,
            task.window.core_end_ns,
            task.window.ensemble_member_id,
        ): task
        for request in plan.workflow_requests
        for task in request.tasks
    }
    expected_rows = {
        (support.support_id, member)
        for support in expected_support.windows
        for member in (
            support.member_ids if support.status == "executable" else (None,)
        )
    }
    if (shard.plan_id, shard.requested_start_ns, shard.requested_end_ns) != (
        plan.plan_id,
        descriptor.requested_start_ns,
        descriptor.requested_end_ns,
    ):
        raise ValueError("campaign product shard differs from planned bounds")
    actual_rows: set[Any] = set()
    for row in shard.entries:
        pair = (row.support_id, row.ensemble_member_id)
        if pair in actual_rows or pair not in expected_rows:
            raise ValueError(
                "campaign row is duplicate or outside actual support"
            )
        actual_rows.add(pair)
        support = support_by_id[row.support_id]
        if (row.plan_id, row.shard_id, row.start_ns, row.end_ns) != (
            plan.plan_id,
            descriptor.shard_id,
            support.start_ns,
            support.end_ns,
        ):
            raise ValueError("campaign row plan/support binding differs")
        if support.status != "executable":
            reason = (
                support.empty_code
                if support.status == "empty"
                else support.refusal_code
            )
            if (
                row.status != support.status
                or row.reason_code != reason
                or row.product_ref is not None
            ):
                raise ValueError(
                    "campaign empty/refused support was reinterpreted"
                )
            continue
        task = tasks[(support.start_ns, support.end_ns, row.ensemble_member_id)]
        key = (
            plan.run.run_id,
            task.window.window_id,
            task.window.ensemble_member_id,
        )
        seen_coordinates.add(key)
        if row.window_id != task.window.window_id:
            raise ValueError("campaign row window differs from executable task")
        actual = products.get(key)
        if actual is None:
            if (
                row.status != "missing_product"
                or row.reason_code != "missing_committed_product"
            ):
                raise ValueError(
                    "campaign absent product is not explicitly missing"
                )
            continue
        if row.status != "verified_product" or row.product_ref is None:
            raise ValueError(
                "campaign index omits an existing required product"
            )
        path, manifest = actual
        guard.read(path, ref=row.product_ref)
        if _path(row.product_ref.path) != path:
            raise ValueError(
                "campaign row points outside discovered committed product"
            )
        for name in (
            "manifest_id",
            "publication_id",
            "run_id",
            "window_id",
            "ensemble_member_id",
            "delivery_profile_id",
            "observed_event_count",
            "synthetic_event_count",
        ):
            _same(
                row.product_ref.metadata.get(name),
                getattr(manifest, name),
                f"product metadata {name} differs",
            )
        _same(
            row.product_ref.metadata.get("logical_content_sha256"),
            manifest.replay.logical_content_sha256,
            "product logical hash metadata differs",
        )
        if (row.observed_event_count, row.synthetic_event_count) != (
            manifest.observed_event_count,
            manifest.synthetic_event_count,
        ):
            raise ValueError("campaign row event counts differ")
        verified = _verify_product(
            plan, task, row, path, manifest, index, guard
        )
        product_digest.update(verified.to_json().encode("ascii") + b"\n")
    if actual_rows != expected_rows:
        raise ValueError("campaign row denominator omits planned outcomes")


def _source_context(invocation: Any, stage_plan: Any) -> tuple[Any, ...]:
    """Repeat the native source/context mathematics without scratch writes."""
    from histdatacom.cross_series_constraints import (
        CrossSeriesConstraintUseStatus,
        CrossSeriesSourceBindingV1,
        compile_histdata_cross_series_constraints,
        cross_series_constraint_use,
    )
    from histdatacom.reconstruction_evidence import (
        ReconstructionEvidenceUseStatus,
        compile_histdata_point_in_time_evidence,
        reconstruction_evidence_use,
    )
    from histdatacom.synthetic import reconstruction_handlers as handlers

    window = invocation.task.window
    events, cached = handlers._read_source_events(invocation, stage_plan)
    core = {
        symbol: tuple(
            event
            for event in rows
            if window.owns_event_time(event.event_time_ns)
        )
        for symbol, rows in events.items()
    }
    if any(len(rows) < 2 for rows in events.values()) or any(
        not rows for rows in core.values()
    ):
        raise ValueError("campaign source lacks real triangle anchors")
    if (
        sum(map(len, events.values()))
        != invocation.task.resource_estimate.input_event_count
    ):
        raise ValueError(
            "campaign source row count differs from actual plan preflight"
        )
    policy = handlers._read_evidence_policy(stage_plan)
    used_at = handlers._evidence_stage_used_at(stage_plan, invocation)
    mode = stage_plan.configuration.information_policy.information_mode
    projections = []
    partitions = stage_plan.source_inventory.partitions_for_window(window)
    for partition in partitions:
        rows = tuple(
            sorted(
                (
                    row
                    for row in events[partition.symbol]
                    if row.source_period == partition.period
                    and row.source_series_id is not None
                    and row.source_series_id.endswith(partition.artifact.sha256)
                ),
                key=lambda row: cast(int, row.source_row_id),
            )
        )
        lower = max(window.input_start_ns, partition.coverage_start_ns)
        upper = min(window.input_end_ns, partition.coverage_end_ns)
        if upper <= lower:
            continue
        schema, evidence, complete = cached[partition.partition_id]
        projections.append(
            compile_histdata_point_in_time_evidence(
                rows,
                evidence_window_id=window.window_id,
                source_partition_id=partition.partition_id,
                source_artifact_id=f"{partition.artifact.kind}:sha256:{partition.artifact.sha256}",
                source_artifact_sha256=partition.artifact.sha256,
                symbol=partition.symbol,
                period=partition.period,
                support_start_ns=lower,
                support_end_ns=upper,
                available_at_ns=window.core_end_ns,
                as_of_ns=used_at,
                information_mode=mode,
                policy=policy,
                source_cache_schema_version=schema,
                cached_row_evidence=evidence,
                cached_row_evidence_complete=complete,
            )
        )
    uses = {
        symbol: reconstruction_evidence_use(
            tuple(item for item in projections if item.symbol == symbol),
            stage="source_enrichment",
            used_at_ns=used_at,
            policy=policy,
        )
        for symbol in invocation.run.symbols
    }
    final_use = reconstruction_evidence_use(
        tuple(
            sorted(
                projections, key=lambda item: (item.symbol, item.projection_id)
            )
        ),
        stage="validation",
        used_at_ns=used_at,
        policy=policy,
    )
    if final_use.status is ReconstructionEvidenceUseStatus.REFUSED or any(
        item.status is ReconstructionEvidenceUseStatus.REFUSED
        for item in uses.values()
    ):
        raise ValueError("campaign source evidence refuses native validation")
    cross_policy = handlers._read_cross_series_constraint_policy(stage_plan)
    bindings = tuple(
        CrossSeriesSourceBindingV1(
            provider_id="histdata.com",
            dataset_version_id=invocation.run.source_version_ids[0],
            symbol=partition.symbol,
            period=partition.period,
            series_id=f"ascii-tick:{partition.symbol}:{partition.period}:sha256:{partition.artifact.sha256}",
            source_partition_id=partition.partition_id,
            source_artifact_id=f"{partition.artifact.kind}:sha256:{partition.artifact.sha256}",
            source_artifact_sha256=partition.artifact.sha256,
        )
        for partition in partitions
    )
    bundle = compile_histdata_cross_series_constraints(
        core,
        source_bindings=bindings,
        synchronization_unit_id=window.synchronization_unit_id,
        evidence_window_id=window.window_id,
        dataset_version_ids=invocation.run.source_version_ids,
        support_start_ns=window.core_start_ns,
        support_end_ns=window.core_end_ns,
        available_at_ns=window.core_end_ns,
        as_of_ns=used_at,
        information_mode=mode,
        policy=cross_policy,
    )
    cross_use = cross_series_constraint_use(
        (bundle,),
        stage="validation",
        used_at_ns=used_at,
        policy=cross_policy,
    )
    if cross_use.status is CrossSeriesConstraintUseStatus.REFUSED:
        raise ValueError("campaign cross-series evidence refuses validation")
    support = handlers._require_planned_cross_series_support(
        invocation, stage_plan, bundle
    )
    join, maximum_age = handlers._cross_currency_join_contract(
        stage_plan, support
    )
    context, positioning = handlers._window_context(stage_plan, invocation)
    operator = handlers.read_observation_operator_artifact(
        stage_plan.execution_manifest.artifacts["observation_operator"]
    )
    if window.left_halo_ns < operator.required_left_halo_ns:
        raise ValueError(
            "campaign source halo is shorter than operator support"
        )
    scientific = handlers._source_scientific_conditioning(
        stage_plan,
        context=context,
        positioning=positioning,
        cftc_conditioning=handlers._cftc_conditioning_evidence(
            stage_plan, positioning
        ),
    )
    conditions = handlers._motif_conditions(
        stage_plan,
        invocation,
        events,
        context=context,
        positioning=positioning,
        evidence_uses=uses,
    )
    cross_condition = handlers._cross_condition(
        invocation, conditions, context=context
    )
    return (
        events,
        core,
        conditions,
        cross_condition,
        join,
        maximum_age,
        bundle,
        scientific,
        final_use,
        cross_use,
        projections,
        context,
    )


def _local_support_invariants(
    event: Any,
    anchors: Any,
    condition: Any,
    context: Any,
    constraints: Any,
    evidence: Any,
    run: Any,
    cross_config: Any,
) -> dict[str, Any]:
    """Recompute only invariants preserved by final cross projection.

    A native ReferenceMotifCondition supplies exactly the three policy-token
    attributes consumed by _matching_policies/_closed_session. It is not a
    forged candidate batch and does not attest generation or candidate quotes.
    """
    from histdatacom.synthetic import carving
    from histdatacom.synthetic import reconstruction_handlers as handlers
    from histdatacom.synthetic.contracts import derive_anchor_interval_id

    if event.anchor_interval_id != derive_anchor_interval_id(
        anchors[0].event_id, anchors[1].event_id
    ):
        raise ValueError("product anchor interval identity differs")
    gap = handlers._optional_int_effect(evidence, "max_anchor_gap_ns")
    if anchors[1].event_time_ns - anchors[0].event_time_ns > min(
        constraints.max_anchor_gap_ns,
        gap if gap is not None else constraints.max_anchor_gap_ns,
    ):
        raise ValueError("product violates native local anchor-gap support")
    if (
        context.missing_reason is not None
        and context.missing_reason.value != "no_matching_event"
    ):
        raise ValueError("product lacks native market-context support")
    if context.calendar_state is None or (
        constraints.require_complete_calendar_profile
        and not context.calendar_state.profile_complete
    ):
        raise ValueError("product lacks native complete calendar support")
    if carving._closed_session(cast(Any, condition), context, constraints):
        raise ValueError("product contains a native closed-session event")
    if any(
        item.symbol == event.symbol.upper()
        and item.start_ns <= event.event_time_ns < item.end_ns
        for item in constraints.quarantines
    ):
        raise ValueError("product contains a native quarantined event")
    if constraints.require_fingerprint_validation:
        # The current first-party handler supplies None, never an invented
        # passing fingerprint report. Such a policy cannot emit accepted rows.
        raise ValueError(
            "product requires unavailable native fingerprint evidence"
        )
    policies, _ = carving._matching_policies(
        event, cast(Any, condition), context, constraints
    )
    if not carving._motif_eligible(event, policies):
        raise ValueError(
            "product violates native conditioned motif eligibility"
        )
    if carving._acceptance_score(
        run, event, constraints, policies
    ) >= math.prod(item.acceptance_rate for item in policies):
        raise ValueError(
            "product violates native deterministic intensity thinning"
        )
    multiplier = math.prod(item.spread_multiplier for item in policies)
    if multiplier > constraints.max_combined_spread_multiplier:
        raise ValueError(
            "product conditioned spread multiplier exceeds native bound"
        )
    threshold = handlers._optional_float_effect(
        evidence, "wide_spread_threshold"
    )
    spread_scope = "pre_cross_candidate_quote_not_retained"
    if threshold is not None and cross_config.max_projection_relative == 0.0:
        # Carving may scale the pre-carve spread and round each price. With no
        # nonzero cross projection the necessary final bound is reconstructible.
        rounding = (
            10.0**-constraints.price_precision_digits
            if not math.isclose(multiplier, 1.0, rel_tol=0.0, abs_tol=1e-15)
            else 0.0
        )
        cap = (
            threshold * constraints.max_combined_spread_multiplier * multiplier
            + rounding
        )
        if event.ask - event.bid > math.nextafter(cap, math.inf):
            raise ValueError(
                "product violates reconstructible native wide-spread bound"
            )
        spread_scope = "zero_cross_projection_final_spread_bound"
    return {
        "policy_ids": [item.policy_id for item in policies],
        "spread_check_scope": spread_scope,
    }


def _runtime_scope(
    stage: Any,
    invocation: Any,
    conditions: Any,
    source_events: Any,
    guard: _Guard,
) -> tuple[str, Any, dict[str, Any]]:
    """Recompute exact assignment/scenario metadata, not stochastic paths."""
    from histdatacom.synthetic import reconstruction_handlers as handlers
    from histdatacom.synthetic.generation import (
        EMPIRICAL_MOTIF_GENERATOR_ID,
        EmpiricalMotifGeneratorConfigV1,
    )
    from histdatacom.synthetic.marked_hawkes import (
        MarkedHawkesConfigV1,
        MarkedHawkesFitResultV1,
    )
    from histdatacom.synthetic.proposal_engines import (
        ProposalEnginePortfolioV1,
        ProposalEngineRegistryV1,
        proposal_engine_registry,
        read_proposal_engine_fit_artifact,
    )
    from histdatacom.synthetic.reconstruction_plan import (
        ReconstructionPlanConfigurationV2,
    )

    config = stage.configuration.generator_config
    engine = EMPIRICAL_MOTIF_GENERATOR_ID
    fields: dict[str, Any] = {
        "proposal_engine_id": engine,
        "proposal_engine_registry_id": None,
        "proposal_portfolio_id": None,
        "proposal_binding_id": None,
        "proposal_eligibility_audit_id": None,
        "generator_config_id": config.config_id,
        "generation_scenario": None,
        "historical_product_observation_conditioning": None,
        "observation_uncertainty_ensemble_id": None,
        "observation_scenario_id": None,
        "observation_scenario_kind": None,
        "observation_path_seed": None,
        "feed_epoch_transition_policy_id": None,
        "transition_scenario_id": None,
        "transition_scenario_kind": None,
        "transition_boundary_id": None,
    }
    binding = None
    if type(stage.configuration) is ReconstructionPlanConfigurationV2:
        artifacts = stage.execution_manifest.artifacts
        ref = artifacts["proposal_engine_portfolio"]
        portfolio = guard.native(ref.path, ProposalEnginePortfolioV1, ref)
        ref = artifacts["proposal_engine_registry"]
        registry = guard.native(ref.path, ProposalEngineRegistryV1, ref)
        _same(
            portfolio.to_dict(),
            stage.configuration.proposal_portfolio.to_dict(),
            "campaign portfolio differs from configuration",
        )
        _same(
            registry.to_dict(),
            proposal_engine_registry().to_dict(),
            "campaign engine registry differs from installed native code",
        )
        if portfolio.registry_id != registry.registry_id:
            raise ValueError("campaign portfolio registry binding differs")
        engine = handlers._assigned_proposal_engine(
            portfolio,
            ensemble_member_id=invocation.task.window.ensemble_member_id,
            ensemble_member_ids=invocation.run.ensemble_member_ids,
        )
        binding = portfolio.binding(engine)
        audit = next(
            item
            for item in portfolio.eligibility_audits
            if item.engine_id == engine
        )
        if not audit.reconstruction_eligible:
            raise ValueError(
                "campaign planned engine is not reconstruction-eligible"
            )
        cls = (
            EmpiricalMotifGeneratorConfigV1
            if engine == EMPIRICAL_MOTIF_GENERATOR_ID
            else MarkedHawkesConfigV1
        )
        if engine != EMPIRICAL_MOTIF_GENERATOR_ID and not engine.startswith(
            "histdatacom.marked-hawkes."
        ):
            raise ValueError("unsupported campaign native engine family")
        config = guard.native(binding.config_ref.path, cls, binding.config_ref)
        if config.config_id != binding.config_id:
            raise ValueError("campaign generator config binding differs")
        fields.update(
            {
                "proposal_engine_id": engine,
                "proposal_engine_registry_id": registry.registry_id,
                "proposal_portfolio_id": portfolio.portfolio_id,
                "proposal_binding_id": binding.binding_id,
                "proposal_eligibility_audit_id": audit.audit_id,
                "generator_config_id": config.config_id,
            }
        )
    if engine != EMPIRICAL_MOTIF_GENERATOR_ID:
        if binding is None or binding.fit_ref is None:
            raise ValueError("campaign marked engine lacks exact retained fit")
        fit = guard.native(
            binding.fit_ref.path, MarkedHawkesFitResultV1, binding.fit_ref
        )
        _same(
            read_proposal_engine_fit_artifact(binding.fit_ref).to_dict(),
            fit.to_dict(),
            "campaign fit strong reference differs",
        )
        if (
            fit.config_id != config.config_id
            or fit.information_mode.value != "ex_post_reconstruction"
        ):
            raise ValueError("campaign marked fit is not ex-post")
        _marked_scenario(
            stage, invocation, config, conditions, source_events, fields
        )
    return engine, config, fields


def _marked_scenario(
    stage: Any,
    invocation: Any,
    config: Any,
    conditions: Any,
    events: Any,
    fields: dict[str, Any],
) -> None:
    from histdatacom.synthetic import reconstruction_handlers as handlers
    from histdatacom.synthetic.benchmark import (
        BenchmarkScenarioV1,
        BenchmarkSplitKind,
    )
    from histdatacom.synthetic.ensembles import ReconstructionEnsemblePlanV1
    from histdatacom.synthetic.feed_epoch_transition import (
        read_feed_epoch_transition_policy,
        transition_scenario_kind_for_member,
    )
    from histdatacom.synthetic.observation_uncertainty import (
        build_observation_uncertainty_ensemble,
        read_observation_uncertainty_policy,
    )

    artifacts = stage.execution_manifest.artifacts
    operator = handlers.read_observation_operator_artifact(
        artifacts["observation_operator"]
    )
    definition = handlers.read_active_time_feed_epoch_definition(
        artifacts["feed_epochs"].path
    )
    ensemble = ReconstructionEnsemblePlanV1.from_json(
        Path(artifacts["ensemble_plan"].path).read_text(encoding="utf-8")
    )
    epochs = {item.feed_epoch_id for item in conditions.values()}
    sessions = {item.session_state for item in conditions.values()}
    if len(epochs) != 1 or len(sessions) != 1:
        raise ValueError(
            "campaign marked engine lacks synchronized epoch/session"
        )
    transition_policy = transition_kind = None
    if next(iter(epochs)).startswith("transition:"):
        ref = artifacts.get("feed_epoch_transition_policy")
        if ref is None:
            raise ValueError("campaign transition lacks policy")
        transition_policy = read_feed_epoch_transition_policy(ref.path)
        ordinals = {
            item.member_id: ordinal
            for ordinal, item in enumerate(ensemble.members, start=1)
        }
        transition_kind = transition_scenario_kind_for_member(
            transition_policy,
            member_ordinal=ordinals[invocation.task.window.ensemble_member_id],
            observation_scenario_count=3,
        )
    conditioning = handlers._historical_product_observation_conditioning(
        operator,
        conditions=conditions,
        feed_epoch_definition=definition,
        used_at_ns=(
            invocation.task.window.core_start_ns
            + invocation.task.window.core_end_ns
        )
        // 2,
        information_mode=stage.configuration.information_policy.information_mode,
        transition_policy=transition_policy,
        transition_scenario_kind=transition_kind,
    )
    fields["historical_product_observation_conditioning"] = conditioning
    joint = conditioning["joint_retention"]
    if type(joint) is not dict:
        raise TypeError("campaign joint retention must be an object")
    extra: dict[str, Any] = {
        "legacy_observation_uncertainty_policy": "v2.4-point-estimate-replay-not-v2.5-scenario-v1"
    }
    cardinality = (
        "synchronized-epoch-point-estimate-with-bounded-uncertainty-v1"
    )
    retention = _number(joint["retention_probability"])
    ref = artifacts.get("observation_uncertainty_policy")
    if ref is not None:
        counts = {symbol.upper(): len(rows) for symbol, rows in events.items()}
        counts["GLOBAL"] = sum(counts.values())
        uncertainty = build_observation_uncertainty_ensemble(
            read_observation_uncertainty_policy(ref.path),
            conditioning,
            ensemble_members=tuple(
                (item.member_id, item.seed) for item in ensemble.members
            ),
            observed_counts=counts,
            session=next(iter(sessions)),
            maximum_missing_event_count=config.limits.max_generated_events_per_window,
            maximum_candidate_amplification=min(
                config.limits.max_candidate_amplification,
                invocation.run.storage_policy.max_candidate_amplification,
            ),
        )
        if not uncertainty.admitted:
            raise ValueError("campaign uncertainty cardinality is refused")
        worst = next(
            item
            for item in uncertainty.cardinality_evidence
            if item.symbol == "GLOBAL"
            and item.retention_probability
            == min(
                scenario.retention_probability
                for scenario in uncertainty.scenarios
            )
        )
        if (
            worst.admission_missing_count_bound
            != invocation.task.resource_estimate.candidate_event_count
        ):
            raise ValueError(
                "campaign uncertainty differs from candidate preflight"
            )
        member = uncertainty.member_for(
            invocation.task.window.ensemble_member_id
        )
        scenario = uncertainty.scenario_for(
            invocation.task.window.ensemble_member_id
        )
        extra = {
            "observation_scenario_id": scenario.scenario_id,
            "observation_scenario_kind": scenario.kind.value,
            "observation_path_seed": member.path_seed,
        }
        fields.update(extra)
        fields["observation_uncertainty_ensemble_id"] = uncertainty.ensemble_id
        cardinality = "synchronized-epoch-scenario-propagated-uncertainty-v1"
        retention = scenario.retention_probability
    if conditioning.get("transition_scenario_id"):
        for name in (
            "feed_epoch_transition_policy_id",
            "transition_scenario_id",
            "transition_scenario_kind",
            "transition_boundary_id",
        ):
            fields[name] = conditioning[name]
            extra[name] = conditioning[name]
    fields["generation_scenario"] = BenchmarkScenarioV1(
        split_kind=BenchmarkSplitKind.PRODUCT_INPUT,
        epoch_id=next(iter(epochs)),
        severity_id="historical-product-input-operator-conditioned-missingness",
        observation_operator_id=str(conditioning["observation_operator_id"]),
        degradation_parameters={
            "runtime_role": "historical_product_input",
            "missingness_identified": True,
            "cardinality_conditioning_policy": cardinality,
            "observation_conditioning_id": str(conditioning["conditioning_id"]),
            **extra,
            "retention_probability": retention,
            "retention_lower_bound": _number(joint["retention_lower_bound"]),
            "retention_upper_bound": _number(joint["retention_upper_bound"]),
        },
    ).to_dict()


def _verify_product(
    plan: Any,
    task: Any,
    row: Any,
    path: Path,
    manifest: Any,
    index: Any,
    guard: _Guard,
) -> CampaignProductVerificationV1:
    from histdatacom.orchestration.reconstruction import (
        ReconstructionStage,
        ReconstructionStageInvocationV1,
    )
    from histdatacom.reconstruction_experiment import (
        read_reconstruction_experiment,
    )
    from histdatacom.synthetic import persistence
    from histdatacom.synthetic.contracts import (
        SyntheticEventOrigin,
        SyntheticEventStreamV1,
    )
    from histdatacom.synthetic.cross_currency import (
        CrossCurrencyValidationStage,
        validate_cross_currency_output,
    )
    from histdatacom.synthetic.reconstruction_plan import (
        load_reconstruction_stage_plan,
    )

    command = next(
        item
        for item in task.commands
        if item.stage is ReconstructionStage.VALIDATION
    )
    _same(
        [ref.to_dict() for ref in command.configuration_refs],
        [plan.artifact_graph["execution_manifest"].to_dict()],
        "campaign task execution graph differs",
    )
    invocation = ReconstructionStageInvocationV1(plan.run, task, command, ())
    stage = load_reconstruction_stage_plan(command)
    if (
        stage.execution_manifest.run_id != plan.run.run_id
        or stage.configuration.configuration_id != plan.configuration_id
    ):
        raise ValueError("campaign loaded stage plan differs")
    if stage.configuration.delivery_mode.value != "modern_reference":
        raise ValueError(
            "campaign deep validation admits modern-reference delivery only"
        )
    _same(
        plan.run.source_version_ids,
        (index.observed_dataset_version_id,),
        "campaign source dataset differs",
    )
    _same(
        manifest.source.source_version_ids,
        plan.run.source_version_ids,
        "product source dataset differs",
    )
    _same(
        manifest.delivery_profile_id,
        "modern-reference:" + plan.configuration_id,
        "product delivery configuration differs",
    )
    _same(
        manifest.delivery_profile_id,
        index.delivery_profile_id,
        "product delivery differs from campaign",
    )
    _same(
        manifest.synchronization_unit_id,
        task.window.synchronization_unit_id,
        "product synchronization unit differs",
    )
    _same(
        manifest.symbols, plan.run.symbols, "product symbol rectangle differs"
    )
    if (
        manifest.synthetic_event_count
        > task.resource_estimate.candidate_event_count
    ):
        raise ValueError(
            "product synthetic count exceeds retained candidate bound"
        )
    _same(
        manifest.symbol_group_id,
        task.window.synchronization_unit_id,
        "product symbol group differs",
    )
    artifacts = stage.execution_manifest.artifacts
    experiment = read_reconstruction_experiment(
        artifacts["experiment_manifest"].path
    )
    _same(
        manifest.source.experiment_id,
        experiment.experiment_id,
        "product source experiment differs",
    )
    retention_ref = artifacts["retention_plan"]
    retention = guard.native(
        retention_ref.path,
        persistence.ReconstructionRetentionPlanV1,
        retention_ref,
    )
    _same(
        manifest.retention.to_dict(),
        retention.to_dict(),
        "product retention differs from actual plan",
    )
    _same(
        retention.plan_id,
        stage.execution_manifest.retention_plan_id,
        "execution retention identity differs",
    )
    _same(retention.run_id, plan.run.run_id, "retention run differs")
    _same(
        manifest.ensemble.to_dict(),
        persistence.ReconstructionEnsembleManifestV1(
            run_id=plan.run.run_id,
            materialized_member_id=task.window.ensemble_member_id,
            primary_member_id=retention.primary_member_id,
            retained_member_ids=retention.retained_member_ids,
            member_event_estimates=retention.member_event_counts,
            retention_plan_id=retention.plan_id,
        ).to_dict(),
        "product actual ensemble differs",
    )
    (
        events,
        core,
        conditions,
        condition,
        join,
        maximum_age,
        bundle,
        scientific,
        final_use,
        cross_use,
        projections,
        context,
    ) = _source_context(invocation, stage)
    engine, config, runtime = _runtime_scope(
        stage, invocation, conditions, events, guard
    )
    if engine not in index.selected_proposal_engine_ids:
        raise ValueError("assigned product engine is outside campaign roster")
    retained_runtime = manifest.quality.benchmark_evidence.get(
        "runtime_proposal_evidence"
    )
    if type(retained_runtime) is not dict:
        raise ValueError("product lacks exact runtime assignment evidence")
    for name in (
        "scientific_ledger_id",
        "estimand_id",
        "conditioning_state_ids",
        "invalid_for_backtest",
        "invalid_for_backtest_reason",
    ):
        _same(
            manifest.quality.benchmark_evidence.get(name),
            scientific[name],
            f"product scientific scope {name} differs",
        )
    for name, expected in runtime.items():
        _same(
            retained_runtime.get(name),
            expected,
            f"product runtime {name} differs",
        )
    for name in (
        "observation_uncertainty_ensemble_id",
        "observation_scenario_id",
        "observation_scenario_kind",
        "observation_path_seed",
        "feed_epoch_transition_policy_id",
        "transition_scenario_id",
        "transition_scenario_kind",
        "transition_boundary_id",
    ):
        _same(getattr(row, name), runtime[name], f"campaign row {name} differs")
        _same(
            row.product_ref.metadata.get(name),
            runtime[name],
            f"campaign product metadata {name} differs",
        )
    streams = persistence.read_reconstruction_streams(path)
    if {stream.symbol for stream in streams} != set(plan.run.symbols):
        raise ValueError("campaign output lacks complete symbol streams")
    _same(
        manifest.logical_min_event_time_ns,
        min(
            event.event_time_ns for stream in streams for event in stream.events
        ),
        "product logical minimum differs from actual events",
    )
    _same(
        manifest.logical_max_event_time_ns,
        max(
            event.event_time_ns for stream in streams for event in stream.events
        ),
        "product logical maximum differs from actual events",
    )
    local = []
    from histdatacom.reconstruction_evidence import (
        ReconstructionEvidenceUseStatus,
        reconstruction_evidence_use,
    )
    from histdatacom.synthetic import reconstruction_handlers as handlers

    for stream in streams:
        source = events[stream.symbol]
        pairs = {
            (left.event_id, right.event_id): (left, right)
            for left, right in pairwise(source)
        }
        observed = tuple(
            event
            for event in stream.events
            if event.origin is SyntheticEventOrigin.OBSERVED
        )
        generated = tuple(
            event
            for event in stream.events
            if event.origin is SyntheticEventOrigin.SYNTHETIC
        )
        evidence = reconstruction_evidence_use(
            tuple(
                sorted(
                    (
                        item
                        for item in projections
                        if item.symbol == stream.symbol
                    ),
                    key=lambda item: item.projection_id,
                )
            ),
            stage="carving",
            used_at_ns=handlers._evidence_stage_used_at(stage, invocation),
            policy=handlers._read_evidence_policy(stage),
        )
        if (
            generated
            and evidence.status is ReconstructionEvidenceUseStatus.REFUSED
        ):
            raise ValueError("product has events under refused source evidence")
        support_checks = []
        _same(
            [event.to_dict() for event in observed],
            [event.to_dict() for event in core[stream.symbol]],
            "product observed anchors differ from actual source window",
        )
        for event in stream.events:
            if not task.window.owns_event_time(event.event_time_ns):
                raise ValueError("product event is outside its core window")
        for event in generated:
            anchors = pairs.get(
                (event.left_anchor_event_id, event.right_anchor_event_id)
            )
            if (
                anchors is None
                or not anchors[0].event_time_ns
                < event.event_time_ns
                < anchors[1].event_time_ns
            ):
                raise ValueError(
                    "product synthetic event lacks its actual adjacent source anchors"
                )
            if (
                event.constraint_set_id
                != stage.configuration.carving_constraints.constraint_set_id
                or event.generator_config_id != config.config_id
                or event.generator_id != engine
                or event.feed_epoch_id
                != conditions[stream.symbol].feed_epoch_id
                or event.source_version_id not in plan.run.source_version_ids
            ):
                raise ValueError(
                    "product local constraint/generator lineage differs"
                )
            support_checks.append(
                _local_support_invariants(
                    event,
                    anchors,
                    conditions[stream.symbol],
                    context,
                    stage.configuration.carving_constraints,
                    evidence,
                    plan.run,
                    stage.configuration.cross_currency_config,
                )
            )
        rebuilt = SyntheticEventStreamV1.merge(
            run_id=plan.run.run_id,
            ensemble_member_id=task.window.ensemble_member_id,
            symbol=stream.symbol,
            observed_events=observed,
            synthetic_events=generated,
            source_version_ids=stream.source_version_ids,
        )
        _same(
            rebuilt.to_dict(),
            stream.to_dict(),
            "product native local merge differs",
        )
        local.append(
            {
                "symbol": stream.symbol,
                "stream_id": rebuilt.stream_id,
                "observed_ids": [event.event_id for event in observed],
                "synthetic_ids": [event.event_id for event in generated],
                "local_support_checks": support_checks,
            }
        )
    actual_constraint = persistence._constraint_manifest(
        event for stream in streams for event in stream.events
    )
    _same(
        actual_constraint.to_dict(),
        manifest.constraints.to_dict(),
        "product actual constraint assignments differ",
    )
    report = validate_cross_currency_output(
        run=plan.run,
        window=task.window,
        streams={stream.symbol: stream for stream in streams},
        config=stage.configuration.cross_currency_config,
        stage=CrossCurrencyValidationStage.POST_BROKER,
        observed_anchors=tuple(
            event for rows in core.values() for event in rows
        ),
        conditions=(condition,),
        join_policy=join,
        nearest_prior_max_age_ns=maximum_age,
    )
    if (
        not report.passed
        or report.validation_id != manifest.quality.final_validation_id
    ):
        raise ValueError(
            "product fresh final cross-currency validation differs"
        )
    _verify_delivery_output_hash(
        streams,
        manifest.quality.delivery_output_content_sha256,
    )
    quality = manifest.quality
    if (
        final_use.decision_id not in quality.point_in_time_evidence_decision_ids
        or cross_use.decision_id
        not in quality.cross_series_constraint_decision_ids
    ):
        raise ValueError(
            "product lacks its actual final source evidence decisions"
        )
    _same(
        tuple(sorted(item.projection_id for item in projections)),
        quality.point_in_time_evidence_projection_ids,
        "product source evidence projection set differs",
    )
    _verify_quality_evidence_hash(quality, report)
    if quality.cross_series_constraint_bundle_ids != (bundle.bundle_id,):
        raise ValueError("product actual cross-series source bundle differs")
    source_ids = {
        "configuration_id": plan.configuration_id,
        "source_inventory_id": stage.source_inventory.inventory_id,
        "source_files": sorted(
            {
                partition.artifact.sha256
                for partition in stage.source_inventory.partitions_for_window(
                    task.window
                )
            }
        ),
        "runtime_scope": runtime,
        "cross_condition_id": condition.condition_id,
        "cross_bundle_id": bundle.bundle_id,
    }
    return CampaignProductVerificationV1(
        plan_id=plan.plan_id,
        shard_id=row.shard_id,
        support_id=row.support_id,
        run_id=plan.run.run_id,
        window_id=task.window.window_id,
        ensemble_member_id=task.window.ensemble_member_id,
        product_ref=CampaignArtifactRefV1.from_artifact_ref(row.product_ref),
        manifest_id=manifest.manifest_id,
        configuration_id=plan.configuration_id,
        proposal_engine_id=engine,
        generator_config_id=config.config_id,
        runtime_scope_json=canonical(runtime),
        logical_content_sha256=manifest.replay.logical_content_sha256,
        observed_content_sha256=manifest.source.observed_content_sha256,
        observed_event_count=manifest.observed_event_count,
        synthetic_event_count=manifest.synthetic_event_count,
        local_validation_sha256=_digest(local),
        final_validation_json=canonical(report.to_dict()),
        verified_inputs_sha256=_digest(source_ids),
    )


def iter_campaign_manifest_paths(output_root: str | Path) -> Iterator[Path]:
    """Stream a bounded deterministic committed-product scan, without loading.

    No cap truncates output. A directory/tree/product bound fails explicitly.
    This is shared by the certification builder, never an authority claim.
    """
    from histdatacom.synthetic.persistence import (
        RECONSTRUCTION_PRODUCT_DIRECTORY,
    )

    _require_platform()
    root = _path(output_root) / RECONSTRUCTION_PRODUCT_DIRECTORY
    if not root.exists() and not root.is_symlink():
        return
    _regular_ancestors(root)
    pending = [(root, 0)]
    visited = products = 0
    while pending:
        directory, depth = pending.pop()
        if depth > 16 or not stat.S_ISDIR(directory.lstat().st_mode):
            raise ValueError("campaign discovery directory/depth differs")
        entries: list[os.DirEntry[str]] = []
        with os.scandir(directory) as iterator:
            for entry in iterator:
                if len(entries) >= 100000:
                    raise ValueError(
                        "campaign discovery directory entry bound exceeded"
                    )
                entries.append(entry)
        for entry in sorted(entries, key=lambda item: item.name, reverse=True):
            visited += 1
            if visited > MAX_DISCOVERED_PRODUCTS * 16:
                raise ValueError("campaign discovery tree bound exceeded")
            mode = entry.stat(follow_symlinks=False).st_mode
            path = Path(entry.path)
            if stat.S_ISLNK(mode):
                raise ValueError("campaign product tree contains a symlink")
            if stat.S_ISDIR(mode):
                pending.append((path, depth + 1))
            elif entry.name == "manifest.json":
                if not stat.S_ISREG(mode):
                    raise ValueError("campaign product manifest is not regular")
                if path.parent.parent.name != "commits":
                    raise ValueError(
                        "campaign manifest is outside committed layout"
                    )
                products += 1
                if products > MAX_DISCOVERED_PRODUCTS:
                    raise ValueError(
                        "campaign discovery product bound exceeded"
                    )
                yield path


def read_campaign_control_snapshot(
    path: str | Path,
) -> tuple[dict[str, Any], bytes]:
    """Return decoded JSON and its exact bytes from one guarded snapshot.

    This is only byte/JSON admission, not native semantic or deep verification.
    The final guard repeats the file identity/hash check without substituting
    a second, independently opened payload for the bytes that were decoded.
    """
    guard = _Guard()
    encoded = guard.read(path, control=True)
    result = _json(encoded)
    guard.finish()
    return result, encoded


def read_campaign_control_json(path: str | Path) -> dict[str, Any]:
    """Read bounded duplicate-free JSON, without verification authority."""
    return read_campaign_control_snapshot(path)[0]


def verify_campaign_product_index(
    index_path: str | Path,
) -> CampaignDeepVerificationV1:
    """Re-read every required input and freshly verify actual native products.

    Missing products produce an incomplete receipt. Invalid/resealed lineage,
    changed bytes or unsupported native validation raise; no partial proof is
    returned. Scientific model promotion and historical campaign execution are
    separate claims, not inferred from this integrity/local-validation receipt.
    """
    from histdatacom.synthetic.persistence import (
        RECONSTRUCTION_MANIFEST_ARTIFACT_KIND,
        ReconstructionProductManifestV3,
    )
    from histdatacom.synthetic.reconstruction_plan import (
        ReconstructionPlanExecutionManifestV1,
    )

    guard = _Guard()
    index, fields = _structural(index_path, guard)
    plan_set, plans = _plans(index, guard)
    terminal_intervals = _terminal_support_intervals(plan_set, plans)
    executions: dict[str, Any] = {}
    expected_coordinates: dict[tuple[str, str, str], str] = {}
    for shard_id, (_, plan) in plans.items():
        ref = plan.artifact_graph["execution_manifest"]
        execution = guard.native(
            ref.path, ReconstructionPlanExecutionManifestV1, ref
        )
        if (
            execution.configuration_id != plan.configuration_id
            or execution.manifest_id != plan.execution_manifest_id
        ):
            raise ValueError("campaign execution/configuration binding differs")
        executions[shard_id] = execution
        for request in plan.workflow_requests:
            for task in request.tasks:
                key = (
                    plan.run.run_id,
                    task.window.window_id,
                    task.window.ensemble_member_id,
                )
                if key in expected_coordinates:
                    raise ValueError(
                        "campaign plan duplicates a product coordinate"
                    )
                if len(expected_coordinates) >= MAX_CAMPAIGN_COORDINATES:
                    raise ValueError(
                        "campaign coordinate inventory bound exceeded"
                    )
                expected_coordinates[key] = shard_id
    roots = tuple(sorted({item.output_root for item in executions.values()}))
    products: dict[tuple[str, str, str], tuple[Path, Any]] = {}
    out_of_plan: list[CampaignArtifactRefV1] = []
    discovery = hashlib.sha256()
    for path, manifest in _discover(roots, guard):
        _refuse_terminal_product(path, manifest, terminal_intervals)
        key = (manifest.run_id, manifest.window_id, manifest.ensemble_member_id)
        discovery.update(
            str(path).encode("utf-8")
            + b"\0"
            + manifest.manifest_id.encode("ascii")
            + b"\n"
        )
        if key not in expected_coordinates:
            if len(out_of_plan) >= MAX_OUT_OF_PLAN_PRODUCTS:
                raise ValueError("campaign out-of-plan report bound exceeded")
            out_of_plan.append(
                guard.ref(path, RECONSTRUCTION_MANIFEST_ARTIFACT_KIND)
            )
            continue
        if type(manifest) is not ReconstructionProductManifestV3:
            raise ValueError("campaign coordinate requires native V3 product")
        if key in products:
            if products[key][1].manifest_id != manifest.manifest_id:
                raise ValueError(
                    "campaign has conflicting products for one coordinate"
                )
            raise ValueError(
                "campaign repeats a committed coordinate at different paths"
            )
        products[key] = (path, manifest)
    product_digest = hashlib.sha256(b"histdatacom.campaign-products.v1\n")
    seen_shards: set[str] = set()
    seen_coordinates: set[tuple[str, str, str]] = set()
    for shard in _shards(index, guard):
        if shard.shard_id not in plans:
            raise ValueError("campaign index contains an out-of-plan shard")
        seen_shards.add(shard.shard_id)
        descriptor, plan = plans[shard.shard_id]
        _verify_shard(
            shard,
            descriptor,
            plan,
            index,
            products,
            guard,
            product_digest,
            seen_coordinates,
            plan_set,
        )
    if seen_shards != set(plans) or seen_coordinates != set(
        expected_coordinates
    ):
        raise ValueError("campaign index omits part of its planned rectangle")
    # Repeat discovery membership after all native reads; each existing file is
    # separately rehashed below. This detects newly created committed products.
    from histdatacom.synthetic.persistence import load_reconstruction_manifest

    after_discovery = hashlib.sha256()
    after_seen: set[Path] = set()
    for root in roots:
        for path in iter_campaign_manifest_paths(root):
            if path in after_seen:
                continue
            after_seen.add(path)
            manifest = load_reconstruction_manifest(path)
            after_discovery.update(
                str(path).encode("utf-8")
                + b"\0"
                + manifest.manifest_id.encode("ascii")
                + b"\n"
            )
    if after_discovery.digest() != discovery.digest():
        raise ValueError("campaign committed-product inventory changed")
    del plan_set
    return CampaignDeepVerificationV1(
        **fields,
        product_verifications_sha256=product_digest.hexdigest(),
        out_of_plan_products=tuple(
            sorted(out_of_plan, key=lambda ref: (ref.path, ref.sha256))
        ),
        verified_inputs_sha256=guard.finish(),
    )


__all__ = [
    "inspect_campaign_product_index",
    "iter_campaign_manifest_paths",
    "read_campaign_control_json",
    "read_campaign_control_snapshot",
    "verify_campaign_product_index",
]
