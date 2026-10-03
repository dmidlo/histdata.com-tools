"""Verified wide composition without changing native row or join contracts."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Any, cast

from .training_contracts import (
    TrainingRowV1,
    training_json,
    training_load,
)
from .training_join_contracts import (
    TrainingJoinBatchV1,
    TrainingJoinPlanV1,
    TrainingJoinValueV1,
)
from .training_join_views import materialize_training_joins
from .training_wide_contracts import (
    MAX_WIDE_CHILD_BYTES,
    MAX_WIDE_TILES,
    TrainingWideChildV1,
    TrainingWideColumnV1,
    TrainingWideControlV1,
    TrainingWideGrainV1,
    TrainingWideGroupV1,
    TrainingWideManifestV1,
    TrainingWidePlanV1,
    TrainingWideRowV1,
    TrainingWideTileV1,
    WideRowGrain,
    WideValueType,
    _wide_geometry,
)
from .training_wide_schema import _describe_training_wide_column


@dataclass(frozen=True, slots=True)
class TrainingWideViewV1:
    """Process-local result. Public consumers always replay it, not trust it."""

    manifest: TrainingWideManifestV1
    controls: tuple[TrainingJoinBatchV1, ...]
    groups: tuple[TrainingJoinBatchV1, ...]


def _current(
    batches: tuple[TrainingJoinBatchV1, ...], *, retention: bool = False
) -> None:
    from histdatacom.broker_plugin_policy import (
        BrokerPolicyOperation,
        require_provider_operation,
    )

    from .training_provider_policy import (
        require_training_retention,
        training_provider_subject,
    )

    for batch in batches:
        subject = training_provider_subject(batch)
        if subject is not None:
            require_provider_operation(
                subject, BrokerPolicyOperation.MATERIAL_USE
            )
            require_provider_operation(subject, BrokerPolicyOperation.DERIVE)
        if retention:
            require_training_retention(subject)


def _child(batch: TrainingJoinBatchV1) -> TrainingWideChildV1:
    data = batch.to_json().encode()
    return TrainingWideChildV1(
        batch.plan.artifact_id,
        batch.artifact_id,
        hashlib.sha256(data).hexdigest(),
        len(data),
    )


def _column_bindings(
    batch: TrainingJoinBatchV1,
) -> tuple[TrainingWideColumnV1, ...]:
    sources = {s.key: s for s in batch.plan.sources}
    definitions = []
    empty_roots: dict[str, tuple[str, ...]] = {}
    if not batch.rows:
        from .training_join_sources import prepare_training_join_sources

        empty_roots = {
            key: adapter.root_ids
            for key, adapter in prepare_training_join_sources(
                batch.plan
            ).items()
        }
    for index, column in enumerate(batch.plan.columns):
        definition, basis = _describe_training_wide_column(
            sources[column.source_key],
            column,
            information_mode=batch.plan.information_mode,
        )
        identities = {
            identity
            for row in batch.rows
            for identity in (
                *row.values[index].source_ids,
                *row.values[index].parent_source_ids,
            )
        }
        identities.update(
            identity
            for row in batch.rows
            for refusal in row.values[index].refusal_evidence
            for identity in (*refusal.source_ids, *refusal.parent_source_ids)
        )
        identities.update(empty_roots.get(column.source_key, ()))
        if not set(basis) <= identities:
            raise ValueError(
                "wide unit metadata differs from exact replayed source identities"
            )
        for row in batch.rows:
            value = training_load(row.values[index].value_json)["value"]
            if value is None:
                continue
            admitted = {
                WideValueType.INTEGER: (int,),
                WideValueType.NUMBER: (int, float),
                WideValueType.BOOLEAN: (bool,),
                WideValueType.STRING: (str,),
                WideValueType.UNAVAILABLE: (),
            }[definition.dtype]
            if type(value) not in admitted:
                raise ValueError(
                    "wide native value differs from its exact logical dtype"
                )
        definitions.append(definition)
    return tuple(definitions)


def build_training_wide_plan(
    plans: tuple[TrainingJoinPlanV1, ...], *, grain: TrainingWideGrainV1
) -> TrainingWidePlanV1:
    """Build descriptors from genuinely executed bounded native plans.

    Callers partition columns, not native batch.rows. Every column tile keeps
    the same complete source inventory and full replayable spine at its cutoff.
    """
    if type(plans) is not tuple or not 1 <= len(plans) <= MAX_WIDE_TILES:
        raise ValueError("wide plan builder requires bounded native plan tuple")
    if type(grain) is not TrainingWideGrainV1 or any(
        type(p) is not TrainingJoinPlanV1 for p in plans
    ):
        raise TypeError("wide plan builder requires exact native types")
    ordered = tuple(
        sorted(
            plans,
            key=lambda p: (
                p.spine.artifact_id,
                tuple(c.name for c in p.columns),
            ),
        )
    )
    _wide_geometry(grain, ordered)
    tiles = tuple(TrainingWideTileV1(p.to_json()) for p in ordered)
    restored = tuple(tile.join_plan for tile in tiles)
    if (
        restored != ordered
        or TrainingWideGrainV1.from_json(grain.to_json()) != grain
    ):
        raise ValueError(
            "wide builder inputs differ from their canonical fields"
        )
    definitions: dict[str, TrainingWideColumnV1] = {}
    for plan in restored:
        batch = materialize_training_joins(plan)
        for definition in _column_bindings(batch):
            name = definition.column.name
            if name in definitions and definitions[name] != definition:
                raise ValueError(
                    "wide column semantics vary across row coordinates"
                )
            definitions[name] = definition
        _current((batch,))
    return TrainingWidePlanV1(
        grain, tuple(definitions[k] for k in sorted(definitions)), tiles
    )


def _rows(plan: TrainingWidePlanV1) -> tuple[TrainingWideRowV1, ...]:
    spines = {
        tile.join_plan.spine.artifact_id: tile.join_plan.spine
        for tile in plan.tiles
    }
    ordered = sorted(
        spines.values(),
        key=lambda s: (
            (
                s.request.decision_time_ns
                if s.request.decision_time_ns is not None
                else s.request.start_ns
            ),
            s.request.product_manifest_id or "",
            s.artifact_id,
        ),
    )
    rows: list[TrainingWideRowV1] = []
    seen = set()
    if len({s.request.symbols for s in ordered}) != 1:
        raise ValueError(
            "wide row coordinates require one declared symbol inventory"
        )
    for spine in ordered:
        if spine.request.feature_artifact_id is not None or any(
            row.symbol is None for row in spine.rows
        ):
            raise ValueError(
                "wide market row grain cannot relabel context cells as events"
            )
        if plan.grain.kind in (WideRowGrain.EVENT, WideRowGrain.PANEL):
            for row in spine.rows:
                key = (
                    row.source_row_key,
                    row.ensemble_member_id,
                    row.scenario_ids,
                )
                if key in seen:
                    raise ValueError(
                        "wide row grain duplicates a native source coordinate"
                    )
                seen.add(key)
                assert row.symbol is not None
                rows.append(
                    TrainingWideRowV1(
                        spine.artifact_id,
                        row.symbol,
                        row.artifact_id,
                        None,
                        "retained_native_row",
                    )
                )
            continue
        cutoff = spine.request.decision_time_ns
        age = plan.grain.max_anchor_age_ns
        assert cutoff is not None and age is not None
        start = max(spine.ownership.units[0].start_ns, cutoff - age)
        end = min(spine.ownership.units[-1].end_ns, cutoff + 1)
        if (spine.request.start_ns, spine.request.end_ns) != (start, end):
            raise ValueError(
                "grid anchor request must cover its complete bounded lookup interval"
            )
        for symbol in spine.request.symbols:
            candidates = tuple(
                row
                for row in spine.rows
                if row.symbol == symbol and start <= row.event_time_ns <= cutoff
            )
            # Complete native replay fixes physical/event sequence order. No
            # hash ordering or invented event at a vacant grid coordinate.
            latest = max((r.event_time_ns for r in candidates), default=None)
            selected = tuple(r for r in candidates if r.event_time_ns == latest)
            anchor = selected[-1] if selected else None
            rows.append(
                TrainingWideRowV1(
                    spine.artifact_id,
                    symbol,
                    anchor.artifact_id if anchor is not None else None,
                    cutoff,
                    (
                        "retained_native_row"
                        if anchor is not None
                        else "missing_spine_anchor"
                    ),
                )
            )
    return tuple(rows)


def _indexes(
    view: TrainingWideViewV1,
) -> tuple[
    dict[tuple[str, str], TrainingRowV1],
    dict[tuple[str, str, str], TrainingJoinValueV1],
]:
    native_rows = {
        (b.plan.spine.artifact_id, row.artifact_id): row
        for b in view.controls
        for row in b.plan.spine.rows
    }
    values = {
        (batch.plan.spine.artifact_id, row.spine_row_id, value.column): value
        for batch in view.groups
        for row in batch.rows
        for value in row.values
    }
    return native_rows, values


def _content(
    plan: TrainingWidePlanV1,
    rows: tuple[TrainingWideRowV1, ...],
    controls: tuple[TrainingJoinBatchV1, ...],
    groups: tuple[TrainingJoinBatchV1, ...],
) -> str:
    native = {
        (b.plan.spine.artifact_id, r.artifact_id): r
        for b in controls
        for r in b.plan.spine.rows
    }
    values = {
        (b.plan.spine.artifact_id, r.spine_row_id, v.column): v
        for b in groups
        for r in b.rows
        for v in r.values
    }
    digest = hashlib.sha256(b"histdatacom.training-wide-content.v1\x00")
    header = training_json(
        {
            "schema_sha256": plan.schema_sha256,
            "grain": plan.grain.to_dict(),
            "controls": [b.artifact_id for b in controls],
            "row_count": len(rows),
        }
    ).encode()
    digest.update(len(header).to_bytes(8, "big") + header)
    for row in rows:
        key = (row.spine_id, row.native_row_id or "")
        data = training_json(
            {
                "row": row.to_dict(),
                "native": (
                    native[key].to_dict()
                    if row.native_row_id is not None
                    else None
                ),
                "cells": [
                    (
                        values[(*key, c.column.name)].to_dict()
                        if row.native_row_id is not None
                        else None
                    )
                    for c in plan.columns
                ],
            }
        ).encode()
        digest.update(len(data).to_bytes(8, "big") + data)
    return digest.hexdigest()


def materialize_training_wide_view(
    plan: TrainingWidePlanV1,
) -> TrainingWideViewV1:
    if type(plan) is not TrainingWidePlanV1:
        raise TypeError("wide materialization requires its exact bounded plan")
    # Reconstruct to reject any illicit object-level mutation of cached wires.
    if TrainingWidePlanV1.from_json(plan.to_json()) != plan:
        raise ValueError("wide plan differs from its canonical fields")
    plans = tuple(tile.join_plan for tile in plan.tiles)
    by_spine = {p.spine.artifact_id: p for p in plans}
    controls = tuple(
        materialize_training_joins(replace(by_spine[key], columns=()))
        for key in sorted(by_spine)
    )
    groups: list[TrainingJoinBatchV1] = []
    total_bytes = sum(len(b.to_json().encode()) for b in controls)
    expected = {c.column.name: c for c in plan.columns}
    for native_plan in plans:
        batch = materialize_training_joins(native_plan)
        total_bytes += len(batch.to_json().encode())
        if total_bytes > MAX_WIDE_CHILD_BYTES:
            raise ValueError("wide native child bytes exceed aggregate budget")
        for definition in _column_bindings(batch):
            if expected[definition.column.name] != definition:
                raise ValueError(
                    "wide registry differs from source-derived semantics"
                )
        groups.append(batch)
    group_tuple = tuple(groups)
    rows = _rows(plan)
    manifest = TrainingWideManifestV1(
        plan.grain,
        plan.columns,
        plan.artifact_id,
        rows,
        tuple(
            TrainingWideControlV1(b.plan.spine.artifact_id, _child(b))
            for b in controls
        ),
        tuple(
            TrainingWideGroupV1(
                b.plan.spine.artifact_id,
                tuple(c.name for c in b.plan.columns),
                _child(b),
            )
            for b in group_tuple
        ),
        plan.schema_sha256,
        _content(plan, rows, controls, group_tuple),
    )
    _current(controls + group_tuple)
    return TrainingWideViewV1(manifest, controls, group_tuple)


def _restore_plan(
    manifest: TrainingWideManifestV1, controls: tuple[TrainingJoinBatchV1, ...]
) -> TrainingWidePlanV1:
    if len(controls) != len(manifest.controls):
        raise ValueError("wide control inventory differs")
    by_spine = {}
    for expected, batch in zip(manifest.controls, controls):
        if (
            type(batch) is not TrainingJoinBatchV1
            or batch.plan.columns
            or _child(batch) != expected.child
            or batch.plan.spine.artifact_id != expected.spine_id
        ):
            raise ValueError(
                "wide control is not its exact zero-column native batch"
            )
        by_spine[expected.spine_id] = batch
    columns = {c.column.name: c.column for c in manifest.columns}
    tiles = []
    for group in manifest.groups:
        if not set(group.columns) <= set(columns):
            raise ValueError("wide group references an unknown column")
        plan = replace(
            by_spine[group.spine_id].plan,
            columns=tuple(columns[n] for n in group.columns),
        )
        if plan.artifact_id != group.child.tile_id:
            raise ValueError("wide native plan does not match layout identity")
        tiles.append(TrainingWideTileV1(plan.to_json()))
    wide_plan = TrainingWidePlanV1(
        manifest.grain, manifest.columns, tuple(tiles)
    )
    if wide_plan.artifact_id != manifest.plan_id:
        raise ValueError("wide reconstructed plan differs from original")
    return wide_plan


def replay_training_wide_view(view: TrainingWideViewV1) -> TrainingWideViewV1:
    if (
        type(view) is not TrainingWideViewV1
        or type(view.manifest) is not TrainingWideManifestV1
    ):
        raise TypeError("wide consumer requires an exact complete view")
    expected = materialize_training_wide_view(
        _restore_plan(view.manifest, view.controls)
    )
    if expected != view:
        raise ValueError(
            "wide view differs from full native source/value replay"
        )
    return expected


def _projection_names(
    manifest: TrainingWideManifestV1, columns: tuple[str, ...] | None
) -> tuple[str, ...]:
    names = tuple(c.column.name for c in manifest.columns)
    selected = names if columns is None else columns
    if (
        type(selected) is not tuple
        or any(type(c) is not str for c in selected)
        or len(set(selected)) != len(selected)
        or not set(selected) <= set(names)
    ):
        raise ValueError("invalid wide column projection")
    # Logical schema order, never caller-set ordering, determines hashes/frames.
    return tuple(name for name in names if name in selected)


def _project_verified(
    view: TrainingWideViewV1, columns: tuple[str, ...]
) -> tuple[dict[str, object], ...]:
    native, values = _indexes(view)
    result: list[dict[str, object]] = []
    for ref in view.manifest.rows:
        key = (ref.spine_id, ref.native_row_id or "")
        row = native.get(key)
        cells = (
            {name: values[(*key, name)] for name in columns}
            if row is not None
            else {}
        )
        result.append(
            {
                "schema_version": "histdatacom.training-wide-record.v1",
                "wide_row_id": ref.artifact_id,
                "row_grain": view.manifest.grain.kind.value,
                "grid_cutoff_ns": ref.grid_cutoff_ns,
                "row_state": ref.state,
                "spine": row.to_dict() if row is not None else None,
                "spine_id": ref.spine_id,
                "symbol": ref.symbol,
                "values": {
                    name: (
                        training_load(cells[name].value_json)["value"]
                        if name in cells
                        else None
                    )
                    for name in columns
                },
                "states": {
                    name: (
                        cells[name].state.value
                        if name in cells
                        else "missing_spine_anchor"
                    )
                    for name in columns
                },
                "cell_provenance": {
                    name: cells[name].to_dict() if name in cells else None
                    for name in columns
                },
                "schema_sha256": view.manifest.schema_sha256,
                "content_sha256": view.manifest.content_sha256,
                "nonclaims": list(view.manifest.nonclaims),
            }
        )
    _current(view.controls + view.groups)
    return tuple(result)


def project_training_wide_view(
    view: TrainingWideViewV1, *, columns: tuple[str, ...] | None = None
) -> tuple[dict[str, object], ...]:
    expected = replay_training_wide_view(view)
    return _project_verified(
        expected, _projection_names(expected.manifest, columns)
    )


def diagnose_training_wide_view(
    view: TrainingWideViewV1,
) -> tuple[dict[str, object], ...]:
    """Freshly replayed, explicitly within-block descriptive diagnostics."""
    from .training_wide_diagnostics import diagnose_wide_feature_values

    expected = replay_training_wide_view(view)
    names = _projection_names(expected.manifest, None)
    records = _project_verified(expected, names)
    reports = []
    # The diagnostics contract admits at most4096 values; split rows as well
    # as columns, and report exact block membership, never a global rank.
    for column_start in range(0, len(names), 32):
        block = names[column_start : column_start + 32]
        stride = min(256, 4096 // len(block))
        for row_start in range(0, max(1, len(records)), stride):
            selected = records[row_start : row_start + stride]
            matrix: Any = tuple(
                tuple(
                    cast(dict[str, Any], record["values"])[name]
                    for name in block
                )
                for record in selected
            )
            _current(expected.controls + expected.groups)
            report = diagnose_wide_feature_values(block, matrix)
            reports.append(
                {
                    "schema_sha256": expected.manifest.schema_sha256,
                    "content_sha256": expected.manifest.content_sha256,
                    "row_start": row_start,
                    "row_end": row_start + len(selected),
                    "report": report.to_dict(),
                    "scope": "this_declared_block_only_not_global_rank",
                }
            )
    _current(expected.controls + expected.groups)
    return tuple(reports)
