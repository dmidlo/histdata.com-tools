"""Bounded exploratory generator diagnostics on already-verified inputs.

This pure layer does not open files, grant approvals, or verify external source
bytes. The supervising workflow must verify the bridge/model/index in the same
closed invocation and enforce the hard day deadline. A new executable identity
can change native seeds: this is not a replay claim about attempt 001.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar, cast

from histdatacom.synthetic.contracts import SyntheticEventStreamV1
from histdatacom.synthetic.contracts import SyntheticEventV1
from histdatacom.synthetic.generation import (
    EmpiricalMotifGeneratorConfigV1,
    EmpiricalMotifEventLineageV1,
    EmpiricalMotifTransformationV1,
    MotifGenerationDecision,
    MotifGenerationStatus,
    _MotifPlanningObservation,
    _observe_motif_planning,
    _target_cardinality,
    generate_empirical_motif_candidates,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.motifs import (
    ReferenceMotifIndexV1,
    ReferenceMotifQueryResultV1,
    ReferenceMotifQueryV1,
    query_reference_motifs,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionResourceEstimateV1,
    ReconstructionWindowV1,
)

from .training_contracts import (
    TrainingContract,
    _string_cost,
    training_json,
    training_load,
)
from .training_weight_calibration import WEIGHT_MEMBERS, WEIGHT_SYMBOLS
from .training_weight_candidates import (
    TrainingWeightModelV1,
    TrainingWeightOperationalFailure,
    _compact_result,
    _decode_native_rows,
    _decode_stream,
    _encode_native_rows,
    _encode_stream,
    _mapping_condition,
    _native_json,
)
from .training_weight_contracts import (
    _object,
    read_training_weight_preregistration,
)
from .training_weight_lineage import (
    CORE_END_NS,
    CORE_START_NS,
    MAX_AGE_NS,
    TrainingWeightDayBridgeV1,
    WeightEvidenceKind,
    _day_ns,
)
from .training_weight_diagnostic_protocol import (
    TrainingWeightDiagnosticEvidenceKind,
    read_training_weight_diagnostic_protocol,
)

MAX_BYTES = 8 * 1024 * 1024
MAX_NODES = 131072
MAX_EVENTS = 4096
_CELL_STATUSES = {
    "success",
    "deterministic_refused",
    "diagnostic_failed",
    "not_attempted",
}
_DIAGNOSTIC_REASONS = {
    "diagnostic_component_bound",
    "diagnostic_day_byte_bound",
    "diagnostic_day_expanded_node_bound",
}
_CELL_REASONS = (
    _DIAGNOSTIC_REASONS
    | {
        "declared_triangle_event_budget",
        "native_event_bound",
        "transformation_bound",
    }
    | {"engine:" + decision.value for decision in MotifGenerationDecision}
)
_COMMON_RECORD_KEYS = {
    "interval_ordinal",
    "left_anchor_event_id",
    "right_anchor_event_id",
    "left_time_ns",
    "right_time_ns",
    "conditioning_start_ns",
    "conditioning_end_ns",
    "information_mode",
    "decision",
}
_PLANNING_KEYS = {
    "target_times_ns",
    "cadence_ns",
    "left_time_ns",
    "timestamp_precision_ns",
    "output_cursor",
    "segment_ordinal",
    "segment_seed",
    "refusal",
    "prefix",
    "retrieved_scale_summaries",
}
_SUMMARY_KEYS = {
    "fragment_id",
    "duration_ns",
    "source_gap_count",
    "allowed_min",
    "allowed_max",
    "tested_count",
    "normal_min",
    "normal_max",
    "terminal_branch_count",
    "terminal_min",
    "terminal_max",
    "below_count",
    "inside_count",
    "above_count",
}
_BATCH_KEYS = {
    "schema_version",
    "run_id",
    "window_id",
    "ensemble_member_id",
    "symbol",
    "anchor_interval_id",
    "left_anchor_event_id",
    "right_anchor_event_id",
    "generator_config_id",
    "query_result_id",
    "status",
    "decision",
    "target_event_count",
    "owned_event_count",
    "transformation_count",
    "event_lineage_count",
    "event_content_sha256",
    "transformation_content_sha256",
    "event_lineage_content_sha256",
    "resource_estimate_id",
    "carry_id",
    "decision_details",
    "candidate_only",
    "hard_carving_status",
    "broker_conditioning_status",
    "final_storage_status",
    "batch_id",
}


def _transforms(table: dict[str, object]) -> None:
    for raw in _decode_native_rows(table, "transformation"):
        restored = EmpiricalMotifTransformationV1.from_dict(raw)
        if _native_json(restored.to_dict()) != _native_json(raw):
            raise ValueError("unknown/coercive diagnostic transformation")


def _record(raw: dict[str, object], ordinal: int) -> None:
    extras = (
        set()
        if raw.get("decision") == "no_strict_interior_timestamp"
        else {
            "query_result_id",
            "native_batch",
            "resource_estimate",
            "planning",
            "event_lineage",
        }
    )
    if (
        set(raw) != _COMMON_RECORD_KEYS | extras
        or raw.get("interval_ordinal") != ordinal
    ):
        raise ValueError("diagnostic interval schema/order differs")
    if raw["information_mode"] != "ex_post_whole_retained_window":
        raise ValueError("diagnostic interval information mode differs")
    clocks = [
        raw[key]
        for key in (
            "left_time_ns",
            "right_time_ns",
            "conditioning_start_ns",
            "conditioning_end_ns",
        )
    ]
    if any(type(v) is not int or v < 0 for v in clocks):
        raise ValueError("diagnostic interval clocks require exact integers")
    if not extras:
        if raw["left_time_ns"] != raw["right_time_ns"]:
            raise ValueError("zero interior geometry differs")
        return
    if raw["decision"] not in {d.value for d in MotifGenerationDecision}:
        raise ValueError("unknown diagnostic engine decision")
    batch = _object(raw["native_batch"])
    if set(batch) != _BATCH_KEYS:
        raise ValueError("unknown/missing diagnostic native batch field")
    expected_batch = (
        "empirical-motif-candidate-batch:sha256:"
        + hashlib.sha256(
            _native_json(
                {k: v for k, v in batch.items() if k != "batch_id"}
            ).encode("utf-8")
        ).hexdigest()
    )
    if (
        batch["batch_id"] != expected_batch
        or batch["decision"] != raw["decision"]
        or batch["query_result_id"] != raw["query_result_id"]
    ):
        raise ValueError("diagnostic batch identity/ownership differs")
    lineages = raw["event_lineage"]
    if (
        type(lineages) is not list
        or len(lineages) != batch["event_lineage_count"]
    ):
        raise ValueError("diagnostic native lineage count differs")
    for value in lineages:
        lineage = _object(value)
        if training_json(
            EmpiricalMotifEventLineageV1.from_dict(lineage).to_dict()
        ) != training_json(lineage):
            raise ValueError("unknown/coercive diagnostic event lineage")
    resource = _object(raw["resource_estimate"])
    restored_resource = ReconstructionResourceEstimateV1.from_dict(resource)
    if training_json(restored_resource.to_dict()) != training_json(resource):
        raise ValueError("unknown/coercive diagnostic resource estimate")
    planning = raw["planning"]
    if planning is None:
        return
    plan = _object(planning)
    if set(plan) != _PLANNING_KEYS:
        raise ValueError("unknown/missing diagnostic planner field")
    times = plan["target_times_ns"]
    cursor = plan["output_cursor"]
    if (
        type(times) is not list
        or len(times) > MAX_EVENTS
        or any(type(t) is not int for t in times)
    ):
        raise ValueError("diagnostic target-time bound/type differs")
    if type(cursor) is not int or not 0 <= cursor <= len(times):
        raise ValueError("diagnostic planner cursor differs")
    if times != sorted(times) or any(
        not cast(int, raw["left_time_ns"]) < t < cast(int, raw["right_time_ns"])
        for t in times
    ):
        raise ValueError("diagnostic target-time ownership differs")
    for key in ("cadence_ns", "timestamp_precision_ns"):
        if type(plan[key]) is not int or cast(int, plan[key]) <= 0:
            raise ValueError("diagnostic cadence/precision must be positive")
    seed = plan["segment_seed"]
    if seed is not None and (
        type(seed) is not str
        or not seed.isascii()
        or not seed.isdecimal()
        or str(int(seed)) != seed
        or not 0 <= int(seed) < 2**64
    ):
        raise ValueError("diagnostic seed must be canonical uint64 decimal")
    _transforms(_object(plan["prefix"]))
    rows = plan["retrieved_scale_summaries"]
    if type(rows) is not list or len(rows) > 64:
        raise ValueError("diagnostic retrieved scale summaries exceed64")
    for value in rows:
        row = _object(value)
        if set(row) != _SUMMARY_KEYS:
            raise ValueError("unknown/missing scale diagnostic field")
        keys = (
            "tested_count",
            "below_count",
            "inside_count",
            "above_count",
            "terminal_branch_count",
        )
        if any(type(row[k]) is not int or cast(int, row[k]) < 0 for k in keys):
            raise ValueError("diagnostic tested counts require integers")
        if row["tested_count"] != sum(
            cast(int, row[k])
            for k in ("below_count", "inside_count", "above_count")
        ):
            raise ValueError("diagnostic tested counts do not reconcile")


def _nodes(value: object) -> int:
    if isinstance(value, dict):
        return 1 + sum(_nodes(k) + _nodes(v) for k, v in value.items())
    if isinstance(value, list):
        return 1 + sum(_nodes(v) for v in value)
    return 1


def _canonical(text: str) -> dict[str, object]:
    value = training_load(text)
    if training_json(value) != text:
        raise ValueError("diagnostic embedded JSON must be canonical")
    return value


def _embedded_nodes(value: object) -> int:
    """Charge transparent nested JSON, not merely its string wrapper."""
    count = _nodes(value)
    if isinstance(value, dict):
        for key, child in value.items():
            if key.endswith("_json") and type(child) is str:
                count += _nodes(_canonical(child))
            elif key in {"interval_records", "query_results", "query_supports"}:
                if type(child) is not list:
                    raise ValueError("diagnostic JSON catalog must be an array")
                count += sum(_nodes(_canonical(t)) for t in child)
            elif isinstance(child, (dict, list)):
                count += _embedded_nodes(child) - _nodes(child)
    elif isinstance(value, list):
        count += sum(_embedded_nodes(v) - _nodes(v) for v in value)
    return count


@dataclass(frozen=True, slots=True)
class TrainingWeightGeneratorCellDiagnosticV1(TrainingContract):
    """One complete cell ledger, possibly a refused/incomplete diagnostic."""

    KIND: ClassVar[str] = "weight-generator-cell-diagnostic"
    member_id: str
    symbol: str
    status: str
    reason: str | None
    interval_count: int
    attempted_intervals: int
    not_attempted_intervals: int
    counts_complete: bool
    run_json: str
    window_json: str
    stream_encoding_json: str | None
    transformations_encoding_json: str | None
    interval_records: tuple[str, ...]

    def _validate(self) -> None:
        if (
            self.member_id not in WEIGHT_MEMBERS
            or self.symbol not in WEIGHT_SYMBOLS
        ):
            raise ValueError("diagnostic cell has unknown member/symbol")
        if self.status not in _CELL_STATUSES:
            raise ValueError("unknown diagnostic cell status")
        if (self.status == "success") != (self.reason is None):
            raise ValueError("cell reason does not match status")
        if self.reason is not None and self.reason not in _CELL_REASONS:
            raise ValueError("unknown diagnostic cell reason")
        if not (
            0 <= self.attempted_intervals <= self.interval_count < MAX_EVENTS
            and self.not_attempted_intervals
            == self.interval_count - self.attempted_intervals
        ):
            raise ValueError("diagnostic interval denominator differs")
        if self.status == "success" and self.not_attempted_intervals:
            raise ValueError("successful cell omitted intervals")
        if self.status == "not_attempted" and self.attempted_intervals:
            raise ValueError("unattempted cell has attempted intervals")
        if self.counts_complete != (
            self.status in {"success", "deterministic_refused"}
        ):
            raise ValueError(
                "incomplete diagnostic counts must be lower bounds"
            )
        run = ReconstructionRunV1.from_dict(_canonical(self.run_json))
        window = ReconstructionWindowV1.from_dict(_canonical(self.window_json))
        if (
            training_json(run.to_dict()) != self.run_json
            or training_json(window.to_dict()) != self.window_json
        ):
            raise ValueError("unknown/coercive diagnostic native scope")
        if (
            window.run_id != run.run_id
            or window.ensemble_member_id != self.member_id
        ):
            raise ValueError("diagnostic native scope differs")
        if len(self.interval_records) > self.attempted_intervals:
            raise ValueError("diagnostic records exceed attempted intervals")
        if (
            self.counts_complete
            and len(self.interval_records) != self.attempted_intervals
        ):
            raise ValueError("complete diagnostic omitted interval evidence")
        for ordinal, text in enumerate(self.interval_records, 1):
            _record(_canonical(text), ordinal)
        if self.stream_encoding_json is not None:
            raw = _decode_stream(self.stream_encoding_json)
            training_json(raw)  # Bound repeated values before native hashing.
            stream = SyntheticEventStreamV1.from_dict(raw)
            if (
                stream.run_id != run.run_id
                or stream.ensemble_member_id != self.member_id
                or stream.symbol.upper() != self.symbol
                or training_json(stream.to_dict()) != training_json(raw)
            ):
                raise ValueError("diagnostic native stream differs")
        if self.transformations_encoding_json is not None:
            _transforms(_canonical(self.transformations_encoding_json))
        if self.status == "success" and self.stream_encoding_json is None:
            raise ValueError("successful diagnostic requires native lineage")


@dataclass(frozen=True, slots=True)
class TrainingWeightGeneratorDayDiagnosticV1(TrainingContract):
    """Canonical self-consistency is not source verification or approval."""

    KIND: ClassVar[str] = "weight-generator-day-diagnostic"
    protocol_id: str
    utc_date: str
    bridge_id: str
    model_id: str
    index_id: str
    adapter_source_fingerprint: str
    stochastic_model_id: str
    evidence_kind: TrainingWeightDiagnosticEvidenceKind
    status: str
    reason: str | None
    cells: tuple[TrainingWeightGeneratorCellDiagnosticV1, ...]
    query_results: tuple[str, ...]
    query_supports: tuple[str, ...]
    config_json: str
    resource_counts_json: str

    def _validate(self) -> None:
        _day_ns(self.utc_date)
        if (
            self.protocol_id
            != read_training_weight_diagnostic_protocol().artifact_id
        ):
            raise ValueError("diagnostic protocol identity differs")
        if self.status not in {"complete", "diagnostic_failed"}:
            raise ValueError("unknown diagnostic day status")
        if (self.status == "complete") != (self.reason is None):
            raise ValueError("diagnostic day reason differs")
        if self.reason is not None and self.reason not in _DIAGNOSTIC_REASONS:
            raise ValueError("unknown diagnostic day reason")
        if tuple((c.member_id, c.symbol) for c in self.cells) != tuple(
            (m, s) for m in WEIGHT_MEMBERS for s in WEIGHT_SYMBOLS
        ):
            raise ValueError("diagnostic day requires twelve ordered cells")
        if self.status == "complete" and any(
            not c.counts_complete for c in self.cells
        ):
            raise ValueError("complete day has incomplete diagnostic cells")
        if _embedded_nodes(self.to_dict()) > MAX_NODES:
            raise ValueError(
                "complete diagnostic day exceeds expanded-node bound"
            )
        config = EmpiricalMotifGeneratorConfigV1.from_dict(
            _canonical(self.config_json)
        )
        if training_json(config.to_dict()) != self.config_json:
            raise ValueError("unknown/coercive diagnostic config")
        counts = _canonical(self.resource_counts_json)
        if set(counts) != {
            "declared_native_events_per_member",
            "charged_component_escaped_bytes",
            "charged_component_expanded_nodes",
            "ledger_reserve_bytes",
            "ledger_reserve_nodes",
            "counts_complete",
            "incomplete_counts_are_lower_bounds",
            "retained_stage_counts",
        }:
            raise ValueError("unknown/missing diagnostic resource counter")
        if counts["counts_complete"] != (self.status == "complete") or counts[
            "incomplete_counts_are_lower_bounds"
        ] != (self.status != "complete"):
            raise ValueError("diagnostic resource completeness differs")
        for key in (
            "declared_native_events_per_member",
            "charged_component_escaped_bytes",
            "charged_component_expanded_nodes",
        ):
            if type(counts[key]) is not int or cast(int, counts[key]) < 0:
                raise ValueError("diagnostic resource count requires integer")
        if (
            counts["ledger_reserve_bytes"] != 65536
            or counts["ledger_reserve_nodes"] != 4096
        ):
            raise ValueError("diagnostic ledger reserve differs")
        if counts["retained_stage_counts"] != _stage_counts(
            self.cells, self.query_results, self.query_supports
        ):
            raise ValueError("diagnostic retained stage counts differ")
        if self.status == "complete":
            texts = [*self.query_results, *self.query_supports]
            for cell in self.cells:
                texts.extend(cell.interval_records)
                texts.extend(
                    t
                    for t in (
                        cell.stream_encoding_json,
                        cell.transformations_encoding_json,
                    )
                    if t is not None
                )
            if counts["charged_component_escaped_bytes"] != sum(
                _string_cost(t) + 1 for t in texts
            ) or counts["charged_component_expanded_nodes"] != sum(
                _nodes(_canonical(t)) + 1 for t in texts
            ):
                raise ValueError("complete diagnostic resource counts differ")
        supports = {}
        for text in self.query_supports:
            raw = _canonical(text)
            identity = raw.pop("support_id", None)
            expected = (
                "weight-query-support:sha256:"
                + hashlib.sha256(training_json(raw).encode("ascii")).hexdigest()
            )
            if identity != expected or set(raw) != {
                "matches",
                "backoff_attempts",
            }:
                raise ValueError("diagnostic query support identity differs")
            if expected in supports:
                raise ValueError("duplicate diagnostic query support")
            supports[expected] = raw
        result_ids = set()
        used = set()
        for text in self.query_results:
            raw = _canonical(text)
            if set(raw) != {
                "schema_version",
                "index_id",
                "query",
                "status",
                "scanned_fragment_count",
                "hidden_by_availability_count",
                "result_id",
                "support_id",
            }:
                raise ValueError("unknown/missing diagnostic query field")
            query = ReferenceMotifQueryV1.from_dict(_object(raw["query"]))
            if training_json(query.to_dict()) != training_json(raw["query"]):
                raise ValueError("unknown/coercive diagnostic query")
            if raw["index_id"] != self.index_id:
                raise ValueError("diagnostic query index differs")
            support = raw.get("support_id")
            if type(support) is not str or support not in supports:
                raise ValueError("missing diagnostic query support")
            used.add(support)
            result = raw.get("result_id")
            if type(result) is not str or result in result_ids:
                raise ValueError("missing/duplicate diagnostic query identity")
            result_ids.add(result)
        if used != set(supports):
            raise ValueError("unreferenced diagnostic query support")
        referenced = {
            raw["query_result_id"]
            for cell in self.cells
            for text in cell.interval_records
            for raw in [_canonical(text)]
            if "query_result_id" in raw
        }
        if not referenced <= result_ids or (
            self.status == "complete" and referenced != result_ids
        ):
            raise ValueError("diagnostic query ownership differs")
        for member in WEIGHT_MEMBERS:
            count = sum(
                len(
                    cast(
                        list[object],
                        _decode_stream(c.stream_encoding_json)["events"],
                    )
                )
                for c in self.cells
                if c.member_id == member and c.stream_encoding_json is not None
            )
            if count > MAX_EVENTS:
                raise ValueError(
                    "diagnostic triangle native event bound exceeded"
                )


class _DiagnosticLimit(ValueError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


def _stage_counts(
    cells: tuple[TrainingWeightGeneratorCellDiagnosticV1, ...],
    queries: tuple[str, ...],
    supports: tuple[str, ...],
) -> list[dict[str, object]]:
    groups = {
        "query_catalog": [*queries, *supports],
        "interval_records": [t for c in cells for t in c.interval_records],
        "native_streams": [
            c.stream_encoding_json
            for c in cells
            if c.stream_encoding_json is not None
        ],
        "transformations": [
            c.transformations_encoding_json
            for c in cells
            if c.transformations_encoding_json is not None
        ],
    }
    return [
        {
            "stage": stage,
            "components": len(texts),
            "escaped_bytes": sum(_string_cost(t) + 1 for t in texts),
            "expanded_nodes": sum(_nodes(_canonical(t)) + 1 for t in texts),
        }
        for stage, texts in groups.items()
    ]


class _Retainer:
    """Reserve the small final ledger before accumulating heavy diagnostics.

    Counts include escaped embedded text and its parsed nodes. Exhaustion is
    an explicit diagnostic failure, not a scientific engine exclusion.
    """

    def __init__(self) -> None:
        self.bytes = 0
        self.nodes = 0

    def put(self, value: dict[str, object]) -> str:
        try:
            text = training_json(value)
        except ValueError as exc:
            raise _DiagnosticLimit("diagnostic_component_bound") from exc
        byte_count = _string_cost(text) + 1
        node_count = _nodes(value) + 1
        if self.bytes + byte_count > MAX_BYTES - 65536:
            raise _DiagnosticLimit("diagnostic_day_byte_bound")
        if self.nodes + node_count > MAX_NODES - 4096:
            raise _DiagnosticLimit("diagnostic_day_expanded_node_bound")
        self.bytes += byte_count
        self.nodes += node_count
        return text


def _scale_summaries(
    observation: _MotifPlanningObservation,
    result: ReferenceMotifQueryResultV1,
) -> list[dict[str, object]]:
    """Summarize the exact failed greedy cursor, not an alternative search."""
    if observation.refusal != "no_admissible_retrieved_time_scale":
        return []
    times = observation.event_times
    cursor = observation.output_cursor
    previous = observation.left_time_ns if cursor == 0 else times[cursor - 1]
    capacity = len(times) - cursor
    rows = []
    for match in result.matches:
        fragment = match.fragment
        gaps = max(1, len(fragment.event_offsets_ns) - 1)
        lower, upper = (
            fragment.transform_policy.min_time_scale,
            fragment.transform_policy.max_time_scale,
        )
        normal = []
        terminal = []
        counts = [0, 0, 0]
        for count in range(capacity, 0, -1):
            duration = max(
                times[cursor + count - 1] - previous,
                observation.cadence_ns * count,
            )
            source_duration = fragment.duration_ns * ((count + 1) / gaps)
            scale = duration / source_duration
            normal.append(scale)
            terminal_scale = (
                duration + observation.cadence_ns
            ) / source_duration
            if (
                count == capacity
                and scale < lower
                and lower <= terminal_scale <= upper
            ):
                terminal.append(terminal_scale)
                scale = terminal_scale
            counts[0 if scale < lower else 2 if scale > upper else 1] += 1
        rows.append(
            {
                "fragment_id": fragment.fragment_id,
                "duration_ns": fragment.duration_ns,
                "source_gap_count": gaps,
                "allowed_min": lower,
                "allowed_max": upper,
                "tested_count": capacity,
                "normal_min": min(normal),
                "normal_max": max(normal),
                "terminal_branch_count": len(terminal),
                "terminal_min": min(terminal) if terminal else None,
                "terminal_max": max(terminal) if terminal else None,
                "below_count": counts[0],
                "inside_count": counts[1],
                "above_count": counts[2],
            }
        )
    return rows


def _planning_payload(
    observation: _MotifPlanningObservation | None,
    result: ReferenceMotifQueryResultV1,
) -> dict[str, object] | None:
    if observation is None:
        return None
    return {
        "target_times_ns": list(observation.event_times),
        "cadence_ns": observation.cadence_ns,
        "left_time_ns": observation.left_time_ns,
        "timestamp_precision_ns": max(
            1,
            round(
                result.query.condition.metrics.get(
                    "timestamp_precision_ns", 1.0
                )
            ),
        ),
        "output_cursor": observation.output_cursor,
        "segment_ordinal": observation.segment_ordinal,
        # Native seeds span uint64, whereas training JSON integers are int64.
        "segment_seed": (
            str(observation.segment_seed)
            if observation.segment_seed is not None
            else None
        ),
        "refusal": observation.refusal,
        "prefix": _encode_native_rows(
            [dict(t.to_dict()) for t in observation.prefix], "transformation"
        ),
        "retrieved_scale_summaries": _scale_summaries(observation, result),
    }


def _scope(
    bridge: TrainingWeightDayBridgeV1,
    model: TrainingWeightModelV1,
    config: EmpiricalMotifGeneratorConfigV1,
    member: str,
) -> tuple[ReconstructionRunV1, ReconstructionWindowV1]:
    run = ReconstructionRunV1(
        WEIGHT_SYMBOLS,
        (bridge.subset_source.dataset_version_id,),
        (config.config_id, model.stochastic_model_id),
        (member,),
        int(member.removeprefix("member-")),
    )
    start = _day_ns(bridge.utc_date)
    return run, ReconstructionWindowV1(
        run.run_id,
        member,
        WEIGHT_SYMBOLS,
        start + CORE_START_NS,
        start + CORE_END_NS,
        MAX_AGE_NS,
        MAX_AGE_NS + 1,
    )


def diagnose_training_weight_generator_day(
    bridge: TrainingWeightDayBridgeV1,
    model: TrainingWeightModelV1,
    index: ReferenceMotifIndexV1,
    *,
    deadline: float | None = None,
    on_cell: Callable[[str, str], None] | None = None,
) -> TrainingWeightGeneratorDayDiagnosticV1:
    """Diagnose twelve cells, stopping only the refused cell's later intervals.

    Source verification and selected-day authorization belong to the caller.
    This optional monotonic deadline is cooperative, NOT hard containment.
    Unexpected exceptions propagate; they never become empirical exclusions.
    """
    if (
        type(bridge) is not TrainingWeightDayBridgeV1
        or type(model) is not TrainingWeightModelV1
        or type(index) is not ReferenceMotifIndexV1
    ):
        raise TypeError("generator diagnostics require exact typed inputs")
    if bridge.source_plan.evidence_kind is not model.evidence_kind:
        raise ValueError("diagnostic bridge/model evidence kinds differ")
    if training_json(index.to_dict()) != model.index_json:
        raise ValueError("diagnostic index differs from retained full model")
    limit = time.monotonic() + 120.0
    if deadline is not None and (
        type(deadline) not in (float, int)
        or not float("-inf") < deadline < float("inf")
    ):
        raise ValueError("diagnostic deadline must be finite")
    if deadline is not None:
        limit = min(limit, deadline)

    def check_clock() -> None:
        if time.monotonic() >= limit:
            raise TrainingWeightOperationalFailure(
                "candidate_day_deadline", bridge.utc_date
            )

    check_clock()
    policy = read_training_weight_preregistration().to_dict()
    config = EmpiricalMotifGeneratorConfigV1.from_dict(
        _object(_object(policy["generator"])["config"])
    )
    mappings = {m.symbol: m for m in bridge.symbols}
    conditions = {s: _mapping_condition(mappings[s]) for s in WEIGHT_SYMBOLS}
    declared = sum(
        len(m.ordinals)
        + sum(
            _target_cardinality(
                conditions[m.symbol], right.event_time_ns - left.event_time_ns
            )[0]
            for left, right in zip(m.ordinals, m.ordinals[1:])
            if right.event_time_ns > left.event_time_ns
        )
        for m in bridge.symbols
    )
    retained = _Retainer()
    results: dict[str, str] = {}
    supports: dict[str, str] = {}
    queries: dict[tuple[str, int], ReferenceMotifQueryResultV1] = {}
    cells = []
    day_reason: str | None = None
    for member in WEIGHT_MEMBERS:
        for symbol in WEIGHT_SYMBOLS:
            check_clock()
            mapping = mappings[symbol]
            run, window = _scope(bridge, model, config, member)
            total = len(mapping.ordinals) - 1
            records: list[str] = []
            attempted = 0
            stream_text = None
            transforms_text = None
            reason = day_reason
            status = "not_attempted" if day_reason else "success"
            if day_reason is None and on_cell is not None:
                on_cell(member, symbol)
            if day_reason is None and declared > MAX_EVENTS:
                status, reason = (
                    "deterministic_refused",
                    "declared_triangle_event_budget",
                )
            elif day_reason is None:
                anchors = tuple(
                    SyntheticEventV1.observed(
                        symbol=symbol,
                        event_time_ns=o.event_time_ns,
                        event_sequence=o.subset_row_id,
                        bid=o.bid,
                        ask=o.ask,
                        run_id=run.run_id,
                        ensemble_member_id=member,
                        source_version_id=bridge.subset_source.dataset_version_id,
                        source_series_id=mapping.subset_series_id,
                        source_period=mapping.period,
                        source_row_id=o.subset_row_id,
                    )
                    for o in mapping.ordinals
                )
                events: list[SyntheticEventV1] = []
                transforms: list[dict[str, object]] = []
                try:
                    for ordinal, (left, right) in enumerate(
                        zip(anchors, anchors[1:]), 1
                    ):
                        check_clock()
                        attempted += 1
                        common: dict[str, object] = {
                            "interval_ordinal": ordinal,
                            "left_anchor_event_id": left.event_id,
                            "right_anchor_event_id": right.event_id,
                            "left_time_ns": left.event_time_ns,
                            "right_time_ns": right.event_time_ns,
                            "conditioning_start_ns": anchors[0].event_time_ns,
                            "conditioning_end_ns": anchors[-1].event_time_ns
                            + 1,
                            "information_mode": "ex_post_whole_retained_window",
                        }
                        if left.event_time_ns == right.event_time_ns:
                            records.append(
                                retained.put(
                                    {
                                        **common,
                                        "decision": "no_strict_interior_timestamp",
                                    }
                                )
                            )
                            continue
                        key = (symbol, right.event_time_ns)
                        result = queries.get(key)
                        if result is None:
                            result = query_reference_motifs(
                                index,
                                ReferenceMotifQueryV1(
                                    conditions[symbol],
                                    InformationMode.EX_POST_RECONSTRUCTION,
                                    right.event_time_ns,
                                    max_results=64,
                                ),
                            )
                            queries[key] = result
                            raw = _compact_result(result)
                            shared = {
                                "matches": raw.pop("matches"),
                                "backoff_attempts": raw.pop("backoff_attempts"),
                            }
                            support_id = (
                                "weight-query-support:sha256:"
                                + hashlib.sha256(
                                    training_json(shared).encode("ascii")
                                ).hexdigest()
                            )
                            support_text = None
                            if support_id not in supports:
                                support_text = retained.put(
                                    {**shared, "support_id": support_id}
                                )
                            result_text = retained.put(
                                {**raw, "support_id": support_id}
                            )
                            if support_text is not None:
                                supports[support_id] = support_text
                            results[result.result_id] = result_text
                        observed: list[_MotifPlanningObservation] = []
                        with _observe_motif_planning(observed.append):
                            batch = generate_empirical_motif_candidates(
                                run=run,
                                window=window,
                                left_anchor=left,
                                right_anchor=right,
                                query_result=result,
                                config=config,
                            )
                        if len(observed) > 1:
                            raise RuntimeError(
                                "generator emitted duplicate planner observations"
                            )
                        record = {
                            **common,
                            "query_result_id": result.result_id,
                            "decision": batch.decision.value,
                            "native_batch": {
                                **batch.payload(),
                                "batch_id": batch.batch_id,
                            },
                            "resource_estimate": batch.resource_estimate.to_dict(),
                            "planning": _planning_payload(
                                observed[0] if observed else None, result
                            ),
                            "event_lineage": [
                                dict(e.to_dict()) for e in batch.event_lineage
                            ],
                        }
                        records.append(retained.put(record))
                        if batch.status is MotifGenerationStatus.REFUSED:
                            status, reason = (
                                "deterministic_refused",
                                "engine:" + batch.decision.value,
                            )
                            break
                        if any(
                            not left.event_time_ns
                            < e.event_time_ns
                            < right.event_time_ns
                            or not window.owns_event_time(e.event_time_ns)
                            or e.bid <= 0
                            or e.ask < e.bid
                            for e in batch.events
                        ):
                            raise ValueError(
                                "diagnostic native event escaped valid owned interval"
                            )
                        events.extend(batch.events)
                        transforms.extend(
                            dict(t.to_dict()) for t in batch.transformations
                        )
                        if len(events) + len(anchors) > MAX_EVENTS:
                            status, reason = (
                                "deterministic_refused",
                                "native_event_bound",
                            )
                            break
                        if len(transforms) > MAX_EVENTS:
                            status, reason = (
                                "deterministic_refused",
                                "transformation_bound",
                            )
                            break
                    stream = SyntheticEventStreamV1.merge(
                        run_id=run.run_id,
                        ensemble_member_id=member,
                        symbol=symbol,
                        observed_events=anchors,
                        synthetic_events=events,
                    )
                    stream_text = retained.put(
                        training_load(_encode_stream(dict(stream.to_dict())))
                    )
                    transforms_text = retained.put(
                        _encode_native_rows(transforms, "transformation")
                    )
                except _DiagnosticLimit as exc:
                    day_reason = reason = exc.reason
                    status = "diagnostic_failed"
                    stream_text = transforms_text = None
            cell = TrainingWeightGeneratorCellDiagnosticV1(
                member,
                symbol,
                status,
                reason,
                total,
                attempted,
                total - attempted,
                status in {"success", "deterministic_refused"},
                training_json(run.to_dict()),
                training_json(window.to_dict()),
                stream_text,
                transforms_text,
                tuple(records),
            )
            cells.append(cell)
    check_clock()
    day = TrainingWeightGeneratorDayDiagnosticV1(
        read_training_weight_diagnostic_protocol().artifact_id,
        bridge.utc_date,
        bridge.artifact_id,
        model.artifact_id,
        index.index_id,
        model.adapter_source_fingerprint,
        model.stochastic_model_id,
        (
            TrainingWeightDiagnosticEvidenceKind.FIXTURE
            if model.evidence_kind is WeightEvidenceKind.FIXTURE
            else TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC
        ),
        "complete" if day_reason is None else "diagnostic_failed",
        day_reason,
        tuple(cells),
        tuple(results[key] for key in sorted(results)),
        tuple(supports[key] for key in sorted(supports)),
        training_json(config.to_dict()),
        training_json(
            {
                "declared_native_events_per_member": declared,
                "charged_component_escaped_bytes": retained.bytes,
                "charged_component_expanded_nodes": retained.nodes,
                "ledger_reserve_bytes": 65536,
                "ledger_reserve_nodes": 4096,
                "counts_complete": day_reason is None,
                "incomplete_counts_are_lower_bounds": day_reason is not None,
                "retained_stage_counts": _stage_counts(
                    tuple(cells),
                    tuple(results[key] for key in sorted(results)),
                    tuple(supports[key] for key in sorted(supports)),
                ),
            }
        ),
    )
    check_clock()
    return day
