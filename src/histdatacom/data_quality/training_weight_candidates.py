"""Source-bound research-native candidates from a closed immutable model.

These artifacts use the native event schema but are NOT legacy reconstruction
product manifests. No broker, carving, cross-currency or delivery gate is
fabricated. Whole-window conditioning is explicitly ex-post, including when a
legacy query's bookkeeping used_at clock names an earlier right anchor.
"""

from __future__ import annotations

import hashlib
import json
import math
import multiprocessing
import os
import tempfile
import time
from dataclasses import dataclass, fields
from contextvars import ContextVar
from enum import Enum
from pathlib import Path
from typing import Callable, ClassVar, cast

from histdatacom.synthetic.contracts import (
    SyntheticEventStreamV1,
    SyntheticEventV1,
)
from histdatacom.synthetic.generation import (
    EmpiricalMotifGeneratorConfigV1,
    EmpiricalMotifTransformationV1,
    MotifGenerationStatus,
    MotifGenerationDecision,
    _target_cardinality,
    generate_empirical_motif_candidates,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.motifs import (
    ReferenceMotifIndexV1,
    ReferenceMotifConditionV1,
    ReferenceMotifQueryV1,
    ReferenceMotifQueryResultV1,
    query_reference_motifs,
    reference_motif_condition_from_quotes,
)
from histdatacom.synthetic.streaming import (
    ReconstructionRunV1,
    ReconstructionWindowV1,
)

from .training_contracts import (
    TrainingContract,
    training_json,
    training_load,
    _string_cost,
)
from .training_lineage import read_training_regular
from .training_weight_calibration import (
    WEIGHT_MEMBERS,
    WEIGHT_SYMBOLS,
    _hash,
    _label,
)
from .training_weight_contracts import (
    _array,
    _integer,
    _object,
    _text,
    _preregistered_input_hashes,
    read_training_weight_preregistration,
)
from .training_weight_lineage import (
    CORE_END_NS,
    CORE_START_NS,
    MAX_AGE_NS,
    TrainingWeightDayBridgeV1,
    TrainingWeightSymbolBridgeV1,
    TrainingWeightDegradation,
    WeightEvidenceKind,
    _day_ns,
    replay_training_weight_degradation,
)
from .training_weight_approval import (
    TrainingWeightExecutionApprovalV1,
    require_training_weight_approval,
    training_weight_source_fingerprint,
)

MAX_MODEL_BYTES = 2 * 1024 * 1024
MAX_MODEL_INPUT_BYTES = 2 * 1024**3
MAX_SOURCE_PARTITION_BYTES = 128 * 1024**2
MAX_MEMBER_EVENTS = 4096
MAX_EXPANDED_NODES = 131072
_DAY_OBSERVER: ContextVar[Callable[[str | None], None] | None] = ContextVar(
    "training_weight_operational_day_observer", default=None
)


def _encode_native_rows(
    records: list[dict[str, object]], native_type: str
) -> dict[str, object]:
    """Transparent scalar table; repeated fields are shared, never discarded."""
    if len(records) > MAX_MEMBER_EVENTS:
        raise ValueError("native table row bound exceeded")
    keys = _native_fields(native_type)
    if any(set(row) != set(keys) for row in records):
        raise ValueError("native table fields differ from closed schema")
    copied = [dict(row) for row in records]
    if native_type == "transformation":
        for row in copied:
            seed = row["seed"]
            if type(seed) is not int or not 0 <= seed < 2**64:
                raise ValueError("native seed must be exact uint64")
            row["seed"] = str(seed)
    constants = {
        key: copied[0][key]
        for key in keys
        if copied
        and all(
            type(row[key]) is type(copied[0][key])
            and row[key] == copied[0][key]
            and (
                type(row[key]) is not float
                or row[key] != 0.0
                or math.copysign(1.0, cast(float, row[key]))
                == math.copysign(1.0, cast(float, copied[0][key]))
            )
            for row in copied
        )
    }
    columns = [key for key in keys if key not in constants]
    return {
        "format": "histdatacom.weight-native-table.v1",
        "native_type": native_type,
        "seed_encoding": (
            "uint64-decimal" if native_type == "transformation" else None
        ),
        "constants": constants,
        "columns": columns,
        "rows": [[row[key] for key in columns] for row in copied],
    }


def _native_fields(native_type: str) -> tuple[str, ...]:
    if native_type not in ("event", "transformation"):
        raise ValueError("unknown native table type")
    native = (
        SyntheticEventV1
        if native_type == "event"
        else EmpiricalMotifTransformationV1
    )
    keys = {f.name for f in fields(native)}
    if native_type == "transformation":
        keys.add("endpoint_alignment")
    return tuple(sorted(keys))


def _native_scalar_bytes(value: object, key: str, native_type: str) -> int:
    if value is None:
        return 4
    if type(value) is bool:
        return 4 if value else 5
    if type(value) in (int, float):
        return len(_native_json(value))
    if type(value) is str:
        if native_type == "transformation" and key == "seed":
            return len(value)
        return _string_cost(value)
    raise ValueError("native table requires scalar fields")


def _preflight_native_expansion(
    common: dict[str, object],
    columns: tuple[str, ...],
    rows: list[object],
    native_type: str,
) -> None:
    # Charge repeated constants arithmetically BEFORE reconstructing any row
    # or calling native constructors that hash each expanded event/transform.
    keys = (*common, *columns)
    fixed = 2 + max(0, len(keys) - 1) + sum(_string_cost(k) + 1 for k in keys)
    constant_bytes = sum(
        _native_scalar_bytes(v, k, native_type) for k, v in common.items()
    )
    total = 2 + max(0, len(rows) - 1) + len(rows) * (fixed + constant_bytes)
    if total > 8 * 1024 * 1024:
        raise TrainingWeightDeterministicRefusal(
            CandidateRefusalCode.NATIVE_BYTES
        )
    for supplied in rows:
        values = _array(supplied)
        if len(values) != len(columns):
            raise ValueError("native row does not match declared columns")
        total += sum(
            _native_scalar_bytes(v, k, native_type)
            for k, v in zip(columns, values)
        )
        if total > 8 * 1024 * 1024:
            raise TrainingWeightDeterministicRefusal(
                CandidateRefusalCode.NATIVE_BYTES
            )


def _decode_native_rows(
    table: dict[str, object], native_type: str
) -> list[dict[str, object]]:
    if set(table) != {
        "format",
        "native_type",
        "seed_encoding",
        "constants",
        "columns",
        "rows",
    }:
        raise ValueError("unknown or missing native table fields")
    if (
        table["format"] != "histdatacom.weight-native-table.v1"
        or table["native_type"] != native_type
    ):
        raise ValueError("native table schema/type differs")
    if table["seed_encoding"] != (
        "uint64-decimal" if native_type == "transformation" else None
    ):
        raise ValueError("native table seed encoding differs")
    keys = set(_native_fields(native_type))
    common = _object(table["constants"])
    columns = tuple(_text(v) for v in _array(table["columns"]))
    rows = _array(table["rows"])
    if (
        columns != tuple(sorted(set(columns)))
        or set(common) & set(columns)
        or set(common) | set(columns) != keys
        or len(rows) > MAX_MEMBER_EVENTS
    ):
        raise ValueError("native columns/common ownership or row bound differs")
    # Closed fields and <=4096rows explicitly bound native reconstruction;
    # encoded artifact nodes are separately checked across the complete day.
    if len(rows) * len(keys) > MAX_MEMBER_EVENTS * 32:
        raise ValueError("native reconstruction scalar-work bound exceeded")
    _preflight_native_expansion(common, columns, rows, native_type)
    result = []
    for values in rows:
        values = _array(values)
        if len(values) != len(columns):
            raise ValueError("native row does not match declared columns")
        row = {**common, **dict(zip(columns, values))}
        if any(
            v is not None and type(v) not in (str, int, float, bool)
            for v in row.values()
        ):
            raise ValueError("native table accepts only finite scalar values")
        if native_type == "transformation":
            seed = _text(row["seed"])
            if (
                not seed.isascii()
                or not seed.isdecimal()
                or len(seed) > 20
                or str(int(seed)) != seed
                or int(seed) >= 2**64
            ):
                raise ValueError(
                    "native seed encoding must be canonical uint64"
                )
            row["seed"] = int(seed)
        result.append(row)
    # One canonical inverse encoding: no alternate arbitrary placement of
    # constants/columns can produce a second wire identity for the same data.
    if training_json(_encode_native_rows(result, native_type)) != training_json(
        table
    ):
        raise ValueError("native table is not the exact canonical encoding")
    return result


def _encode_stream(stream: dict[str, object]) -> str:
    if "events" not in stream:
        raise ValueError("native stream lacks event rows")
    supplied = _array(stream["events"])
    if len(supplied) > MAX_MEMBER_EVENTS:
        raise ValueError("native table row bound exceeded")
    records = [_object(row) for row in supplied]
    return training_json(
        {
            "header": {
                key: value for key, value in stream.items() if key != "events"
            },
            "events": _encode_native_rows(records, "event"),
        }
    )


def _decode_stream(text: str) -> dict[str, object]:
    raw = training_load(text)
    if training_json(raw) != text:
        raise ValueError("native stream table text must be canonical")
    if set(raw) != {"header", "events"} or "events" in _object(raw["header"]):
        raise ValueError(
            "native stream encoding has unknown/overlapping fields"
        )
    return {
        **_object(raw["header"]),
        "events": _decode_native_rows(_object(raw["events"]), "event"),
    }


def _native_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _consume_nodes(value: object, remaining: list[int]) -> None:
    remaining[0] -= 1
    if remaining[0] < 0:
        raise TrainingWeightDeterministicRefusal(
            CandidateRefusalCode.EXPANDED_NODES
        )
    if isinstance(value, dict):
        for key, item in value.items():
            _consume_nodes(key, remaining)
            _consume_nodes(item, remaining)
    elif isinstance(value, list):
        for item in value:
            _consume_nodes(item, remaining)


class CandidateRefusalCode(str, Enum):
    """Closed deterministic method/resource refusals, never elapsed time."""

    DECLARED_EVENTS = "declared_triangle_event_budget"
    MEMBER_EVENTS = "member_symbol_event_budget"
    LINEAGE_BYTES = "interval_lineage_byte_budget"
    DAY_BYTES = "candidate_day_artifact_byte_budget"
    EXPANDED_NODES = "candidate_expanded_node_budget"
    NATIVE_BYTES = "native_projection_byte_budget"


class TrainingWeightDeterministicRefusal(ValueError):
    def __init__(
        self, reason: CandidateRefusalCode | MotifGenerationDecision
    ) -> None:
        if type(reason) is CandidateRefusalCode:
            self.reason = "candidate:" + reason.value
        elif type(reason) is MotifGenerationDecision and reason not in (
            MotifGenerationDecision.GENERATED,
            MotifGenerationDecision.ZERO_TARGET_ACTIVITY,
        ):
            self.reason = "candidate:engine:" + reason.value
        else:
            raise TypeError("deterministic refusal requires a closed reason")
        super().__init__(self.reason)


class TrainingWeightOperationalFailure(RuntimeError):
    """An unsuccessful execution attempt, not a scientific day exclusion."""

    def __init__(self, reason: str, utc_date: str = "") -> None:
        self.reason = reason
        self.utc_date = utc_date
        super().__init__(reason)


def _mapping_condition(
    mapping: TrainingWeightSymbolBridgeV1,
) -> ReferenceMotifConditionV1:
    return reference_motif_condition_from_quotes(
        symbol=mapping.symbol,
        feed_epoch_id="technology_epoch_03",
        session_state="london",
        event_times_ns=tuple(o.event_time_ns for o in mapping.ordinals),
        bids=tuple(o.bid for o in mapping.ordinals),
        asks=tuple(o.ask for o in mapping.ordinals),
        event_tags=("context:not-admitted",),
        active_sessions=("london",),
        source_quality_score=1.0,
    )


@dataclass(frozen=True, slots=True)
class TrainingWeightFileV1(TrainingContract):
    KIND: ClassVar[str] = "weight-file"
    path: str
    size_bytes: int
    sha256: str

    def _validate(self) -> None:
        _label(self.path)
        _hash(self.sha256)
        if not 0 < self.size_bytes <= MAX_SOURCE_PARTITION_BYTES:
            raise ValueError("weight source artifact exceeds byte bound")

    def verify(self) -> bytes:
        payload = read_training_regular(Path(self.path), self.size_bytes)
        if (
            len(payload) != self.size_bytes
            or hashlib.sha256(payload).hexdigest() != self.sha256
        ):
            raise ValueError(
                "weight source artifact differs from exact retained bytes"
            )
        if not isinstance(payload, bytes):
            raise ValueError("weight source reader did not return bytes")
        return payload


@dataclass(frozen=True, slots=True)
class TrainingWeightModelV1(TrainingContract):
    """Full retained fixed index and source-byte inventory, not a learned refit."""

    KIND: ClassVar[str] = "weight-model"
    evidence_kind: WeightEvidenceKind
    index_file: TrainingWeightFileV1
    index_json: str
    source_files: tuple[TrainingWeightFileV1, ...]
    epoch_file: TrainingWeightFileV1 | None
    adapter_source_fingerprint: str

    def _validate(self) -> None:
        _hash(self.adapter_source_fingerprint)
        if self.evidence_kind is WeightEvidenceKind.FIXTURE:
            known = _preregistered_input_hashes()
            if any(
                item.sha256 in known
                for item in (
                    self.index_file,
                    *self.source_files,
                    *(
                        (self.epoch_file,)
                        if self.epoch_file is not None
                        else ()
                    ),
                )
            ):
                raise ValueError(
                    "fixture cannot relabel a known preregistered input"
                )
        if (
            len(self.index_json) > MAX_MODEL_BYTES
            or self.index_file.size_bytes > MAX_MODEL_BYTES
        ):
            raise ValueError("fixed model index exceeds byte bound")
        raw = training_load(self.index_json)
        index = ReferenceMotifIndexV1.from_dict(raw)
        if training_json(index.to_dict()) != self.index_json:
            raise ValueError(
                "model index contains noncanonical, unknown or coercive fields"
            )
        keys = tuple((f.sha256, f.path) for f in self.source_files)
        if not 0 < len(keys) <= 15 or keys != tuple(sorted(set(keys))):
            raise ValueError(
                "model source inventory must be bounded, sorted and unique"
            )
        if sum(f.size_bytes for f in self.source_files) > MAX_MODEL_INPUT_BYTES:
            raise ValueError(
                "model source verification exceeds total byte bound"
            )
        available = {(f.sha256, f.size_bytes) for f in self.source_files}
        if any(
            (f.source_artifact.sha256, f.source_artifact.size_bytes)
            not in available
            for f in index.fragments
        ):
            raise ValueError(
                "model source bytes do not cover every retained fragment"
            )
        if any(
            2010 <= datetime_year(f.source_start_ns) <= 2011
            or 2010 <= datetime_year(f.source_end_ns) <= 2011
            for f in index.fragments
        ):
            raise ValueError(
                "model training support overlaps fit/application era"
            )
        if self.evidence_kind is WeightEvidenceKind.PREREGISTERED:
            policy = read_training_weight_preregistration().to_dict()
            model = _object(policy["fixed_model"])
            epoch = _object(policy["epoch_mapping"])
            if (
                self.index_file.sha256 != model["index_sha256"]
                or self.index_file.size_bytes != model["index_size_bytes"]
                or index.index_id != model["index_id"]
            ):
                raise ValueError(
                    "model differs from preregistered complete immutable index"
                )
            expected = {
                (p["sha256"], p["size_bytes"])
                for p in (_object(v) for v in _array(model["training_sources"]))
            }
            if available != expected or len(self.source_files) != 15:
                raise ValueError(
                    "model inventory differs from complete TRAIN source scope"
                )
            if (
                self.epoch_file is None
                or self.epoch_file.sha256 != epoch["sha256"]
                or self.epoch_file.size_bytes != 246353
            ):
                raise ValueError("model lacks exact frozen epoch mapping")
        elif self.epoch_file is not None:
            raise ValueError(
                "fixture model cannot claim the empirical epoch artifact"
            )

    @property
    def index(self) -> ReferenceMotifIndexV1:
        return ReferenceMotifIndexV1.from_dict(training_load(self.index_json))

    @property
    def stochastic_model_id(self) -> str:
        """Content/method identity excludes external read locators.

        The complete index bytes still bind its original internal ancestry.
        Copying those unchanged bytes and their input files is not a new
        stochastic model or an opportunity to choose another seed.
        """
        payload = {
            "protocol_id": read_training_weight_preregistration().artifact_id,
            "evidence_kind": self.evidence_kind.value,
            "index": [self.index_file.sha256, self.index_file.size_bytes],
            "source_content": [
                list(pair)
                for pair in sorted(
                    {(f.sha256, f.size_bytes) for f in self.source_files}
                )
            ],
            "epoch": (
                [self.epoch_file.sha256, self.epoch_file.size_bytes]
                if self.epoch_file is not None
                else None
            ),
            "adapter": self.adapter_source_fingerprint,
        }
        return (
            "training-weight-stochastic-model:sha256:"
            + hashlib.sha256(training_json(payload).encode("ascii")).hexdigest()
        )


def datetime_year(time_ns: int) -> int:
    from datetime import datetime, timezone

    return datetime.fromtimestamp(
        time_ns // 1_000_000_000, tz=timezone.utc
    ).year


def _file(path: Path, limit: int) -> tuple[TrainingWeightFileV1, bytes]:
    payload = read_training_regular(path, limit)
    return (
        TrainingWeightFileV1(
            str(path.absolute()),
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        ),
        payload,
    )


def read_training_weight_model(
    index_path: str | Path,
    *,
    evidence_kind: WeightEvidenceKind,
    adapter_source_fingerprint: str,
    model_source_root: str | Path | None = None,
    epoch_path: str | Path | None = None,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightModelV1:
    """Hash model training files without decoding them; retain the whole index."""
    if evidence_kind is WeightEvidenceKind.PREREGISTERED:
        require_training_weight_approval(approval, "candidate-generation")
    index_ref, payload = _file(Path(index_path), MAX_MODEL_BYTES)
    if (
        evidence_kind is WeightEvidenceKind.FIXTURE
        and index_ref.sha256 in _preregistered_input_hashes()
    ):
        raise ValueError("fixture cannot relabel a known preregistered input")
    raw = training_load(payload.decode("utf-8"))
    index = ReferenceMotifIndexV1.from_dict(raw)
    if training_json(raw) != training_json(index.to_dict()):
        raise ValueError("model index contains unknown/coercive fields")
    paths: list[tuple[Path, int, str]] = []
    epoch_ref = None
    if evidence_kind is WeightEvidenceKind.PREREGISTERED:
        if model_source_root is None or epoch_path is None:
            raise ValueError(
                "real model requires explicit TRAIN root and epoch artifact"
            )
        policy = read_training_weight_preregistration().to_dict()
        model = _object(policy["fixed_model"])
        if index_ref.sha256 != model["index_sha256"]:
            raise ValueError("real index is outside preregistered allowlist")
        for value in _array(model["training_sources"]):
            source = _object(value)
            paths.append(
                (
                    Path(model_source_root) / _text(source["path"]),
                    _integer(source["size_bytes"]),
                    _text(source["sha256"]),
                )
            )
        epoch_ref, _ = _file(Path(epoch_path), MAX_MODEL_BYTES)
    elif evidence_kind is WeightEvidenceKind.FIXTURE:
        if model_source_root is not None or epoch_path is not None:
            raise ValueError(
                "fixture model cannot introduce empirical input overrides"
            )
        paths = [
            (Path(a.path), cast(int, a.size_bytes), a.sha256)
            for a in index.lineage_artifacts
        ]
    else:
        raise ValueError("unsupported model evidence kind")
    if (
        not 0 < len(paths) <= 15
        or any(
            not 0 < size <= MAX_SOURCE_PARTITION_BYTES for _, size, _ in paths
        )
        or sum(size for _, size, _ in paths) > MAX_MODEL_INPUT_BYTES
    ):
        raise ValueError("model input inventory exceeds preflight work bounds")
    files = tuple(
        sorted(
            (
                TrainingWeightFileV1(str(path.absolute()), size, digest)
                for path, size, digest in paths
            ),
            key=lambda f: (f.sha256, f.path),
        )
    )
    if evidence_kind is WeightEvidenceKind.FIXTURE and any(
        f.sha256 in _preregistered_input_hashes() for f in files
    ):
        raise ValueError("fixture cannot relabel a known preregistered input")
    for source_file in files:
        source_file.verify()
    result = TrainingWeightModelV1(
        evidence_kind,
        index_ref,
        training_json(raw),
        files,
        epoch_ref,
        adapter_source_fingerprint,
    )
    if index_ref.verify() != payload:
        raise ValueError("model index changed during source verification")
    return result


def verify_training_weight_model(
    model: TrainingWeightModelV1,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> ReferenceMotifIndexV1:
    """Fresh exact source-byte verification, never a model-ID authorization."""
    if type(model) is not TrainingWeightModelV1:
        raise TypeError("model verification requires its typed contract")
    if model.evidence_kind is WeightEvidenceKind.PREREGISTERED:
        require_training_weight_approval(approval, "candidate-generation")
    payload = model.index_file.verify()
    if (
        training_json(training_load(payload.decode("utf-8")))
        != model.index_json
    ):
        raise ValueError("retained model differs from actual index bytes")
    for source in model.source_files:
        source.verify()
    if model.epoch_file is not None:
        model.epoch_file.verify()
    if model.index_file.verify() != payload:
        raise ValueError("model changed during verification")
    return model.index


@dataclass(frozen=True, slots=True)
class TrainingWeightCandidateV1(TrainingContract):
    """Full native events plus replayable compact interval/index references."""

    KIND: ClassVar[str] = "weight-research-candidate"
    model_id: str
    bridge_id: str
    member_id: str
    symbol: str
    run_json: str
    window_json: str
    stream_encoding_json: str
    transformations_encoding_json: str
    interval_records: tuple[str, ...]
    status: str = "research_candidate_not_production_qualified"

    def _validate(self) -> None:
        _label(self.model_id)
        _label(self.bridge_id)
        if (
            self.member_id not in WEIGHT_MEMBERS
            or self.symbol not in WEIGHT_SYMBOLS
            or self.status != "research_candidate_not_production_qualified"
        ):
            raise ValueError("research candidate scope/status differs")
        raw_run, raw_window = (
            training_load(text) for text in (self.run_json, self.window_json)
        )
        raw_stream = _decode_stream(self.stream_encoding_json)
        native_stream_text = training_json(raw_stream)
        run = ReconstructionRunV1.from_dict(raw_run)
        window = ReconstructionWindowV1.from_dict(raw_window)
        stream = SyntheticEventStreamV1.from_dict(raw_stream)
        for text, restored in (
            (self.run_json, run.to_dict()),
            (self.window_json, window.to_dict()),
            (native_stream_text, stream.to_dict()),
        ):
            if training_json(restored) != text:
                raise ValueError(
                    "native candidate has unknown/coercive/noncanonical fields"
                )
        if (
            stream.run_id != run.run_id
            or window.run_id != run.run_id
            or stream.ensemble_member_id != self.member_id
            or window.ensemble_member_id != self.member_id
            or stream.symbol.upper() != self.symbol
            or tuple(s.upper() for s in run.symbols) != WEIGHT_SYMBOLS
            or run.ensemble_member_ids != (self.member_id,)
        ):
            raise ValueError("native event/run/window scope differs")
        if (
            not 0 < len(stream.events) <= MAX_MEMBER_EVENTS
            or not 0 < len(self.interval_records) <= 1025
        ):
            raise ValueError(
                "research candidate exceeds event/interval work bounds"
            )
        for record in self.interval_records:
            if training_json(training_load(record)) != record:
                raise ValueError(
                    "interval lineage must be exact canonical JSON"
                )
        transforms = self.native_transformations
        identifiers = {t.transformation_id for t in transforms}
        if len(identifiers) != len(transforms):
            raise ValueError("candidate repeats native transformation")
        referenced = [
            _text(value)
            for text in self.interval_records
            for value in _array(
                training_load(text).get("transformation_ids", [])
            )
        ]
        if (
            len(referenced) != len(set(referenced))
            or set(referenced) != identifiers
        ):
            raise ValueError("native transformation ownership differs")

    @property
    def stream(self) -> SyntheticEventStreamV1:
        return SyntheticEventStreamV1.from_dict(
            _decode_stream(self.stream_encoding_json)
        )

    @property
    def stream_json(self) -> str:
        """Exact reconstructed native v1 JSON, never changed field meanings."""
        return training_json(self.stream.to_dict())

    @property
    def native_transformations(
        self,
    ) -> tuple[EmpiricalMotifTransformationV1, ...]:
        table = training_load(self.transformations_encoding_json)
        if training_json(table) != self.transformations_encoding_json:
            raise ValueError(
                "native transformation table text must be canonical"
            )
        raw = _decode_native_rows(table, "transformation")
        result = tuple(
            EmpiricalMotifTransformationV1.from_dict(row) for row in raw
        )
        if _native_json([t.to_dict() for t in result]) != _native_json(raw):
            raise ValueError("native transformation contains coercive fields")
        return result


def _compact_result(result: ReferenceMotifQueryResultV1) -> dict[str, object]:
    # Full fragments live once in the immutable retained index, not repeated
    # thousands of times in per-anchor results. The replay reconstructs the
    # original complete result, including its native content ID, exactly.
    raw: dict[str, object] = dict(result.to_dict())
    raw["matches"] = [
        {
            **{k: v for k, v in match.to_dict().items() if k != "fragment"},
            "fragment_id": match.fragment.fragment_id,
        }
        for match in result.matches
    ]
    return raw


def _query_catalog(
    results: tuple[ReferenceMotifQueryResultV1, ...],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Lossless content dictionary for repeated ranked support, not truncation."""
    support: dict[str, str] = {}
    queries = []
    for result in sorted(results, key=lambda r: r.result_id):
        raw = _compact_result(result)
        shared = {
            "matches": raw.pop("matches"),
            "backoff_attempts": raw.pop("backoff_attempts"),
        }
        payload = training_json(shared)
        identity = (
            "weight-query-support:sha256:"
            + hashlib.sha256(payload.encode("ascii")).hexdigest()
        )
        support[identity] = training_json({**shared, "support_id": identity})
        queries.append(training_json({**raw, "support_id": identity}))
    return tuple(queries), tuple(support[key] for key in sorted(support))


def _generate_member_symbol(
    bridge: TrainingWeightDayBridgeV1,
    model: TrainingWeightModelV1,
    index: ReferenceMotifIndexV1,
    member: str,
    symbol: str,
    queries: dict[tuple[str, int], ReferenceMotifQueryResultV1],
    condition: ReferenceMotifConditionV1,
    deadline: float,
) -> TrainingWeightCandidateV1:
    policy = read_training_weight_preregistration().to_dict()
    config = EmpiricalMotifGeneratorConfigV1.from_dict(
        _object(_object(policy["generator"])["config"])
    )
    run = ReconstructionRunV1(
        WEIGHT_SYMBOLS,
        (bridge.subset_source.dataset_version_id,),
        (config.config_id, model.stochastic_model_id),
        (member,),
        int(member.removeprefix("member-")),
    )
    day_start = _day_ns(bridge.utc_date)
    window = ReconstructionWindowV1(
        run.run_id,
        member,
        WEIGHT_SYMBOLS,
        day_start + CORE_START_NS,
        day_start + CORE_END_NS,
        MAX_AGE_NS,
        MAX_AGE_NS + 1,
    )
    mapping = next(s for s in bridge.symbols if s.symbol == symbol)
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
    records: list[str] = []
    events: list[SyntheticEventV1] = []
    transformations: list[dict[str, object]] = []
    record_bytes = 0

    def append_record(record: dict[str, object]) -> None:
        nonlocal record_bytes
        text = training_json(record)
        record_bytes += len(text) + text.count('"') + text.count("\\") + 3
        if record_bytes > 4 * 1024 * 1024:
            raise TrainingWeightDeterministicRefusal(
                CandidateRefusalCode.LINEAGE_BYTES
            )
        records.append(text)

    for left, right in zip(anchors, anchors[1:]):
        if time.monotonic() > deadline:
            raise TrainingWeightOperationalFailure(
                "candidate_day_deadline", bridge.utc_date
            )
        dependency = {
            "conditioning_start_ns": anchors[0].event_time_ns,
            "conditioning_end_ns": anchors[-1].event_time_ns + 1,
            "information_mode": "ex_post_whole_retained_window_not_right_anchor_availability",
            "left_anchor_event_id": left.event_id,
            "right_anchor_event_id": right.event_id,
        }
        if left.event_time_ns == right.event_time_ns:
            append_record(
                {
                    **dependency,
                    "status": "no_strict_interior_timestamp",
                    "generated_event_ids": [],
                }
            )
            continue
        key = (symbol, right.event_time_ns)
        result = queries.get(key)
        if result is None:
            query = ReferenceMotifQueryV1(
                condition,
                InformationMode.EX_POST_RECONSTRUCTION,
                right.event_time_ns,
                max_results=64,
            )
            result = query_reference_motifs(index, query)
            queries[key] = result
        batch = generate_empirical_motif_candidates(
            run=run,
            window=window,
            left_anchor=left,
            right_anchor=right,
            query_result=result,
            config=config,
        )
        if batch.status is MotifGenerationStatus.REFUSED:
            raise TrainingWeightDeterministicRefusal(batch.decision)
        if any(
            not left.event_time_ns < e.event_time_ns < right.event_time_ns
            or not window.owns_event_time(e.event_time_ns)
            or e.bid <= 0
            or e.ask < e.bid
            for e in batch.events
        ):
            raise ValueError(
                "candidate event escaped positive noncrossed owned interval"
            )
        events.extend(batch.events)
        if (
            len(transformations) + len(batch.transformations)
            > MAX_MEMBER_EVENTS
        ):
            raise TrainingWeightDeterministicRefusal(
                CandidateRefusalCode.MEMBER_EVENTS
            )
        transformations.extend(dict(t.to_dict()) for t in batch.transformations)
        if len(events) + len(anchors) > MAX_MEMBER_EVENTS:
            raise TrainingWeightDeterministicRefusal(
                CandidateRefusalCode.MEMBER_EVENTS
            )
        append_record(
            {
                **dependency,
                # Shared once per day across every member, not four copies
                # of the same ranked matches per interval.
                "query_result_id": result.result_id,
                # metadata() repeats the complete condition, configuration
                # and backoff attempts. Those exact values are already in
                # the shared query catalog/frozen configuration; retain the
                # complete native identity payload and batch ID once here.
                "candidate_metadata": {
                    **batch.payload(),
                    "batch_id": batch.batch_id,
                },
                "transformation_ids": [
                    t.transformation_id for t in batch.transformations
                ],
                "event_lineage": [e.to_dict() for e in batch.event_lineage],
                "generated_event_ids": [e.event_id for e in batch.events],
            }
        )
    stream = SyntheticEventStreamV1.merge(
        run_id=run.run_id,
        ensemble_member_id=member,
        symbol=symbol,
        observed_events=anchors,
        synthetic_events=events,
    )
    return TrainingWeightCandidateV1(
        model.artifact_id,
        bridge.artifact_id,
        member,
        symbol,
        training_json(run.to_dict()),
        training_json(window.to_dict()),
        _encode_stream(dict(stream.to_dict())),
        training_json(_encode_native_rows(transformations, "transformation")),
        tuple(records),
    )


@dataclass(frozen=True, slots=True)
class TrainingWeightCandidateDayV1(TrainingContract):
    KIND: ClassVar[str] = "weight-candidate-day"
    bridge_id: str
    model_id: str
    candidates: tuple[TrainingWeightCandidateV1, ...]
    query_results: tuple[str, ...]
    query_supports: tuple[str, ...]

    def _validate(self) -> None:
        if tuple((c.member_id, c.symbol) for c in self.candidates) != tuple(
            (m, s) for m in WEIGHT_MEMBERS for s in WEIGHT_SYMBOLS
        ):
            raise ValueError(
                "candidate day requires all four complete triangle members"
            )
        if any(
            c.bridge_id != self.bridge_id or c.model_id != self.model_id
            for c in self.candidates
        ):
            raise ValueError("candidate day mixes source/model lineage")
        remaining = [MAX_EXPANDED_NODES]
        _consume_nodes(self.to_dict(), remaining)
        support_ids = []
        for text in self.query_supports:
            support = training_load(text)
            _consume_nodes(support, remaining)
            identity = _text(support.pop("support_id"))
            if set(support) != {"matches", "backoff_attempts"}:
                raise ValueError(
                    "query support contains unknown/missing fields"
                )
            expected = (
                "weight-query-support:sha256:"
                + hashlib.sha256(
                    training_json(support).encode("ascii")
                ).hexdigest()
            )
            if (
                identity != expected
                or training_json({**support, "support_id": identity}) != text
            ):
                raise ValueError("query support dictionary identity differs")
            support_ids.append(identity)
        if support_ids != sorted(set(support_ids)):
            raise ValueError(
                "query support dictionary must be sorted and unique"
            )
        query_ids = []
        used_support: set[str] = set()
        for text in self.query_results:
            query = training_load(text)
            if training_json(query) != text:
                raise ValueError("query catalog must be canonical")
            _consume_nodes(query, remaining)
            query_ids.append(_text(query["result_id"]))
            used_support.add(_text(query["support_id"]))
        if query_ids != sorted(set(query_ids)):
            raise ValueError("query catalog must be unique and sorted")
        if used_support != set(support_ids):
            raise ValueError("query support ownership differs")
        referenced: set[str] = set()
        for candidate in self.candidates:
            for text in (
                candidate.run_json,
                candidate.window_json,
                candidate.stream_encoding_json,
                candidate.transformations_encoding_json,
            ):
                _consume_nodes(training_load(text), remaining)
            for text in candidate.interval_records:
                record = training_load(text)
                _consume_nodes(record, remaining)
                if "query_result_id" in record:
                    referenced.add(_text(record["query_result_id"]))
        if referenced != set(query_ids):
            raise ValueError("candidate query catalog ownership differs")
        for member in WEIGHT_MEMBERS:
            if (
                sum(
                    len(c.stream.events)
                    for c in self.candidates
                    if c.member_id == member
                )
                > MAX_MEMBER_EVENTS
            ):
                raise ValueError(
                    "complete triangle member exceeds daily event budget"
                )


def generate_training_weight_candidate_day(
    degradation: TrainingWeightDegradation,
    utc_date: str,
    model: TrainingWeightModelV1,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightCandidateDayV1:
    """Replay source/model then run a complete day; failure returns no partial member."""
    _candidate_gate(degradation, model, approval)
    replay_training_weight_degradation(degradation, approval=approval)
    if model.evidence_kind is not degradation.source_plan.evidence_kind:
        raise ValueError(
            "candidate cannot promote fixture source/model evidence"
        )
    matches = tuple(b for b in degradation.bridges if b.utc_date == utc_date)
    if len(matches) != 1:
        raise ValueError("candidate date is not an admitted exact source day")
    index = verify_training_weight_model(model, approval=approval)
    bridge = matches[0]
    return _generate_day(bridge, model, index)


def _candidate_gate(
    degradation: TrainingWeightDegradation,
    model: TrainingWeightModelV1,
    approval: TrainingWeightExecutionApprovalV1 | None,
) -> None:
    if (
        type(degradation) is not TrainingWeightDegradation
        or type(model) is not TrainingWeightModelV1
    ):
        raise TypeError(
            "candidate workflow requires typed source/model contracts"
        )
    if model.evidence_kind is not degradation.source_plan.evidence_kind:
        raise ValueError(
            "candidate cannot promote fixture source/model evidence"
        )
    if model.evidence_kind is WeightEvidenceKind.PREREGISTERED:
        require_training_weight_approval(approval, "candidate-generation")
        if (
            model.adapter_source_fingerprint
            != training_weight_source_fingerprint()
        ):
            raise ValueError("fixed model uses a different executable adapter")


def _generate_day(
    bridge: TrainingWeightDayBridgeV1,
    model: TrainingWeightModelV1,
    index: ReferenceMotifIndexV1,
) -> TrainingWeightCandidateDayV1:
    observer = _DAY_OBSERVER.get()
    if observer is not None:
        observer(bridge.utc_date)
    try:
        result = _generate_day_supervised(bridge, model, index)
    except TrainingWeightDeterministicRefusal:
        if observer is not None:
            observer(None)
        raise
    if observer is not None:
        observer(None)
    return result


def _generate_day_supervised(
    bridge: TrainingWeightDayBridgeV1,
    model: TrainingWeightModelV1,
    index: ReferenceMotifIndexV1,
) -> TrainingWeightCandidateDayV1:
    """Isolate one complete day under the frozen hard wall-clock limit.

    Source/model bytes were freshly verified by the caller, once per shard.
    The child reconstructs only the retained bounded contracts; it never
    rereads a whole historical month per member or starts a new process group.
    """
    if index.index_id != model.index.index_id:
        raise ValueError("day worker index differs from retained fixed model")
    with tempfile.TemporaryDirectory(prefix="histdatacom-weight-day-") as root:
        _supervise_day_worker(
            _candidate_day_worker,
            (bridge.to_json(), model.to_json(), root),
            120.0,
            bridge.utc_date,
        )
        failure_path = Path(root) / "failure.json"
        if failure_path.exists():
            failure = training_load(
                read_training_regular(failure_path, 4096).decode("ascii")
            )
            if failure.get("status") == "deterministic_refusal":
                reason = _text(failure["reason"])
                for code in CandidateRefusalCode:
                    if reason == "candidate:" + code.value:
                        raise TrainingWeightDeterministicRefusal(code)
                for decision in MotifGenerationDecision:
                    if reason == "candidate:engine:" + decision.value:
                        raise TrainingWeightDeterministicRefusal(decision)
            if failure.get("reason") == "candidate_day_deadline":
                raise TrainingWeightOperationalFailure(
                    "candidate_day_deadline", bridge.utc_date
                )
            raise TrainingWeightOperationalFailure(
                "unexpected_candidate_failure", bridge.utc_date
            )
        payload = read_training_regular(
            Path(root) / "day.json", 8 * 1024 * 1024
        )
        return TrainingWeightCandidateDayV1.from_json(payload.decode("ascii"))


def _supervise_day_worker(
    target: Callable[..., None],
    args: tuple[object, ...],
    timeout_seconds: float,
    utc_date: str,
) -> None:
    """Private testable supervision; public callers always supply120seconds."""
    start = time.monotonic()
    worker = multiprocessing.get_context("spawn").Process(
        target=target, args=args
    )
    worker.start()
    try:
        worker.join(max(0.0, timeout_seconds - (time.monotonic() - start)))
        if worker.is_alive():
            worker.terminate()
            worker.join(2.0)
            if worker.is_alive():
                worker.kill()
                worker.join(2.0)
            if worker.is_alive():
                raise TrainingWeightOperationalFailure(
                    "candidate_worker_not_reaped", utc_date
                )
            raise TrainingWeightOperationalFailure(
                "candidate_day_deadline", utc_date
            )
        if worker.exitcode != 0:
            raise TrainingWeightOperationalFailure(
                "candidate_worker_abnormal_exit", utc_date
            )
    except BaseException:
        if worker.is_alive():
            worker.terminate()
            worker.join(2.0)
            if worker.is_alive():
                worker.kill()
                worker.join(2.0)
        raise
    finally:
        if not worker.is_alive():
            worker.close()


def _candidate_day_worker(
    bridge_json: str, model_json: str, directory: str
) -> None:
    root = Path(directory)
    try:
        bridge = TrainingWeightDayBridgeV1.from_json(bridge_json)
        model = TrainingWeightModelV1.from_json(model_json)
        day = _generate_day_inline(bridge, model, model.index)
        payload = day.to_json()
        name = "day.json"
    except TrainingWeightDeterministicRefusal as exc:
        payload = training_json(
            {"status": "deterministic_refusal", "reason": exc.reason}
        )
        name = "failure.json"
    except TrainingWeightOperationalFailure as exc:
        payload = training_json(
            {"status": "operational_failure", "reason": exc.reason}
        )
        name = "failure.json"
    except Exception:
        # No arbitrary exception text is allowed to become a day exclusion.
        # The campaign supervisor retains this failed attempt separately.
        payload = training_json({"status": "operational_failure"})
        name = "failure.json"
    with (root / name).open("xb") as stream:
        stream.write(payload.encode("ascii"))
        stream.flush()
        os.fsync(stream.fileno())


def _generate_day_inline(
    bridge: TrainingWeightDayBridgeV1,
    model: TrainingWeightModelV1,
    index: ReferenceMotifIndexV1,
) -> TrainingWeightCandidateDayV1:
    """Trusted worker computation; direct calls are only synthetic probes."""
    start = time.monotonic()
    conditions = {
        mapping.symbol: _mapping_condition(mapping)
        for mapping in bridge.symbols
    }
    declared_events = sum(
        len(mapping.ordinals)
        + sum(
            _target_cardinality(
                conditions[mapping.symbol],
                right.event_time_ns - left.event_time_ns,
            )[0]
            for left, right in zip(mapping.ordinals, mapping.ordinals[1:])
            if right.event_time_ns > left.event_time_ns
        )
        for mapping in bridge.symbols
    )
    if declared_events > MAX_MEMBER_EVENTS:
        raise TrainingWeightDeterministicRefusal(
            CandidateRefusalCode.DECLARED_EVENTS
        )
    queries: dict[tuple[str, int], ReferenceMotifQueryResultV1] = {}
    candidates: list[TrainingWeightCandidateV1] = []
    retained_bytes = 1024
    for member in WEIGHT_MEMBERS:
        for symbol in WEIGHT_SYMBOLS:
            if time.monotonic() - start > 120:
                raise TrainingWeightOperationalFailure(
                    "candidate_day_deadline", bridge.utc_date
                )
            candidate = _generate_member_symbol(
                bridge,
                model,
                index,
                member,
                symbol,
                queries,
                conditions[symbol],
                start + 120,
            )
            retained_bytes += len(candidate.to_json()) + 1
            if retained_bytes > 8 * 1024 * 1024:
                raise TrainingWeightDeterministicRefusal(
                    CandidateRefusalCode.DAY_BYTES
                )
            candidates.append(candidate)
    query_records, query_support = _query_catalog(tuple(queries.values()))
    retained_bytes += sum(
        len(text) + text.count('"') + text.count("\\") + 3
        for text in (*query_records, *query_support)
    )
    if retained_bytes > 8 * 1024 * 1024:
        raise TrainingWeightDeterministicRefusal(CandidateRefusalCode.DAY_BYTES)
    result = TrainingWeightCandidateDayV1(
        bridge.artifact_id,
        model.artifact_id,
        tuple(candidates),
        query_records,
        query_support,
    )
    if time.monotonic() - start > 120:
        raise TrainingWeightOperationalFailure(
            "candidate_day_deadline", bridge.utc_date
        )
    return result


@dataclass(frozen=True, slots=True)
class TrainingWeightCandidateRefusalV1(TrainingContract):
    KIND: ClassVar[str] = "weight-candidate-refusal"
    utc_date: str
    reason: str

    def _validate(self) -> None:
        _day_ns(self.utc_date)
        _label(self.reason)


@dataclass(frozen=True, slots=True)
class TrainingWeightCandidateShard:
    """Process-local complete scheduled shard, never a verification receipt."""

    degradation: TrainingWeightDegradation
    model: TrainingWeightModelV1
    shard_id: str
    days: tuple[TrainingWeightCandidateDayV1, ...]
    refusals: tuple[TrainingWeightCandidateRefusalV1, ...]


def _shard_dates(
    degradation: TrainingWeightDegradation, shard_id: str
) -> tuple[str, ...]:
    policy = read_training_weight_preregistration().to_dict()
    for entry in _array(policy["schedule"]):
        item = _object(entry)
        if (
            item["period"] != degradation.source_plan.period
            or item["role"] != degradation.source_plan.role
        ):
            continue
        for candidate in _array(item["shards"]):
            shard = _object(candidate)
            if shard["shard_id"] == shard_id:
                return tuple(_text(d) for d in _array(shard["dates"]))
    raise ValueError(
        "shard is outside the source's fixed preregistered schedule"
    )


def generate_training_weight_candidate_shard(
    degradation: TrainingWeightDegradation,
    shard_id: str,
    model: TrainingWeightModelV1,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightCandidateShard:
    """One parent/model replay for <=8 scheduled days, never per member/row."""
    dates = _shard_dates(degradation, shard_id)
    _candidate_gate(degradation, model, approval)
    replay_training_weight_degradation(degradation, approval=approval)
    index = verify_training_weight_model(model, approval=approval)
    by_date = {b.utc_date: b for b in degradation.bridges}
    refused = {r.utc_date: r.reason.value for r in degradation.refusals}
    days: list[TrainingWeightCandidateDayV1] = []
    refusals: list[TrainingWeightCandidateRefusalV1] = []
    for day in dates:
        if day not in by_date:
            refusals.append(
                TrainingWeightCandidateRefusalV1(day, "source:" + refused[day])
            )
            continue
        try:
            generated = _generate_day(by_date[day], model, index)
        except TrainingWeightDeterministicRefusal as exc:
            # Only closed, explicitly classified method/resource conditions
            # are reproducible exclusions. Timeouts and arbitrary exceptions
            # abort the operational attempt, never silently select its days.
            refusals.append(TrainingWeightCandidateRefusalV1(day, exc.reason))
        else:
            days.append(generated)
    return TrainingWeightCandidateShard(
        degradation, model, shard_id, tuple(days), tuple(refusals)
    )


def replay_training_weight_candidate_shard(
    shard: TrainingWeightCandidateShard,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightCandidateShard:
    if type(shard) is not TrainingWeightCandidateShard:
        raise TypeError("candidate replay requires a typed research shard")
    expected = generate_training_weight_candidate_shard(
        shard.degradation, shard.shard_id, shard.model, approval=approval
    )
    if expected != shard:
        raise ValueError(
            "research shard differs from full source/model/generator replay"
        )
    return shard
