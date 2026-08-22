"""Bounded execution contracts for synthetic reconstruction streams.

The version-one contracts in this module are the control-plane boundary
between narrow synthetic events and later production orchestration/storage.
They intentionally do not generate events, write final Parquet partitions, or
define Temporal workflows.  Data-plane rows remain in process-local memory or
artifacts referenced by :class:`~histdatacom.runtime_contracts.ArtifactRef`.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, cast

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

RECONSTRUCTION_STORAGE_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-storage-policy.v1"
)
RECONSTRUCTION_RESOURCE_ESTIMATE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-resource-estimate.v1"
)
RECONSTRUCTION_RUN_SCHEMA_VERSION = "histdatacom.reconstruction-run.v1"
RECONSTRUCTION_WINDOW_SCHEMA_VERSION = "histdatacom.reconstruction-window.v1"
EVENT_BATCH_SCHEMA_VERSION = "histdatacom.reconstruction-event-batch.v1"
CARRY_STATE_SCHEMA_VERSION = "histdatacom.reconstruction-carry-state.v1"
REJECTION_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-rejection-summary.v1"
)
PARTITION_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-partition-manifest.v1"
)
RECONSTRUCTION_CHECKPOINT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-checkpoint.v1"
)
RECONSTRUCTION_HEARTBEAT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-heartbeat.v1"
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1
MAX_SYMBOLS_PER_SYNCHRONIZATION_UNIT = 64
MAX_ARTIFACT_REFS_PER_CONTRACT = 256
MAX_EVENT_BATCHES_PER_MANIFEST = 4096
MAX_REJECTION_REASONS = 256
MAX_ARTIFACT_METADATA_BYTES = 65_536
MAX_CARRY_STATE_BYTES = 262_144
MAX_HEARTBEAT_BYTES = 65_536
DEFAULT_MAX_CHECKPOINT_BYTES = 524_288

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_INLINE_DATA_KEYS = frozenset(
    {
        "dataframe",
        "events",
        "records",
        "rows",
        "table",
    }
)


class ReconstructionCommitPhase(str, Enum):
    """Two-phase publication and interruption states for one window."""

    PLANNED = "planned"
    RUNNING = "running"
    STAGED = "staged"
    VALIDATED = "validated"
    COMMITTED = "committed"
    CANCELLED = "cancelled"
    FAILED = "failed"

    @classmethod
    def from_value(
        cls,
        value: str | "ReconstructionCommitPhase",
    ) -> "ReconstructionCommitPhase":
        """Return a strict normalized commit phase."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported reconstruction commit phase") from err


class ReconstructionResourceLimitError(ValueError):
    """Resource preflight refusal carrying the rejected estimate."""

    def __init__(
        self,
        estimate: "ReconstructionResourceEstimateV1",
        violations: Sequence[str],
    ) -> None:
        self.estimate = estimate
        self.violations = tuple(violations)
        super().__init__(
            "reconstruction resource preflight failed: "
            + "; ".join(self.violations)
        )


@dataclass(frozen=True, slots=True)
class ReconstructionStoragePolicyV1:
    """Bounded scratch, memory, output, and checkpoint policy."""

    max_events_per_batch: int = 100_000
    max_candidate_amplification: float = 25.0
    max_inflight_batches: int = 8
    max_memory_bytes: int = 2 * 1024**3
    max_scratch_bytes: int = 100 * 1024**3
    max_output_bytes: int = 100 * 1024**3
    max_retained_ensemble_members: int = 9
    checkpoint_every_batches: int = 1
    heartbeat_every_batches: int = 1
    max_checkpoint_bytes: int = DEFAULT_MAX_CHECKPOINT_BYTES
    remove_uncommitted_on_cancel: bool = True
    atomic_promotion_required: bool = True
    advertise_only_committed: bool = True
    policy_id: str = ""
    schema_version: str = RECONSTRUCTION_STORAGE_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_STORAGE_POLICY_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction storage policy schema")
        for name in (
            "max_events_per_batch",
            "max_inflight_batches",
            "max_memory_bytes",
            "max_scratch_bytes",
            "max_output_bytes",
            "max_retained_ensemble_members",
            "checkpoint_every_batches",
            "heartbeat_every_batches",
            "max_checkpoint_bytes",
        ):
            object.__setattr__(
                self,
                name,
                _positive_int(getattr(self, name), name),
            )
        amplification = _finite_float(
            self.max_candidate_amplification,
            "max_candidate_amplification",
        )
        if amplification < 1.0:
            raise ValueError("max_candidate_amplification must be at least one")
        object.__setattr__(
            self,
            "max_candidate_amplification",
            amplification,
        )
        for name in (
            "remove_uncommitted_on_cancel",
            "atomic_promotion_required",
            "advertise_only_committed",
        ):
            object.__setattr__(
                self,
                name,
                _strict_bool(getattr(self, name), name),
            )
        if not self.remove_uncommitted_on_cancel:
            raise ValueError(
                "v1 requires uncommitted scratch cleanup on cancel"
            )
        if not self.atomic_promotion_required:
            raise ValueError("v1 requires atomic promotion")
        if not self.advertise_only_committed:
            raise ValueError("v1 advertises only committed artifacts")
        expected = _stable_id("storage-policy", self.identity_payload())
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("policy_id does not match deterministic identity")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return policy fields used for deterministic identity."""
        return {
            "schema_version": self.schema_version,
            "max_events_per_batch": self.max_events_per_batch,
            "max_candidate_amplification": (self.max_candidate_amplification),
            "max_inflight_batches": self.max_inflight_batches,
            "max_memory_bytes": self.max_memory_bytes,
            "max_scratch_bytes": self.max_scratch_bytes,
            "max_output_bytes": self.max_output_bytes,
            "max_retained_ensemble_members": (
                self.max_retained_ensemble_members
            ),
            "checkpoint_every_batches": self.checkpoint_every_batches,
            "heartbeat_every_batches": self.heartbeat_every_batches,
            "max_checkpoint_bytes": self.max_checkpoint_bytes,
            "remove_uncommitted_on_cancel": (self.remove_uncommitted_on_cancel),
            "atomic_promotion_required": self.atomic_promotion_required,
            "advertise_only_committed": self.advertise_only_committed,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy metadata."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    def preflight(
        self,
        estimate: "ReconstructionResourceEstimateV1",
    ) -> "ReconstructionResourceEstimateV1":
        """Return an accepted estimate or fail early with full evidence."""
        violations: list[str] = []
        allowed_candidates = math.floor(
            estimate.input_event_count * self.max_candidate_amplification
        )
        if estimate.candidate_event_count > allowed_candidates:
            violations.append(
                "candidate_event_count "
                f"{estimate.candidate_event_count} exceeds "
                f"amplification limit {allowed_candidates}"
            )
        limits = (
            (
                "peak_events_per_batch",
                estimate.peak_events_per_batch,
                self.max_events_per_batch,
            ),
            (
                "retained_ensemble_members",
                estimate.retained_ensemble_members,
                self.max_retained_ensemble_members,
            ),
            (
                "inflight_batches",
                estimate.inflight_batches,
                self.max_inflight_batches,
            ),
            (
                "estimated_memory_bytes",
                estimate.estimated_memory_bytes,
                self.max_memory_bytes,
            ),
            (
                "estimated_scratch_bytes",
                estimate.estimated_scratch_bytes,
                self.max_scratch_bytes,
            ),
            (
                "estimated_output_bytes",
                estimate.estimated_output_bytes,
                self.max_output_bytes,
            ),
        )
        for name, actual, limit in limits:
            if actual > limit:
                violations.append(f"{name} {actual} exceeds limit {limit}")
        if violations:
            raise ReconstructionResourceLimitError(estimate, violations)
        return estimate

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
    ) -> "ReconstructionStoragePolicyV1":
        """Restore and verify a version-one storage policy."""
        _require_schema(data, RECONSTRUCTION_STORAGE_POLICY_SCHEMA_VERSION)
        return cls(
            max_events_per_batch=cast(
                int,
                data.get("max_events_per_batch"),
            ),
            max_candidate_amplification=cast(
                float,
                data.get("max_candidate_amplification"),
            ),
            max_inflight_batches=cast(
                int,
                data.get("max_inflight_batches"),
            ),
            max_memory_bytes=cast(int, data.get("max_memory_bytes")),
            max_scratch_bytes=cast(int, data.get("max_scratch_bytes")),
            max_output_bytes=cast(int, data.get("max_output_bytes")),
            max_retained_ensemble_members=cast(
                int,
                data.get("max_retained_ensemble_members"),
            ),
            checkpoint_every_batches=cast(
                int,
                data.get("checkpoint_every_batches"),
            ),
            heartbeat_every_batches=cast(
                int,
                data.get("heartbeat_every_batches"),
            ),
            max_checkpoint_bytes=cast(
                int,
                data.get("max_checkpoint_bytes"),
            ),
            remove_uncommitted_on_cancel=cast(
                bool,
                data.get("remove_uncommitted_on_cancel", True),
            ),
            atomic_promotion_required=cast(
                bool,
                data.get("atomic_promotion_required", True),
            ),
            advertise_only_committed=cast(
                bool,
                data.get("advertise_only_committed", True),
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionStoragePolicyV1":
        """Restore a policy from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionResourceEstimateV1:
    """Pre-execution estimate retained when a run is accepted or refused."""

    input_event_count: int
    candidate_event_count: int
    retained_ensemble_members: int
    inflight_batches: int
    peak_events_per_batch: int
    estimated_memory_bytes: int
    estimated_scratch_bytes: int
    estimated_output_bytes: int
    estimated_batch_count: int
    estimate_id: str = ""
    schema_version: str = RECONSTRUCTION_RESOURCE_ESTIMATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_RESOURCE_ESTIMATE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction resource estimate schema"
            )
        for name in (
            "input_event_count",
            "candidate_event_count",
            "retained_ensemble_members",
            "inflight_batches",
            "peak_events_per_batch",
            "estimated_memory_bytes",
            "estimated_scratch_bytes",
            "estimated_output_bytes",
            "estimated_batch_count",
        ):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        expected = _stable_id("resource-estimate", self.identity_payload())
        supplied = _optional_text(self.estimate_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "estimate_id does not match deterministic identity"
            )
        object.__setattr__(self, "estimate_id", expected)

    @property
    def candidate_amplification(self) -> float:
        """Return the proposed candidate/input ratio."""
        if self.input_event_count == 0:
            return 0.0 if self.candidate_event_count == 0 else math.inf
        return self.candidate_event_count / self.input_event_count

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return estimate fields used for deterministic identity."""
        return {
            "schema_version": self.schema_version,
            "input_event_count": self.input_event_count,
            "candidate_event_count": self.candidate_event_count,
            "retained_ensemble_members": self.retained_ensemble_members,
            "inflight_batches": self.inflight_batches,
            "peak_events_per_batch": self.peak_events_per_batch,
            "estimated_memory_bytes": self.estimated_memory_bytes,
            "estimated_scratch_bytes": self.estimated_scratch_bytes,
            "estimated_output_bytes": self.estimated_output_bytes,
            "estimated_batch_count": self.estimated_batch_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible estimate metadata."""
        amplification: JSONValue = self.candidate_amplification
        if isinstance(amplification, float) and not math.isfinite(
            amplification
        ):
            amplification = "infinite"
        return {
            **self.identity_payload(),
            "estimate_id": self.estimate_id,
            "candidate_amplification": amplification,
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
    ) -> "ReconstructionResourceEstimateV1":
        """Restore and verify a version-one resource estimate."""
        _require_schema(data, RECONSTRUCTION_RESOURCE_ESTIMATE_SCHEMA_VERSION)
        return cls(
            input_event_count=cast(int, data.get("input_event_count")),
            candidate_event_count=cast(
                int,
                data.get("candidate_event_count"),
            ),
            retained_ensemble_members=cast(
                int,
                data.get("retained_ensemble_members"),
            ),
            inflight_batches=cast(int, data.get("inflight_batches")),
            peak_events_per_batch=cast(
                int,
                data.get("peak_events_per_batch"),
            ),
            estimated_memory_bytes=cast(
                int,
                data.get("estimated_memory_bytes"),
            ),
            estimated_scratch_bytes=cast(
                int,
                data.get("estimated_scratch_bytes"),
            ),
            estimated_output_bytes=cast(
                int,
                data.get("estimated_output_bytes"),
            ),
            estimated_batch_count=cast(
                int,
                data.get("estimated_batch_count"),
            ),
            estimate_id=str(data.get("estimate_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionResourceEstimateV1":
        """Restore a resource estimate from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionRunV1:
    """Semantic run identity separated from execution/storage tuning."""

    symbols: tuple[str, ...]
    source_version_ids: tuple[str, ...]
    configuration_ids: tuple[str, ...]
    ensemble_member_ids: tuple[str, ...]
    base_seed: int
    storage_policy: ReconstructionStoragePolicyV1 = field(
        default_factory=ReconstructionStoragePolicyV1
    )
    run_id: str = ""
    schema_version: str = RECONSTRUCTION_RUN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_RUN_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction run schema")
        object.__setattr__(self, "symbols", _normalized_symbols(self.symbols))
        object.__setattr__(
            self,
            "source_version_ids",
            _normalized_id_tuple(self.source_version_ids),
        )
        object.__setattr__(
            self,
            "configuration_ids",
            _normalized_id_tuple(self.configuration_ids),
        )
        object.__setattr__(
            self,
            "ensemble_member_ids",
            _normalized_id_tuple(self.ensemble_member_ids),
        )
        if len(self.symbols) > MAX_SYMBOLS_PER_SYNCHRONIZATION_UNIT:
            raise ValueError("run symbol count exceeds synchronization limit")
        if not self.source_version_ids:
            raise ValueError("run requires source_version_ids")
        if not self.configuration_ids:
            raise ValueError("run requires configuration_ids")
        if not self.ensemble_member_ids:
            raise ValueError("run requires ensemble_member_ids")
        seed = _nonnegative_int(self.base_seed, "base_seed")
        if seed > UINT64_MAX:
            raise ValueError("base_seed is outside unsigned 64-bit range")
        object.__setattr__(self, "base_seed", seed)
        if not isinstance(self.storage_policy, ReconstructionStoragePolicyV1):
            raise ValueError("storage_policy must be a v1 policy")
        expected = _stable_id("reconstruction-run", self.identity_payload())
        supplied = _optional_text(self.run_id)
        if supplied is not None and supplied != expected:
            raise ValueError("run_id does not match deterministic identity")
        object.__setattr__(self, "run_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic inputs; execution policy is intentionally absent."""
        return {
            "schema_version": self.schema_version,
            "symbols": list(self.symbols),
            "source_version_ids": list(self.source_version_ids),
            "configuration_ids": list(self.configuration_ids),
            "ensemble_member_ids": list(self.ensemble_member_ids),
            "base_seed": self.base_seed,
        }

    def seed_for(self, ensemble_member_id: str, semantic_key: str) -> int:
        """Derive a partition-independent seed from stable semantic lineage."""
        member_id = _required_text(ensemble_member_id)
        if member_id not in self.ensemble_member_ids:
            raise ValueError("ensemble member is not part of this run")
        key = _required_text(semantic_key)
        encoded = canonical_contract_json(
            {
                "run_id": self.run_id,
                "ensemble_member_id": member_id,
                "semantic_key": key,
                "base_seed": self.base_seed,
            }
        ).encode("utf-8")
        return int.from_bytes(hashlib.sha256(encoded).digest()[:8], "big")

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible run metadata."""
        return {
            **self.identity_payload(),
            "run_id": self.run_id,
            "storage_policy": self.storage_policy.to_dict(),
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReconstructionRunV1":
        """Restore and verify a version-one run contract."""
        _require_schema(data, RECONSTRUCTION_RUN_SCHEMA_VERSION)
        return cls(
            symbols=_string_tuple(data.get("symbols")),
            source_version_ids=_string_tuple(data.get("source_version_ids")),
            configuration_ids=_string_tuple(data.get("configuration_ids")),
            ensemble_member_ids=_string_tuple(data.get("ensemble_member_ids")),
            base_seed=cast(int, data.get("base_seed")),
            storage_policy=ReconstructionStoragePolicyV1.from_dict(
                _mapping(data.get("storage_policy"))
            ),
            run_id=str(data.get("run_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionRunV1":
        """Restore a run from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionWindowV1:
    """One synchronized, half-open generation window and its input halo."""

    run_id: str
    ensemble_member_id: str
    symbols: tuple[str, ...]
    core_start_ns: int
    core_end_ns: int
    left_halo_ns: int = 0
    right_lookahead_ns: int = 0
    window_id: str = ""
    synchronization_unit_id: str = ""
    schema_version: str = RECONSTRUCTION_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_WINDOW_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction window schema")
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "ensemble_member_id",
            _required_text(self.ensemble_member_id),
        )
        object.__setattr__(self, "symbols", _normalized_symbols(self.symbols))
        if len(self.symbols) > MAX_SYMBOLS_PER_SYNCHRONIZATION_UNIT:
            raise ValueError(
                "window symbol count exceeds synchronization limit"
            )
        start = _bounded_int64(self.core_start_ns, "core_start_ns")
        end = _bounded_int64(self.core_end_ns, "core_end_ns")
        if end <= start:
            raise ValueError("core_end_ns must be greater than core_start_ns")
        object.__setattr__(self, "core_start_ns", start)
        object.__setattr__(self, "core_end_ns", end)
        for name in ("left_halo_ns", "right_lookahead_ns"):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        _bounded_int64(self.input_start_ns, "input_start_ns")
        _bounded_int64(self.input_end_ns, "input_end_ns")
        expected_sync = _stable_id(
            "synchronization-unit",
            self.identity_payload(),
        )
        supplied_sync = _optional_text(self.synchronization_unit_id)
        if supplied_sync is not None and supplied_sync != expected_sync:
            raise ValueError(
                "synchronization_unit_id does not match deterministic identity"
            )
        object.__setattr__(self, "synchronization_unit_id", expected_sync)
        expected_window = _stable_id(
            "reconstruction-window", self.identity_payload()
        )
        supplied_window = _optional_text(self.window_id)
        if supplied_window is not None and supplied_window != expected_window:
            raise ValueError("window_id does not match deterministic identity")
        object.__setattr__(self, "window_id", expected_window)

    @property
    def input_start_ns(self) -> int:
        """Return the inclusive read start including left carry context."""
        return self.core_start_ns - self.left_halo_ns

    @property
    def input_end_ns(self) -> int:
        """Return the exclusive read end including declared future context."""
        return self.core_end_ns + self.right_lookahead_ns

    def owns_event_time(self, event_time_ns: int) -> bool:
        """Return whether this window alone may generate at the timestamp."""
        value = _bounded_int64(event_time_ns, "event_time_ns")
        return self.core_start_ns <= value < self.core_end_ns

    def reads_event_time(self, event_time_ns: int) -> bool:
        """Return whether the timestamp lies in the bounded input interval."""
        value = _bounded_int64(event_time_ns, "event_time_ns")
        return self.input_start_ns <= value < self.input_end_ns

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic synchronization-unit identity fields."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbols": list(self.symbols),
            "core_start_ns": self.core_start_ns,
            "core_end_ns": self.core_end_ns,
            "left_halo_ns": self.left_halo_ns,
            "right_lookahead_ns": self.right_lookahead_ns,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded JSON-compatible window metadata."""
        return {
            **self.identity_payload(),
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "input_start_ns": self.input_start_ns,
            "input_end_ns": self.input_end_ns,
            "ownership_interval": "[core_start_ns,core_end_ns)",
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReconstructionWindowV1":
        """Restore and verify a version-one window contract."""
        _require_schema(data, RECONSTRUCTION_WINDOW_SCHEMA_VERSION)
        return cls(
            run_id=str(data.get("run_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbols=_string_tuple(data.get("symbols")),
            core_start_ns=cast(int, data.get("core_start_ns")),
            core_end_ns=cast(int, data.get("core_end_ns")),
            left_halo_ns=cast(int, data.get("left_halo_ns", 0)),
            right_lookahead_ns=cast(
                int,
                data.get("right_lookahead_ns", 0),
            ),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionWindowV1":
        """Restore a window from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


def plan_reconstruction_windows(
    run: ReconstructionRunV1,
    *,
    ensemble_member_id: str,
    start_ns: int,
    end_ns: int,
    window_size_ns: int,
    left_halo_ns: int = 0,
    right_lookahead_ns: int = 0,
) -> tuple[ReconstructionWindowV1, ...]:
    """Plan contiguous synchronized windows without worker-count inputs."""
    member_id = _required_text(ensemble_member_id)
    if member_id not in run.ensemble_member_ids:
        raise ValueError("ensemble member is not part of this run")
    start = _bounded_int64(start_ns, "start_ns")
    end = _bounded_int64(end_ns, "end_ns")
    if end <= start:
        raise ValueError("end_ns must be greater than start_ns")
    size = _positive_int(window_size_ns, "window_size_ns")
    windows: list[ReconstructionWindowV1] = []
    current = start
    while current < end:
        next_end = min(end, current + size)
        windows.append(
            ReconstructionWindowV1(
                run_id=run.run_id,
                ensemble_member_id=member_id,
                symbols=run.symbols,
                core_start_ns=current,
                core_end_ns=next_end,
                left_halo_ns=left_halo_ns,
                right_lookahead_ns=right_lookahead_ns,
            )
        )
        current = next_end
    return validate_reconstruction_window_plan(
        windows,
        expected_start_ns=start,
        expected_end_ns=end,
    )


def validate_reconstruction_window_plan(
    windows: Sequence[ReconstructionWindowV1],
    *,
    expected_start_ns: int | None = None,
    expected_end_ns: int | None = None,
) -> tuple[ReconstructionWindowV1, ...]:
    """Validate one contiguous plan with exclusive generation ownership."""
    ordered = tuple(sorted(windows, key=lambda item: item.core_start_ns))
    if not ordered:
        raise ValueError("window plan cannot be empty")
    first = ordered[0]
    for previous, current in zip(ordered, ordered[1:]):
        if (
            current.run_id != first.run_id
            or current.ensemble_member_id != first.ensemble_member_id
            or current.symbols != first.symbols
        ):
            raise ValueError("window plan synchronization scope drifted")
        if previous.core_end_ns != current.core_start_ns:
            raise ValueError(
                "window plan must be contiguous and non-overlapping"
            )
    if (
        expected_start_ns is not None
        and first.core_start_ns != expected_start_ns
    ):
        raise ValueError("window plan does not start at expected boundary")
    if (
        expected_end_ns is not None
        and ordered[-1].core_end_ns != expected_end_ns
    ):
        raise ValueError("window plan does not end at expected boundary")
    return ordered


@dataclass(frozen=True, slots=True)
class EventBatchV1:
    """Bounded metadata for an event batch stored outside workflow history."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    symbol: str
    batch_ordinal: int
    event_count: int
    ownership_start_ns: int
    ownership_end_ns: int
    first_event_time_ns: int
    last_event_time_ns: int
    content_sha256: str
    artifact: ArtifactRef
    batch_id: str = ""
    schema_version: str = EVENT_BATCH_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != EVENT_BATCH_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction event batch schema")
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self,
            "batch_ordinal",
            _nonnegative_int(self.batch_ordinal, "batch_ordinal"),
        )
        object.__setattr__(
            self,
            "event_count",
            _positive_int(self.event_count, "event_count"),
        )
        ownership_start = _bounded_int64(
            self.ownership_start_ns,
            "ownership_start_ns",
        )
        ownership_end = _bounded_int64(
            self.ownership_end_ns,
            "ownership_end_ns",
        )
        if ownership_end <= ownership_start:
            raise ValueError(
                "ownership_end_ns must be greater than ownership_start_ns"
            )
        object.__setattr__(self, "ownership_start_ns", ownership_start)
        object.__setattr__(self, "ownership_end_ns", ownership_end)
        first_time = _bounded_int64(
            self.first_event_time_ns,
            "first_event_time_ns",
        )
        last_time = _bounded_int64(
            self.last_event_time_ns,
            "last_event_time_ns",
        )
        if last_time < first_time:
            raise ValueError("last_event_time_ns precedes first_event_time_ns")
        if first_time < ownership_start or last_time >= ownership_end:
            raise ValueError(
                "event batch lies outside half-open ownership interval"
            )
        object.__setattr__(self, "first_event_time_ns", first_time)
        object.__setattr__(self, "last_event_time_ns", last_time)
        object.__setattr__(
            self,
            "content_sha256",
            _required_sha256(self.content_sha256, "content_sha256"),
        )
        object.__setattr__(
            self, "artifact", _validated_artifact_ref(self.artifact)
        )
        expected = _stable_id("event-batch", self.identity_payload())
        supplied = _optional_text(self.batch_id)
        if supplied is not None and supplied != expected:
            raise ValueError("batch_id does not match deterministic identity")
        object.__setattr__(self, "batch_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return retry/worker-independent batch identity fields."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol": self.symbol,
            "batch_ordinal": self.batch_ordinal,
            "event_count": self.event_count,
            "ownership_start_ns": self.ownership_start_ns,
            "ownership_end_ns": self.ownership_end_ns,
            "first_event_time_ns": self.first_event_time_ns,
            "last_event_time_ns": self.last_event_time_ns,
            "content_sha256": self.content_sha256,
            "artifact": _artifact_content_identity_payload(self.artifact),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded metadata and one artifact reference, never rows."""
        return {
            **self.identity_payload(),
            "batch_id": self.batch_id,
            "artifact": self.artifact.to_dict(),
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "EventBatchV1":
        """Restore and verify a version-one event batch."""
        _require_schema(data, EVENT_BATCH_SCHEMA_VERSION)
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbol=str(data.get("symbol", "")),
            batch_ordinal=cast(int, data.get("batch_ordinal")),
            event_count=cast(int, data.get("event_count")),
            ownership_start_ns=cast(int, data.get("ownership_start_ns")),
            ownership_end_ns=cast(int, data.get("ownership_end_ns")),
            first_event_time_ns=cast(int, data.get("first_event_time_ns")),
            last_event_time_ns=cast(int, data.get("last_event_time_ns")),
            content_sha256=str(data.get("content_sha256", "")),
            artifact=ArtifactRef.from_dict(_mapping(data.get("artifact"))),
            batch_id=str(data.get("batch_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "EventBatchV1":
        """Restore a batch from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class CarryStateV1:
    """Bounded cross-window watermarks and references to larger state."""

    run_id: str
    ensemble_member_id: str
    symbol_watermarks_ns: dict[str, int]
    last_event_ids: dict[str, str] = field(default_factory=dict)
    state_artifacts: tuple[ArtifactRef, ...] = ()
    carry_id: str = ""
    schema_version: str = CARRY_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CARRY_STATE_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction carry-state schema")
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "ensemble_member_id",
            _required_text(self.ensemble_member_id),
        )
        watermarks = {
            _normalized_symbol(symbol): _bounded_int64(
                watermark,
                f"symbol_watermarks_ns.{symbol}",
            )
            for symbol, watermark in self.symbol_watermarks_ns.items()
        }
        if not watermarks:
            raise ValueError("carry state requires symbol watermarks")
        if len(watermarks) > MAX_SYMBOLS_PER_SYNCHRONIZATION_UNIT:
            raise ValueError("carry-state symbol count exceeds limit")
        object.__setattr__(
            self,
            "symbol_watermarks_ns",
            dict(sorted(watermarks.items())),
        )
        last_ids = {
            _normalized_symbol(symbol): _required_text(event_id)
            for symbol, event_id in self.last_event_ids.items()
        }
        if not set(last_ids).issubset(watermarks):
            raise ValueError("last_event_ids contains an unknown symbol")
        object.__setattr__(
            self,
            "last_event_ids",
            dict(sorted(last_ids.items())),
        )
        object.__setattr__(
            self,
            "state_artifacts",
            _validated_artifact_refs(self.state_artifacts),
        )
        expected = _stable_id("carry-state", self.identity_payload())
        supplied = _optional_text(self.carry_id)
        if supplied is not None and supplied != expected:
            raise ValueError("carry_id does not match deterministic identity")
        object.__setattr__(self, "carry_id", expected)
        _ensure_payload_size(
            self.to_dict(),
            MAX_CARRY_STATE_BYTES,
            "carry state",
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic carry-state identity fields."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbol_watermarks_ns": dict(self.symbol_watermarks_ns),
            "last_event_ids": dict(self.last_event_ids),
            "state_artifacts": [
                _artifact_content_identity_payload(ref)
                for ref in self.state_artifacts
            ],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded scalar state and artifact references."""
        return {
            **self.identity_payload(),
            "carry_id": self.carry_id,
            "state_artifacts": [
                artifact.to_dict() for artifact in self.state_artifacts
            ],
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarryStateV1":
        """Restore and verify a version-one carry state."""
        _require_schema(data, CARRY_STATE_SCHEMA_VERSION)
        return cls(
            run_id=str(data.get("run_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbol_watermarks_ns={
                str(key): cast(int, value)
                for key, value in _mapping(
                    data.get("symbol_watermarks_ns")
                ).items()
            },
            last_event_ids={
                str(key): str(value)
                for key, value in _mapping(data.get("last_event_ids")).items()
            },
            state_artifacts=tuple(
                ArtifactRef.from_dict(_mapping(item))
                for item in _sequence(data.get("state_artifacts"))
            ),
            carry_id=str(data.get("carry_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "CarryStateV1":
        """Restore carry state from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class RejectionSummaryV1:
    """Bounded aggregate rejection evidence without candidate rows."""

    run_id: str
    window_id: str
    candidate_count: int
    accepted_count: int
    rejected_count: int
    reason_counts: dict[str, int] = field(default_factory=dict)
    summary_id: str = ""
    schema_version: str = REJECTION_SUMMARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != REJECTION_SUMMARY_SCHEMA_VERSION:
            raise ValueError("unsupported rejection-summary schema")
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(self, "window_id", _required_text(self.window_id))
        for name in ("candidate_count", "accepted_count", "rejected_count"):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        if self.accepted_count + self.rejected_count != self.candidate_count:
            raise ValueError(
                "accepted plus rejected must equal candidate count"
            )
        reasons = {
            _required_text(reason): _positive_int(count, f"reason.{reason}")
            for reason, count in self.reason_counts.items()
        }
        if len(reasons) > MAX_REJECTION_REASONS:
            raise ValueError("rejection reason count exceeds bounded limit")
        if sum(reasons.values()) != self.rejected_count:
            raise ValueError("reason counts must reconcile with rejected_count")
        object.__setattr__(self, "reason_counts", dict(sorted(reasons.items())))
        expected = _stable_id("rejection-summary", self.identity_payload())
        supplied = _optional_text(self.summary_id)
        if supplied is not None and supplied != expected:
            raise ValueError("summary_id does not match deterministic identity")
        object.__setattr__(self, "summary_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic rejection-summary identity fields."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "candidate_count": self.candidate_count,
            "accepted_count": self.accepted_count,
            "rejected_count": self.rejected_count,
            "reason_counts": dict(self.reason_counts),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded rejection counts."""
        return {**self.identity_payload(), "summary_id": self.summary_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RejectionSummaryV1":
        """Restore and verify a version-one rejection summary."""
        _require_schema(data, REJECTION_SUMMARY_SCHEMA_VERSION)
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            candidate_count=cast(int, data.get("candidate_count")),
            accepted_count=cast(int, data.get("accepted_count")),
            rejected_count=cast(int, data.get("rejected_count")),
            reason_counts={
                str(key): cast(int, value)
                for key, value in _mapping(data.get("reason_counts")).items()
            },
            summary_id=str(data.get("summary_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "RejectionSummaryV1":
        """Restore a summary from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class PartitionManifestV1:
    """Compact manifest for one all-symbol synchronization unit."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    symbols: tuple[str, ...]
    symbol_event_counts: dict[str, int]
    event_batches: tuple[EventBatchV1, ...]
    rejection_summary_ref: ArtifactRef
    carry_state_ref: ArtifactRef
    manifest_id: str = ""
    schema_version: str = PARTITION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PARTITION_MANIFEST_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction partition manifest")
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        symbols = _normalized_symbols(self.symbols)
        object.__setattr__(self, "symbols", symbols)
        counts = {
            _normalized_symbol(symbol): _nonnegative_int(
                count,
                f"symbol_event_counts.{symbol}",
            )
            for symbol, count in self.symbol_event_counts.items()
        }
        if set(counts) != set(symbols):
            raise ValueError(
                "symbol event counts must cover synchronization unit"
            )
        object.__setattr__(
            self,
            "symbol_event_counts",
            dict(sorted(counts.items())),
        )
        batches = tuple(
            sorted(
                tuple(self.event_batches),
                key=lambda item: (
                    item.symbol,
                    item.batch_ordinal,
                    item.batch_id,
                ),
            )
        )
        if len(batches) > MAX_EVENT_BATCHES_PER_MANIFEST:
            raise ValueError("partition manifest batch count exceeds limit")
        if len({batch.batch_id for batch in batches}) != len(batches):
            raise ValueError("partition manifest contains duplicate batch IDs")
        calculated_counts = dict.fromkeys(symbols, 0)
        for batch in batches:
            if (
                batch.run_id != self.run_id
                or batch.window_id != self.window_id
                or batch.synchronization_unit_id != self.synchronization_unit_id
                or batch.ensemble_member_id != self.ensemble_member_id
            ):
                raise ValueError("event batch scope does not match manifest")
            if batch.symbol not in calculated_counts:
                raise ValueError("event batch symbol is outside manifest")
            calculated_counts[batch.symbol] += batch.event_count
        if calculated_counts != counts:
            raise ValueError("event batch counts do not reconcile by symbol")
        object.__setattr__(self, "event_batches", batches)
        object.__setattr__(
            self,
            "rejection_summary_ref",
            _validated_artifact_ref(self.rejection_summary_ref),
        )
        object.__setattr__(
            self,
            "carry_state_ref",
            _validated_artifact_ref(self.carry_state_ref),
        )
        expected = _stable_id("partition-manifest", self.identity_payload())
        supplied = _optional_text(self.manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "manifest_id does not match deterministic identity"
            )
        object.__setattr__(self, "manifest_id", expected)

    @property
    def event_count(self) -> int:
        """Return total events across the synchronized symbol unit."""
        return sum(self.symbol_event_counts.values())

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic manifest identity fields."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "symbols": list(self.symbols),
            "symbol_event_counts": dict(self.symbol_event_counts),
            "event_batches": [
                batch.identity_payload() for batch in self.event_batches
            ],
            "rejection_summary_ref": _artifact_content_identity_payload(
                self.rejection_summary_ref
            ),
            "carry_state_ref": _artifact_content_identity_payload(
                self.carry_state_ref
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact all-symbol manifest metadata."""
        return {
            **self.identity_payload(),
            "manifest_id": self.manifest_id,
            "event_count": self.event_count,
            "event_batches": [batch.to_dict() for batch in self.event_batches],
            "rejection_summary_ref": self.rejection_summary_ref.to_dict(),
            "carry_state_ref": self.carry_state_ref.to_dict(),
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PartitionManifestV1":
        """Restore and verify a version-one partition manifest."""
        _require_schema(data, PARTITION_MANIFEST_SCHEMA_VERSION)
        manifest = cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            symbols=_string_tuple(data.get("symbols")),
            symbol_event_counts={
                str(key): cast(int, value)
                for key, value in _mapping(
                    data.get("symbol_event_counts")
                ).items()
            },
            event_batches=tuple(
                EventBatchV1.from_dict(_mapping(item))
                for item in _sequence(data.get("event_batches"))
            ),
            rejection_summary_ref=ArtifactRef.from_dict(
                _mapping(data.get("rejection_summary_ref"))
            ),
            carry_state_ref=ArtifactRef.from_dict(
                _mapping(data.get("carry_state_ref"))
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        derived_count = data.get("event_count")
        if derived_count is not None and derived_count != manifest.event_count:
            raise ValueError("manifest event_count does not reconcile")
        return manifest

    @classmethod
    def from_json(cls, text: str) -> "PartitionManifestV1":
        """Restore a manifest from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


_ALLOWED_PHASE_TRANSITIONS: dict[
    ReconstructionCommitPhase,
    frozenset[ReconstructionCommitPhase],
] = {
    ReconstructionCommitPhase.PLANNED: frozenset(
        {
            ReconstructionCommitPhase.RUNNING,
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }
    ),
    ReconstructionCommitPhase.RUNNING: frozenset(
        {
            ReconstructionCommitPhase.RUNNING,
            ReconstructionCommitPhase.STAGED,
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }
    ),
    ReconstructionCommitPhase.STAGED: frozenset(
        {
            ReconstructionCommitPhase.VALIDATED,
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }
    ),
    ReconstructionCommitPhase.VALIDATED: frozenset(
        {
            ReconstructionCommitPhase.COMMITTED,
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }
    ),
    ReconstructionCommitPhase.COMMITTED: frozenset(
        {ReconstructionCommitPhase.COMMITTED}
    ),
    ReconstructionCommitPhase.CANCELLED: frozenset(
        {ReconstructionCommitPhase.RUNNING}
    ),
    ReconstructionCommitPhase.FAILED: frozenset(
        {ReconstructionCommitPhase.RUNNING}
    ),
}


@dataclass(frozen=True, slots=True)
class ReconstructionCheckpointV1:
    """Bounded, chained recovery state for one synchronization unit."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    revision: int
    phase: ReconstructionCommitPhase
    input_watermark_ns: int | None = None
    output_watermark_ns: int | None = None
    completed_batch_ids: tuple[str, ...] = ()
    carry_state_ref: ArtifactRef | None = None
    rejection_summary_ref: ArtifactRef | None = None
    staged_manifest_ref: ArtifactRef | None = None
    committed_manifest_ref: ArtifactRef | None = None
    parent_checkpoint_id: str | None = None
    interruption_reason: str = ""
    checkpoint_id: str = ""
    schema_version: str = RECONSTRUCTION_CHECKPOINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction checkpoint schema")
        for name in ("run_id", "window_id", "synchronization_unit_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "revision",
            _nonnegative_int(self.revision, "revision"),
        )
        object.__setattr__(
            self,
            "phase",
            ReconstructionCommitPhase.from_value(self.phase),
        )
        for name in ("input_watermark_ns", "output_watermark_ns"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(
                    self,
                    name,
                    _bounded_int64(value, name),
                )
        completed_values = tuple(
            _required_text(value) for value in self.completed_batch_ids
        )
        if len(set(completed_values)) != len(completed_values):
            raise ValueError("checkpoint completed_batch_ids must be unique")
        completed = tuple(sorted(completed_values))
        object.__setattr__(self, "completed_batch_ids", completed)
        for name in (
            "carry_state_ref",
            "rejection_summary_ref",
            "staged_manifest_ref",
            "committed_manifest_ref",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _validated_artifact_ref(value))
        object.__setattr__(
            self,
            "parent_checkpoint_id",
            _optional_text(self.parent_checkpoint_id),
        )
        object.__setattr__(
            self,
            "interruption_reason",
            str(self.interruption_reason or "").strip(),
        )
        self._validate_phase_state()
        expected = _stable_id("checkpoint", self.identity_payload())
        supplied = _optional_text(self.checkpoint_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "checkpoint_id does not match deterministic identity"
            )
        object.__setattr__(self, "checkpoint_id", expected)

    @classmethod
    def planned(
        cls,
        window: ReconstructionWindowV1,
    ) -> "ReconstructionCheckpointV1":
        """Create the initial checkpoint for a planned window."""
        return cls(
            run_id=window.run_id,
            window_id=window.window_id,
            synchronization_unit_id=window.synchronization_unit_id,
            revision=0,
            phase=ReconstructionCommitPhase.PLANNED,
        )

    def _validate_phase_state(self) -> None:
        if self.phase is ReconstructionCommitPhase.PLANNED:
            if self.revision != 0 or self.completed_batch_ids:
                raise ValueError(
                    "planned checkpoint must be an empty revision zero"
                )
            if any(
                ref is not None
                for ref in (
                    self.carry_state_ref,
                    self.rejection_summary_ref,
                    self.staged_manifest_ref,
                    self.committed_manifest_ref,
                )
            ):
                raise ValueError("planned checkpoint cannot reference outputs")
        if (
            self.phase
            in {
                ReconstructionCommitPhase.STAGED,
                ReconstructionCommitPhase.VALIDATED,
            }
            and self.staged_manifest_ref is None
        ):
            raise ValueError(
                "staged/validated checkpoint requires staged manifest"
            )
        if self.phase is ReconstructionCommitPhase.COMMITTED:
            if self.committed_manifest_ref is None:
                raise ValueError(
                    "committed checkpoint requires committed manifest"
                )
            if self.staged_manifest_ref is not None:
                raise ValueError(
                    "committed checkpoint cannot advertise staged path"
                )
        elif self.committed_manifest_ref is not None:
            raise ValueError(
                "only committed checkpoint may reference final manifest"
            )
        interrupted = self.phase in {
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }
        if interrupted != bool(self.interruption_reason):
            raise ValueError(
                "interrupted checkpoints require a reason and active ones reject it"
            )

    @property
    def advertised_manifest_ref(self) -> ArtifactRef | None:
        """Return a discoverable manifest only after atomic promotion."""
        if self.phase is ReconstructionCommitPhase.COMMITTED:
            return self.committed_manifest_ref
        return None

    def pending_batches(
        self,
        batches: Sequence[EventBatchV1],
    ) -> tuple[EventBatchV1, ...]:
        """Deduplicate retry delivery and return only unfinished batch work."""
        unique: dict[str, EventBatchV1] = {}
        for batch in batches:
            self._validate_batch_scope(batch)
            existing = unique.get(batch.batch_id)
            if (
                existing is not None
                and existing.identity_payload() != batch.identity_payload()
            ):
                raise ValueError(
                    "batch ID collision contains different metadata"
                )
            if existing is None:
                unique[batch.batch_id] = batch
        completed = set(self.completed_batch_ids)
        return tuple(
            batch
            for batch in sorted(
                unique.values(),
                key=lambda item: (
                    item.symbol,
                    item.batch_ordinal,
                    item.batch_id,
                ),
            )
            if batch.batch_id not in completed
        )

    def transition(
        self,
        phase: ReconstructionCommitPhase | str,
        *,
        expected_checkpoint_id: str,
        completed_batches: Sequence[EventBatchV1] = (),
        input_watermark_ns: int | None = None,
        output_watermark_ns: int | None = None,
        carry_state_ref: ArtifactRef | None = None,
        rejection_summary_ref: ArtifactRef | None = None,
        staged_manifest_ref: ArtifactRef | None = None,
        committed_manifest_ref: ArtifactRef | None = None,
        interruption_reason: str = "",
    ) -> "ReconstructionCheckpointV1":
        """Advance with optimistic concurrency and idempotent final commit."""
        if _required_text(expected_checkpoint_id) != self.checkpoint_id:
            raise ValueError("stale checkpoint transition rejected")
        target = ReconstructionCommitPhase.from_value(phase)

        if self.phase is ReconstructionCommitPhase.COMMITTED:
            if target is not ReconstructionCommitPhase.COMMITTED:
                raise ValueError("committed checkpoint is terminal")
            requested_ref = (
                _validated_artifact_ref(committed_manifest_ref)
                if committed_manifest_ref is not None
                else self.committed_manifest_ref
            )
            if requested_ref != self.committed_manifest_ref:
                raise ValueError("idempotent commit cannot change manifest")
            if completed_batches:
                pending = self.pending_batches(completed_batches)
                if pending:
                    raise ValueError("committed checkpoint rejects new batches")
            return self

        if target not in _ALLOWED_PHASE_TRANSITIONS[self.phase]:
            raise ValueError(
                f"invalid checkpoint transition {self.phase.value}->{target.value}"
            )
        new_batch_ids = set(self.completed_batch_ids)
        for batch in completed_batches:
            self._validate_batch_scope(batch)
            new_batch_ids.add(batch.batch_id)

        input_watermark = _monotonic_optional_int64(
            self.input_watermark_ns,
            input_watermark_ns,
            "input_watermark_ns",
        )
        output_watermark = _monotonic_optional_int64(
            self.output_watermark_ns,
            output_watermark_ns,
            "output_watermark_ns",
        )
        next_carry = (
            _validated_artifact_ref(carry_state_ref)
            if carry_state_ref is not None
            else self.carry_state_ref
        )
        next_rejections = (
            _validated_artifact_ref(rejection_summary_ref)
            if rejection_summary_ref is not None
            else self.rejection_summary_ref
        )
        next_staged = self.staged_manifest_ref
        next_committed: ArtifactRef | None = None
        if target is ReconstructionCommitPhase.RUNNING and self.phase in {
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }:
            next_staged = None
        if target is ReconstructionCommitPhase.STAGED:
            if staged_manifest_ref is None:
                raise ValueError(
                    "staging transition requires temporary manifest"
                )
            next_staged = _validated_artifact_ref(staged_manifest_ref)
        elif target is ReconstructionCommitPhase.VALIDATED:
            if staged_manifest_ref is not None:
                candidate = _validated_artifact_ref(staged_manifest_ref)
                if next_staged is not None and candidate != next_staged:
                    raise ValueError(
                        "validation cannot replace staged manifest"
                    )
                next_staged = candidate
        elif target is ReconstructionCommitPhase.COMMITTED:
            if committed_manifest_ref is None:
                raise ValueError("commit transition requires promoted manifest")
            next_committed = _validated_artifact_ref(committed_manifest_ref)
            if next_staged is None:
                raise ValueError("commit transition requires validated staging")
            if (
                next_committed.sha256 != next_staged.sha256
                or next_committed.size_bytes != next_staged.size_bytes
            ):
                raise ValueError(
                    "promoted manifest bytes do not match validated staging"
                )
            next_staged = None
        elif committed_manifest_ref is not None:
            raise ValueError("final manifest is accepted only during commit")

        reason = str(interruption_reason or "").strip()
        interrupted = target in {
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }
        if interrupted and not reason:
            raise ValueError("cancelled/failed transition requires reason")
        if not interrupted and reason:
            raise ValueError("active transition rejects interruption reason")

        return ReconstructionCheckpointV1(
            run_id=self.run_id,
            window_id=self.window_id,
            synchronization_unit_id=self.synchronization_unit_id,
            revision=self.revision + 1,
            phase=target,
            input_watermark_ns=input_watermark,
            output_watermark_ns=output_watermark,
            completed_batch_ids=tuple(sorted(new_batch_ids)),
            carry_state_ref=next_carry,
            rejection_summary_ref=next_rejections,
            staged_manifest_ref=next_staged,
            committed_manifest_ref=next_committed,
            parent_checkpoint_id=self.checkpoint_id,
            interruption_reason=reason,
        )

    def _validate_batch_scope(self, batch: EventBatchV1) -> None:
        if (
            batch.run_id != self.run_id
            or batch.window_id != self.window_id
            or batch.synchronization_unit_id != self.synchronization_unit_id
        ):
            raise ValueError("event batch scope does not match checkpoint")

    def assert_within(
        self,
        policy: ReconstructionStoragePolicyV1,
    ) -> "ReconstructionCheckpointV1":
        """Fail before persistence when checkpoint metadata exceeds policy."""
        _ensure_payload_size(
            self.to_dict(),
            policy.max_checkpoint_bytes,
            "checkpoint",
        )
        return self

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic chained checkpoint identity fields."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "revision": self.revision,
            "phase": self.phase.value,
            "input_watermark_ns": self.input_watermark_ns,
            "output_watermark_ns": self.output_watermark_ns,
            "completed_batch_ids": list(self.completed_batch_ids),
            "carry_state_ref": _optional_artifact_identity_payload(
                self.carry_state_ref
            ),
            "rejection_summary_ref": _optional_artifact_identity_payload(
                self.rejection_summary_ref
            ),
            "staged_manifest_ref": _optional_artifact_identity_payload(
                self.staged_manifest_ref
            ),
            "committed_manifest_ref": _optional_artifact_identity_payload(
                self.committed_manifest_ref
            ),
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "interruption_reason": self.interruption_reason,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded checkpoint state and artifact references."""
        return {
            **self.identity_payload(),
            "checkpoint_id": self.checkpoint_id,
            "carry_state_ref": _optional_artifact_dict(self.carry_state_ref),
            "rejection_summary_ref": _optional_artifact_dict(
                self.rejection_summary_ref
            ),
            "staged_manifest_ref": _optional_artifact_dict(
                self.staged_manifest_ref
            ),
            "committed_manifest_ref": _optional_artifact_dict(
                self.committed_manifest_ref
            ),
            "advertised_manifest_ref": _optional_artifact_dict(
                self.advertised_manifest_ref
            ),
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReconstructionCheckpointV1":
        """Restore and verify a version-one checkpoint."""
        _require_schema(data, RECONSTRUCTION_CHECKPOINT_SCHEMA_VERSION)
        checkpoint = cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            revision=cast(int, data.get("revision")),
            phase=ReconstructionCommitPhase.from_value(
                str(data.get("phase", ""))
            ),
            input_watermark_ns=cast(
                int | None,
                data.get("input_watermark_ns"),
            ),
            output_watermark_ns=cast(
                int | None,
                data.get("output_watermark_ns"),
            ),
            completed_batch_ids=_string_tuple(data.get("completed_batch_ids")),
            carry_state_ref=_optional_artifact_from_value(
                data.get("carry_state_ref")
            ),
            rejection_summary_ref=_optional_artifact_from_value(
                data.get("rejection_summary_ref")
            ),
            staged_manifest_ref=_optional_artifact_from_value(
                data.get("staged_manifest_ref")
            ),
            committed_manifest_ref=_optional_artifact_from_value(
                data.get("committed_manifest_ref")
            ),
            parent_checkpoint_id=_optional_text(
                data.get("parent_checkpoint_id")
            ),
            interruption_reason=str(data.get("interruption_reason", "")),
            checkpoint_id=str(data.get("checkpoint_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        advertised = _optional_artifact_from_value(
            data.get("advertised_manifest_ref")
        )
        if advertised != checkpoint.advertised_manifest_ref:
            raise ValueError("advertised manifest does not match commit phase")
        return checkpoint

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionCheckpointV1":
        """Restore a checkpoint from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionHeartbeatV1:
    """Bounded activity progress suitable for Temporal heartbeat payloads."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    phase: ReconstructionCommitPhase
    sequence: int
    completed_units: int
    total_units: int
    observed_event_count: int = 0
    candidate_event_count: int = 0
    accepted_event_count: int = 0
    scratch_bytes: int = 0
    output_bytes: int = 0
    checkpoint_id: str | None = None
    cancellation_requested: bool = False
    message: str = ""
    heartbeat_id: str = ""
    schema_version: str = RECONSTRUCTION_HEARTBEAT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_HEARTBEAT_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction heartbeat schema")
        for name in ("run_id", "window_id", "synchronization_unit_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "phase",
            ReconstructionCommitPhase.from_value(self.phase),
        )
        for name in (
            "sequence",
            "completed_units",
            "total_units",
            "observed_event_count",
            "candidate_event_count",
            "accepted_event_count",
            "scratch_bytes",
            "output_bytes",
        ):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        if self.completed_units > self.total_units:
            raise ValueError("completed_units cannot exceed total_units")
        object.__setattr__(
            self,
            "checkpoint_id",
            _optional_text(self.checkpoint_id),
        )
        object.__setattr__(self, "message", str(self.message or "").strip())
        object.__setattr__(
            self,
            "cancellation_requested",
            _strict_bool(
                self.cancellation_requested,
                "cancellation_requested",
            ),
        )
        expected = _stable_id("heartbeat", self.identity_payload())
        supplied = _optional_text(self.heartbeat_id)
        if supplied is not None and supplied != expected:
            raise ValueError(
                "heartbeat_id does not match deterministic identity"
            )
        object.__setattr__(self, "heartbeat_id", expected)
        _ensure_payload_size(
            self.to_dict(),
            MAX_HEARTBEAT_BYTES,
            "heartbeat",
        )

    @property
    def percent_complete(self) -> float:
        """Return bounded progress percentage."""
        if self.total_units == 0:
            return 0.0
        return min(100.0, self.completed_units / self.total_units * 100.0)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic bounded progress fields."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "phase": self.phase.value,
            "sequence": self.sequence,
            "completed_units": self.completed_units,
            "total_units": self.total_units,
            "observed_event_count": self.observed_event_count,
            "candidate_event_count": self.candidate_event_count,
            "accepted_event_count": self.accepted_event_count,
            "scratch_bytes": self.scratch_bytes,
            "output_bytes": self.output_bytes,
            "checkpoint_id": self.checkpoint_id,
            "cancellation_requested": self.cancellation_requested,
            "message": self.message,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return GUI/Temporal-ready progress metadata without rows."""
        return {
            **self.identity_payload(),
            "heartbeat_id": self.heartbeat_id,
            "event_type": "reconstruction_progress",
            "percent_complete": self.percent_complete,
            "stops_future_work_on_cancel": True,
            "resume_mode": "last_valid_checkpoint",
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReconstructionHeartbeatV1":
        """Restore and verify a version-one heartbeat."""
        _require_schema(data, RECONSTRUCTION_HEARTBEAT_SCHEMA_VERSION)
        heartbeat = cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            phase=ReconstructionCommitPhase.from_value(
                str(data.get("phase", ""))
            ),
            sequence=cast(int, data.get("sequence")),
            completed_units=cast(int, data.get("completed_units")),
            total_units=cast(int, data.get("total_units")),
            observed_event_count=cast(
                int,
                data.get("observed_event_count", 0),
            ),
            candidate_event_count=cast(
                int,
                data.get("candidate_event_count", 0),
            ),
            accepted_event_count=cast(
                int,
                data.get("accepted_event_count", 0),
            ),
            scratch_bytes=cast(int, data.get("scratch_bytes", 0)),
            output_bytes=cast(int, data.get("output_bytes", 0)),
            checkpoint_id=_optional_text(data.get("checkpoint_id")),
            cancellation_requested=cast(
                bool,
                data.get("cancellation_requested", False),
            ),
            message=str(data.get("message", "")),
            heartbeat_id=str(data.get("heartbeat_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        if data.get("event_type", "reconstruction_progress") != (
            "reconstruction_progress"
        ):
            raise ValueError("unsupported heartbeat event_type")
        return heartbeat

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionHeartbeatV1":
        """Restore a heartbeat from deterministic JSON."""
        return cls.from_dict(_json_mapping(text))


def artifact_ref_for_json_contract(
    contract: Any,
    *,
    kind: str,
    path: str,
    metadata: Mapping[str, JSONValue] | None = None,
) -> ArtifactRef:
    """Build a strong artifact reference for deterministic contract JSON."""
    serializer = getattr(contract, "to_json", None)
    if not callable(serializer):
        raise ValueError("contract must provide to_json()")
    encoded = str(serializer()).encode("utf-8")
    return _validated_artifact_ref(
        ArtifactRef(
            kind=_required_text(kind),
            path=_required_text(path),
            size_bytes=len(encoded),
            sha256=hashlib.sha256(encoded).hexdigest(),
            metadata=dict(metadata or {}),
        )
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _required_text(value: Any) -> str:
    normalized = str(value).strip() if value is not None else ""
    if not normalized:
        raise ValueError("required text value is empty")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalized_symbol(value: Any) -> str:
    return _required_text(value).lower()


def _normalized_symbols(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({_normalized_symbol(value) for value in values}))
    if not normalized:
        raise ValueError("at least one symbol is required")
    return normalized


def _normalized_id_tuple(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def _positive_int(value: Any, name: str) -> int:
    normalized = _nonnegative_int(value, name)
    if normalized < 1:
        raise ValueError(f"{name} must be positive")
    return normalized


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _bounded_int64(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not INT64_MIN <= value <= INT64_MAX:
        raise ValueError(f"{name} is outside signed 64-bit range")
    return value


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _required_sha256(value: Any, name: str) -> str:
    normalized = _required_text(value).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return normalized


def _validated_artifact_ref(value: ArtifactRef) -> ArtifactRef:
    if not isinstance(value, ArtifactRef):
        raise ValueError("artifact reference must be an ArtifactRef")
    size = _nonnegative_int(value.size_bytes, "artifact.size_bytes")
    metadata = dict(value.metadata)
    _validate_json_value(metadata, "artifact.metadata")
    _reject_inline_data(metadata)
    _ensure_payload_size(
        metadata,
        MAX_ARTIFACT_METADATA_BYTES,
        "artifact metadata",
    )
    return ArtifactRef(
        kind=_required_text(value.kind),
        path=_required_text(value.path),
        size_bytes=size,
        sha256=_required_sha256(value.sha256, "artifact.sha256"),
        metadata=metadata,
    )


def _validated_artifact_refs(
    values: Sequence[ArtifactRef],
) -> tuple[ArtifactRef, ...]:
    if len(values) > MAX_ARTIFACT_REFS_PER_CONTRACT:
        raise ValueError("artifact reference count exceeds bounded limit")
    normalized = tuple(_validated_artifact_ref(value) for value in values)
    if len({(ref.kind, ref.path, ref.sha256) for ref in normalized}) != len(
        normalized
    ):
        raise ValueError("duplicate artifact reference")
    return tuple(
        sorted(normalized, key=lambda ref: (ref.kind, ref.path, ref.sha256))
    )


def _artifact_identity_payload(value: ArtifactRef) -> dict[str, JSONValue]:
    return {
        "kind": value.kind,
        "path": value.path,
        "size_bytes": value.size_bytes,
        "sha256": value.sha256,
    }


def _artifact_content_identity_payload(
    value: ArtifactRef,
) -> dict[str, JSONValue]:
    """Return content identity without worker-local artifact placement."""
    return {
        "kind": value.kind,
        "size_bytes": value.size_bytes,
        "sha256": value.sha256,
    }


def _optional_artifact_identity_payload(
    value: ArtifactRef | None,
) -> dict[str, JSONValue] | None:
    if value is None:
        return None
    return _artifact_identity_payload(value)


def _optional_artifact_dict(
    value: ArtifactRef | None,
) -> dict[str, JSONValue] | None:
    return value.to_dict() if value is not None else None


def _optional_artifact_from_value(value: Any) -> ArtifactRef | None:
    if value is None:
        return None
    return ArtifactRef.from_dict(_mapping(value))


def _reject_inline_data(value: JSONValue) -> None:
    if isinstance(value, dict):
        forbidden = _FORBIDDEN_INLINE_DATA_KEYS.intersection(value)
        if forbidden:
            raise ValueError(
                "artifact metadata cannot contain inline data keys: "
                + ", ".join(sorted(forbidden))
            )
        for nested in value.values():
            _reject_inline_data(nested)
    elif isinstance(value, list):
        for nested in value:
            _reject_inline_data(nested)


def _validate_json_value(value: Any, path: str) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must contain only finite numbers")
        return
    if isinstance(value, list):
        for index, nested in enumerate(value):
            _validate_json_value(nested, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, nested in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{path} keys must be strings")
            _validate_json_value(nested, f"{path}.{key}")
        return
    raise ValueError(f"{path} contains unsupported JSON value")


def _monotonic_optional_int64(
    previous: int | None,
    requested: int | None,
    name: str,
) -> int | None:
    if requested is None:
        return previous
    normalized = _bounded_int64(requested, name)
    if previous is not None and normalized < previous:
        raise ValueError(f"{name} cannot move backwards")
    return normalized


def _ensure_payload_size(
    payload: Mapping[str, JSONValue],
    limit: int,
    name: str,
) -> None:
    size = len(canonical_contract_json(payload).encode("utf-8"))
    if size > limit:
        raise ValueError(f"{name} payload {size} bytes exceeds limit {limit}")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError(f"unsupported schema version; expected {expected}")


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a JSON object")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError("expected a JSON array")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)
