"""First-party deterministic planning for the ASCII tick reconstruction product.

This module is the bounded integration seam between the repository's
scientific contracts and its Temporal reconstruction request contract.  It
resolves strong artifacts, inventories immutable ASCII tick partitions,
performs compatibility and information-safety preflight, estimates resources,
and emits stage-complete workflow requests.  Tick rows never enter the plan or
workflow payloads.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, cast

from histdatacom.data_analytics.feed_epochs_v2 import (
    read_active_time_feed_epoch_definition,
)
from histdatacom.cross_series_constraints import (
    CROSS_SERIES_CONSTRAINT_POLICY_ARTIFACT_KIND,
    CrossSeriesConstraintPolicyV1,
)
from histdatacom.datasets import (
    DatasetContractError,
    DatasetFailureCode,
    HistDataProviderAdapter,
)
from histdatacom.market_context import (
    CftcPositioningCorpusV1,
    CftcReportFamily,
    CftcReportScope,
    MarketContextCorpusV1,
    MarketContextKind,
    preflight_cftc_positioning_corpus,
    preflight_market_context_corpus,
    read_cftc_positioning_corpus,
    read_market_context_corpus,
)
from histdatacom.orchestration.reconstruction import (
    MAX_STAGE_ARTIFACT_REFS,
    RECONSTRUCTION_STAGE_ORDER,
    ReconstructionStage,
    ReconstructionStageCommandV1,
    ReconstructionWindowTaskV1,
    ReconstructionWorkflowRequestV1,
    artifact_ref_for_file,
    verify_artifact_ref,
)
from histdatacom.reconstruction_evidence import (
    CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
    RECONSTRUCTION_EVIDENCE_POLICY_ARTIFACT_KIND,
    ReconstructionEvidencePolicyV1,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.benchmark_corpus import (
    PREDECLARED_GATE_COMMIT,
    read_reverse_degradation_benchmark_corpus,
)
from histdatacom.synthetic.benchmark_gates import (
    load_default_benchmark_promotion_gate_policy,
)
from histdatacom.synthetic.carving import HistoricalCarvingConstraintSetV1
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.cross_currency import (
    EURUSD_TRIANGLE_SYMBOLS,
    CrossCurrencyReconciliationConfigV1,
    CrossCurrencySymbolCoverageV1,
    CrossCurrencyWindowPlanStatus,
    eurusd_triangle_reconciliation_config,
    plan_cross_currency_windows,
)
from histdatacom.synthetic.delivery import ReconstructionDeliveryMode
from histdatacom.synthetic.ensembles import (
    EnsembleCalibrationConfigV1,
    plan_reconstruction_ensemble,
)
from histdatacom.synthetic.generation import EmpiricalMotifGeneratorConfigV1
from histdatacom.synthetic.information import (
    InformationAuditReportV1,
    InformationInputKind,
    InformationMode,
    InformationScope,
    InformationSplitKind,
    InformationStage,
    ReconstructionInformationInputV1,
    ReconstructionInformationManifestV1,
    ReconstructionInformationPolicyV1,
    ReconstructionInformationSplitV1,
    reconstruction_information_window_plan_id,
    require_reconstruction_information_audit,
)
from histdatacom.synthetic.motif_library import (
    ModernReferenceMotifProfileV1,
    read_modern_reference_motif_artifact,
    read_modern_reference_motif_index,
)
from histdatacom.synthetic.observation import (
    ObservationOperatorV1,
    read_observation_operator_artifact,
)
from histdatacom.synthetic.persistence import estimate_reconstruction_retention
from histdatacom.synthetic.streaming import (
    ReconstructionResourceEstimateV1,
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
    ReconstructionWindowV1,
)

RECONSTRUCTION_SOURCE_PARTITION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-source-partition.v1"
)
RECONSTRUCTION_SOURCE_INVENTORY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-source-inventory.v1"
)
RECONSTRUCTION_PLAN_CONFIGURATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-configuration.v1"
)
RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-execution-manifest.v1"
)
RECONSTRUCTION_PLAN_REFUSAL_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-refusal.v1"
)
RECONSTRUCTION_PLAN_RESOURCE_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-plan-resource-summary.v1"
)
SYNTHETIC_INFILL_PLAN_SCHEMA_VERSION = "histdatacom.synthetic-infill-plan.v1"

ASCII_TICK_SOURCE_KIND = "histdata_ascii_tick_arrow"
SOURCE_INVENTORY_ARTIFACT_KIND = "reconstruction_source_inventory_v1"
PLAN_CONFIGURATION_ARTIFACT_KIND = "reconstruction_plan_configuration_v1"
PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND = (
    "reconstruction_plan_execution_manifest_v1"
)
SYNTHETIC_INFILL_PLAN_ARTIFACT_KIND = "synthetic_infill_plan_v1"

DEFAULT_RECONSTRUCTION_WINDOW_SIZE_NS = 30 * 24 * 60 * 60 * 1_000_000_000
DEFAULT_RECONSTRUCTION_REQUEST_WINDOW_LIMIT = 32
DEFAULT_RECONSTRUCTION_MAX_PARALLEL_WINDOWS = 2
DEFAULT_RECONSTRUCTION_BASE_SEED = 20260715
_RESOURCE_FIXED_OVERHEAD_BYTES = 512 * 1024 * 1024
_RESOURCE_LEDGER_BYTES_PER_INTERVAL = 8 * 1024
_SOURCE_ROW_DENSITY_SAFETY_FACTOR = 4
MAX_RECONSTRUCTION_PLAN_ARTIFACTS = 64
MAX_RECONSTRUCTION_PLAN_REFUSALS = 4096
MAX_RECONSTRUCTION_PLAN_REQUESTS = 4096
MAX_SYNTHETIC_INFILL_PLAN_BYTES = 64 * 1024 * 1024

SCIENTIFIC_NONCLAIM = (
    "Output is a plausible counterfactual ensemble conditioned on declared "
    "artifacts and constraints; it is not recovered historical truth."
)
IMMUTABLE_ANCHOR_POLICY = (
    "Every observed ASCII tick keeps its original bid, ask, timestamp, "
    "partition, and zero-based Arrow row ordinal; synthetic events are added "
    "without renumbering or replacing observed anchors."
)
TICK_ONLY_INPUT_POLICY = (
    "Only ASCII/T bid-ask tick caches are reconstruction inputs. M1, bars, "
    "OHLC, and downstream bar projections are never anchors or inputs."
)

FIRST_PARTY_RECONSTRUCTION_HANDLERS: Mapping[ReconstructionStage, str] = {
    ReconstructionStage.SOURCE_ENRICHMENT: (
        "histdatacom.reconstruction.source-enrichment.v3"
    ),
    ReconstructionStage.PROPOSAL: "histdatacom.reconstruction.proposal.v7",
    ReconstructionStage.CARVING: "histdatacom.reconstruction.carving.v4",
    ReconstructionStage.CROSS_SERIES_RECONCILIATION: (
        "histdatacom.reconstruction.cross-series-reconciliation.v5"
    ),
    ReconstructionStage.BROKER_TRANSFER: (
        "histdatacom.reconstruction.delivery-projection.v2"
    ),
    ReconstructionStage.VALIDATION: (
        "histdatacom.reconstruction.validation.v2"
    ),
    ReconstructionStage.ATOMIC_PARTITION_COMMIT: (
        "histdatacom.reconstruction.atomic-partition-commit.v1"
    ),
}

_PERIOD_RE = re.compile(r"^\d{6}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_SOURCE_PARTITION_UTC_SPILL_NS = 24 * 60 * 60 * 1_000_000_000


class ReconstructionPlanCompatibilityError(ValueError):
    """Resolved artifacts cannot safely produce an executable plan."""


class ReconstructionPlanRefusalCode(str, Enum):
    """Stable pre-execution refusal categories."""

    FEED_EPOCH_UNSUPPORTED = "feed_epoch_unsupported"
    MARKET_CONTEXT_UNSUPPORTED = "market_context_unsupported"
    CFTC_POSITIONING_UNSUPPORTED = "cftc_positioning_unsupported"
    INFORMATION_LEAKAGE = "information_leakage"


@dataclass(frozen=True, slots=True)
class ReconstructionSourcePartitionV1:
    """One immutable monthly ASCII/T Arrow cache partition."""

    symbol: str
    period: str
    artifact: ArtifactRef
    row_count: int
    coverage_start_ns: int
    coverage_end_ns: int
    first_timestamp_ms: int
    last_timestamp_ms: int
    feed_epoch_evidence_id: str
    partition_id: str = ""
    schema_version: str = RECONSTRUCTION_SOURCE_PARTITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_SOURCE_PARTITION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction source partition schema"
            )
        symbol = _symbol(self.symbol)
        period = _period(self.period)
        artifact = _strong_ref(self.artifact)
        if artifact.kind != ASCII_TICK_SOURCE_KIND:
            raise ValueError("source partition requires an ASCII tick artifact")
        if artifact.metadata.get("symbol") != symbol:
            raise ValueError("source partition artifact symbol differs")
        if artifact.metadata.get("period") != period:
            raise ValueError("source partition artifact period differs")
        rows = _positive_int(self.row_count, "row_count")
        start = _int64(self.coverage_start_ns, "coverage_start_ns")
        end = _int64(self.coverage_end_ns, "coverage_end_ns")
        if end <= start:
            raise ValueError("source partition coverage is empty")
        first = _int64(self.first_timestamp_ms, "first_timestamp_ms")
        last = _int64(self.last_timestamp_ms, "last_timestamp_ms")
        if last < first:
            raise ValueError("source partition timestamps regress")
        if not (
            start - _MAX_SOURCE_PARTITION_UTC_SPILL_NS
            <= first * 1_000_000
            < end + _MAX_SOURCE_PARTITION_UTC_SPILL_NS
        ):
            raise ValueError(
                "source partition first timestamp is outside period"
            )
        if not (
            start - _MAX_SOURCE_PARTITION_UTC_SPILL_NS
            <= last * 1_000_000
            < end + _MAX_SOURCE_PARTITION_UTC_SPILL_NS
        ):
            raise ValueError(
                "source partition last timestamp is outside period"
            )
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "artifact", artifact)
        object.__setattr__(self, "row_count", rows)
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        object.__setattr__(self, "first_timestamp_ms", first)
        object.__setattr__(self, "last_timestamp_ms", last)
        object.__setattr__(
            self,
            "feed_epoch_evidence_id",
            _required_text(self.feed_epoch_evidence_id),
        )
        expected = _stable_id(
            "reconstruction-source-partition", self.identity_payload()
        )
        if self.partition_id and self.partition_id != expected:
            raise ValueError(
                "source partition_id differs from immutable content"
            )
        object.__setattr__(self, "partition_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "period": self.period,
            "sha256": self.artifact.sha256,
            "size_bytes": self.artifact.size_bytes,
            "row_count": self.row_count,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "first_timestamp_ms": self.first_timestamp_ms,
            "last_timestamp_ms": self.last_timestamp_ms,
            "feed_epoch_evidence_id": self.feed_epoch_evidence_id,
            "row_identity_basis": "zero-based-arrow-row-ordinal-v1",
            "partition_clock_policy": "histdata-source-month-with-utc-spill-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "artifact": self.artifact.to_dict(),
            "partition_id": self.partition_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionSourcePartitionV1:
        _require_schema(data, RECONSTRUCTION_SOURCE_PARTITION_SCHEMA_VERSION)
        _require_derived(
            data, "row_identity_basis", "zero-based-arrow-row-ordinal-v1"
        )
        _require_derived(
            data,
            "partition_clock_policy",
            "histdata-source-month-with-utc-spill-v1",
        )
        return cls(
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            artifact=ArtifactRef.from_dict(_mapping(data.get("artifact"))),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            coverage_start_ns=_strict_int(
                data.get("coverage_start_ns"), "coverage_start_ns"
            ),
            coverage_end_ns=_strict_int(
                data.get("coverage_end_ns"), "coverage_end_ns"
            ),
            first_timestamp_ms=_strict_int(
                data.get("first_timestamp_ms"), "first_timestamp_ms"
            ),
            last_timestamp_ms=_strict_int(
                data.get("last_timestamp_ms"), "last_timestamp_ms"
            ),
            feed_epoch_evidence_id=str(data.get("feed_epoch_evidence_id", "")),
            partition_id=str(data.get("partition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionSourceInventoryV1:
    """Complete common-range immutable source inventory."""

    source_root: str
    symbols: tuple[str, ...]
    periods: tuple[str, ...]
    partitions: tuple[ReconstructionSourcePartitionV1, ...]
    requested_start_ns: int
    requested_end_ns: int
    total_row_count: int
    total_size_bytes: int
    inventory_id: str = ""
    schema_version: str = RECONSTRUCTION_SOURCE_INVENTORY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_SOURCE_INVENTORY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction source inventory schema"
            )
        root = str(
            Path(_required_text(self.source_root)).expanduser().resolve()
        )
        symbols = _symbols(self.symbols)
        periods = tuple(sorted({_period(value) for value in self.periods}))
        if not periods:
            raise ValueError("source inventory periods cannot be empty")
        partitions = tuple(
            sorted(self.partitions, key=lambda item: (item.period, item.symbol))
        )
        expected_keys = {
            (period, symbol) for period in periods for symbol in symbols
        }
        actual_keys = {(item.period, item.symbol) for item in partitions}
        if actual_keys != expected_keys or len(partitions) != len(
            expected_keys
        ):
            raise ValueError(
                "source inventory is not a complete synchronized triangle"
            )
        start = _int64(self.requested_start_ns, "requested_start_ns")
        end = _int64(self.requested_end_ns, "requested_end_ns")
        if end <= start:
            raise ValueError("source inventory requested interval is empty")
        rows = sum(item.row_count for item in partitions)
        size = sum(cast(int, item.artifact.size_bytes) for item in partitions)
        if self.total_row_count != rows or self.total_size_bytes != size:
            raise ValueError("source inventory aggregate counts differ")
        object.__setattr__(self, "source_root", root)
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "periods", periods)
        object.__setattr__(self, "partitions", partitions)
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        expected = _stable_id(
            "reconstruction-source-inventory", self.identity_payload()
        )
        if self.inventory_id and self.inventory_id != expected:
            raise ValueError(
                "source inventory_id differs from immutable content"
            )
        object.__setattr__(self, "inventory_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbols": list(self.symbols),
            "periods": list(self.periods),
            "partitions": [item.identity_payload() for item in self.partitions],
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "total_row_count": self.total_row_count,
            "total_size_bytes": self.total_size_bytes,
            "input_contract": "ascii/T-tick-bid-ask-only",
            "observed_values_immutable": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "source_root": self.source_root,
            "partitions": [item.to_dict() for item in self.partitions],
            "inventory_id": self.inventory_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    def partitions_for_window(
        self, window: ReconstructionWindowV1
    ) -> tuple[ReconstructionSourcePartitionV1, ...]:
        selected = tuple(
            item
            for item in self.partitions
            if item.coverage_end_ns > window.input_start_ns
            and item.coverage_start_ns < window.input_end_ns
        )
        if {item.symbol for item in selected} != set(self.symbols):
            raise ReconstructionPlanCompatibilityError(
                "window source partitions do not cover the complete triangle"
            )
        return selected

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionSourceInventoryV1:
        _require_schema(data, RECONSTRUCTION_SOURCE_INVENTORY_SCHEMA_VERSION)
        _require_derived(data, "input_contract", "ascii/T-tick-bid-ask-only")
        _require_derived(data, "observed_values_immutable", True)
        return cls(
            source_root=str(data.get("source_root", "")),
            symbols=_string_tuple(data.get("symbols")),
            periods=_string_tuple(data.get("periods")),
            partitions=tuple(
                ReconstructionSourcePartitionV1.from_dict(_mapping(value))
                for value in _sequence(data.get("partitions"))
            ),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            total_row_count=_strict_int(
                data.get("total_row_count"), "total_row_count"
            ),
            total_size_bytes=_strict_int(
                data.get("total_size_bytes"), "total_size_bytes"
            ),
            inventory_id=str(data.get("inventory_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ReconstructionSourceInventoryV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionPlanConfigurationV1:
    """All scientific and operational policies consumed by stage handlers."""

    delivery_mode: ReconstructionDeliveryMode
    information_policy: ReconstructionInformationPolicyV1
    generator_config: EmpiricalMotifGeneratorConfigV1
    carving_constraints: HistoricalCarvingConstraintSetV1
    cross_currency_config: CrossCurrencyReconciliationConfigV1
    ensemble_config: EnsembleCalibrationConfigV1
    storage_policy: ReconstructionStoragePolicyV1
    window_size_ns: int
    left_halo_ns: int
    right_lookahead_ns: int
    max_parallel_windows: int
    configuration_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_CONFIGURATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_CONFIGURATION_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction plan configuration schema"
            )
        object.__setattr__(
            self,
            "delivery_mode",
            ReconstructionDeliveryMode.from_value(self.delivery_mode),
        )
        expected_types = (
            (self.information_policy, ReconstructionInformationPolicyV1),
            (self.generator_config, EmpiricalMotifGeneratorConfigV1),
            (self.carving_constraints, HistoricalCarvingConstraintSetV1),
            (self.cross_currency_config, CrossCurrencyReconciliationConfigV1),
            (self.ensemble_config, EnsembleCalibrationConfigV1),
            (self.storage_policy, ReconstructionStoragePolicyV1),
        )
        if any(
            not isinstance(value, expected)
            for value, expected in expected_types
        ):
            raise ValueError(
                "reconstruction plan configuration contains a wrong contract type"
            )
        window_size = _positive_int(self.window_size_ns, "window_size_ns")
        left = _nonnegative_int(self.left_halo_ns, "left_halo_ns")
        right = _nonnegative_int(self.right_lookahead_ns, "right_lookahead_ns")
        if (
            self.information_policy.information_mode
            is InformationMode.EX_ANTE_SIMULATION
            and right
        ):
            raise ValueError(
                "ex-ante reconstruction cannot declare right look-ahead"
            )
        parallel = _positive_int(
            self.max_parallel_windows, "max_parallel_windows"
        )
        if parallel > self.storage_policy.max_inflight_batches:
            raise ValueError("plan parallelism exceeds storage inflight quota")
        object.__setattr__(self, "window_size_ns", window_size)
        object.__setattr__(self, "left_halo_ns", left)
        object.__setattr__(self, "right_lookahead_ns", right)
        object.__setattr__(self, "max_parallel_windows", parallel)
        if (
            tuple(FIRST_PARTY_RECONSTRUCTION_HANDLERS)
            != RECONSTRUCTION_STAGE_ORDER
        ):
            raise ValueError(
                "first-party handler map does not cover every stage"
            )
        expected = _stable_id(
            "reconstruction-plan-configuration", self.identity_payload()
        )
        if self.configuration_id and self.configuration_id != expected:
            raise ValueError("plan configuration_id differs")
        object.__setattr__(self, "configuration_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "delivery_mode": self.delivery_mode.value,
            "information_policy": self.information_policy.to_dict(),
            "generator_config": self.generator_config.to_dict(),
            "carving_constraints": self.carving_constraints.to_dict(),
            "cross_currency_config": self.cross_currency_config.to_dict(),
            "ensemble_config": self.ensemble_config.to_dict(),
            "storage_policy": self.storage_policy.to_dict(),
            "window_size_ns": self.window_size_ns,
            "left_halo_ns": self.left_halo_ns,
            "right_lookahead_ns": self.right_lookahead_ns,
            "max_parallel_windows": self.max_parallel_windows,
            "handler_names": {
                stage.value: FIRST_PARTY_RECONSTRUCTION_HANDLERS[stage]
                for stage in RECONSTRUCTION_STAGE_ORDER
            },
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
            "immutable_anchor_policy": IMMUTABLE_ANCHOR_POLICY,
            "input_policy": TICK_ONLY_INPUT_POLICY,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "configuration_id": self.configuration_id,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanConfigurationV1:
        _require_schema(data, RECONSTRUCTION_PLAN_CONFIGURATION_SCHEMA_VERSION)
        _require_derived(data, "scientific_nonclaim", SCIENTIFIC_NONCLAIM)
        _require_derived(
            data, "immutable_anchor_policy", IMMUTABLE_ANCHOR_POLICY
        )
        _require_derived(data, "input_policy", TICK_ONLY_INPUT_POLICY)
        _require_derived(
            data,
            "handler_names",
            {
                stage.value: FIRST_PARTY_RECONSTRUCTION_HANDLERS[stage]
                for stage in RECONSTRUCTION_STAGE_ORDER
            },
        )
        return cls(
            delivery_mode=ReconstructionDeliveryMode.from_value(
                str(data.get("delivery_mode", ""))
            ),
            information_policy=ReconstructionInformationPolicyV1.from_dict(
                _mapping(data.get("information_policy"))
            ),
            generator_config=EmpiricalMotifGeneratorConfigV1.from_dict(
                _mapping(data.get("generator_config"))
            ),
            carving_constraints=HistoricalCarvingConstraintSetV1.from_dict(
                _mapping(data.get("carving_constraints"))
            ),
            cross_currency_config=CrossCurrencyReconciliationConfigV1.from_dict(
                _mapping(data.get("cross_currency_config"))
            ),
            ensemble_config=EnsembleCalibrationConfigV1.from_dict(
                _mapping(data.get("ensemble_config"))
            ),
            storage_policy=ReconstructionStoragePolicyV1.from_dict(
                _mapping(data.get("storage_policy"))
            ),
            window_size_ns=_strict_int(
                data.get("window_size_ns"), "window_size_ns"
            ),
            left_halo_ns=_strict_int(data.get("left_halo_ns"), "left_halo_ns"),
            right_lookahead_ns=_strict_int(
                data.get("right_lookahead_ns"), "right_lookahead_ns"
            ),
            max_parallel_windows=_strict_int(
                data.get("max_parallel_windows"), "max_parallel_windows"
            ),
            configuration_id=str(data.get("configuration_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ReconstructionPlanConfigurationV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionPlanRefusalV1:
    """One window rejected before workflow submission."""

    start_ns: int
    end_ns: int
    code: ReconstructionPlanRefusalCode
    reason: str
    symbols: tuple[str, ...] = EURUSD_TRIANGLE_SYMBOLS
    refusal_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_REFUSAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_PLAN_REFUSAL_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction plan refusal schema")
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ValueError("refusal interval is empty")
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(
            self, "code", ReconstructionPlanRefusalCode(self.code)
        )
        object.__setattr__(self, "reason", _bounded_text(self.reason, 2048))
        object.__setattr__(self, "symbols", _symbols(self.symbols))
        expected = _stable_id(
            "reconstruction-plan-refusal", self.identity_payload()
        )
        if self.refusal_id and self.refusal_id != expected:
            raise ValueError("reconstruction refusal_id differs")
        object.__setattr__(self, "refusal_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "code": self.code.value,
            "reason": self.reason,
            "symbols": list(self.symbols),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "refusal_id": self.refusal_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionPlanRefusalV1:
        _require_schema(data, RECONSTRUCTION_PLAN_REFUSAL_SCHEMA_VERSION)
        return cls(
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            code=ReconstructionPlanRefusalCode(str(data.get("code", ""))),
            reason=str(data.get("reason", "")),
            symbols=_string_tuple(data.get("symbols")),
            refusal_id=str(data.get("refusal_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanResourceSummaryV1:
    """Full-plan source, candidate, memory, scratch, output, and graph bounds."""

    source_event_count: int
    source_size_bytes: int
    planned_window_count: int
    executable_window_count: int
    refused_window_count: int
    ensemble_member_count: int
    retained_member_count: int
    workflow_request_count: int
    estimated_input_event_count: int
    estimated_candidate_event_count: int
    estimated_candidate_bytes: int
    estimated_peak_memory_bytes: int
    estimated_peak_scratch_bytes: int
    estimated_output_bytes: int
    estimated_partition_count: int
    summary_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_RESOURCE_SUMMARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_RESOURCE_SUMMARY_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction plan resource summary schema"
            )
        for name in (
            "source_event_count",
            "source_size_bytes",
            "planned_window_count",
            "executable_window_count",
            "refused_window_count",
            "ensemble_member_count",
            "retained_member_count",
            "workflow_request_count",
            "estimated_input_event_count",
            "estimated_candidate_event_count",
            "estimated_candidate_bytes",
            "estimated_peak_memory_bytes",
            "estimated_peak_scratch_bytes",
            "estimated_output_bytes",
            "estimated_partition_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.planned_window_count != (
            self.executable_window_count + self.refused_window_count
        ):
            raise ValueError("planned window counts do not reconcile")
        if not self.ensemble_member_count:
            raise ValueError("resource summary requires an ensemble")
        if not 1 <= self.retained_member_count <= self.ensemble_member_count:
            raise ValueError("retained member count is outside ensemble")
        if self.executable_window_count == 0:
            if self.refused_window_count != self.planned_window_count:
                raise ValueError(
                    "zero-work resource summary requires every window refused"
                )
            if self.workflow_request_count:
                raise ValueError(
                    "zero-work resource summary cannot contain workflow requests"
                )
            if any(
                getattr(self, name)
                for name in (
                    "estimated_input_event_count",
                    "estimated_candidate_event_count",
                    "estimated_candidate_bytes",
                    "estimated_peak_memory_bytes",
                    "estimated_peak_scratch_bytes",
                    "estimated_output_bytes",
                    "estimated_partition_count",
                )
            ):
                raise ValueError(
                    "zero-work resource summary must have zero work estimates"
                )
        elif not self.workflow_request_count:
            raise ValueError("resource summary requires workflow requests")
        expected = _stable_id(
            "reconstruction-plan-resources", self.identity_payload()
        )
        if self.summary_id and self.summary_id != expected:
            raise ValueError("resource summary_id differs")
        object.__setattr__(self, "summary_id", expected)

    @property
    def candidate_amplification(self) -> float:
        if not self.estimated_input_event_count:
            return 0.0
        return (
            self.estimated_candidate_event_count
            / self.estimated_input_event_count
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_event_count": self.source_event_count,
            "source_size_bytes": self.source_size_bytes,
            "planned_window_count": self.planned_window_count,
            "executable_window_count": self.executable_window_count,
            "refused_window_count": self.refused_window_count,
            "ensemble_member_count": self.ensemble_member_count,
            "retained_member_count": self.retained_member_count,
            "workflow_request_count": self.workflow_request_count,
            "estimated_input_event_count": self.estimated_input_event_count,
            "estimated_candidate_event_count": self.estimated_candidate_event_count,
            "estimated_candidate_bytes": self.estimated_candidate_bytes,
            "estimated_peak_memory_bytes": self.estimated_peak_memory_bytes,
            "estimated_peak_scratch_bytes": self.estimated_peak_scratch_bytes,
            "estimated_output_bytes": self.estimated_output_bytes,
            "estimated_partition_count": self.estimated_partition_count,
            "candidate_amplification": self.candidate_amplification,
            "scratch_basis": "peak-concurrent-window-scratch-v1",
            "output_basis": "retained-member-compressed-upper-bound-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "summary_id": self.summary_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanResourceSummaryV1:
        _require_schema(
            data, RECONSTRUCTION_PLAN_RESOURCE_SUMMARY_SCHEMA_VERSION
        )
        _require_derived(
            data, "scratch_basis", "peak-concurrent-window-scratch-v1"
        )
        _require_derived(
            data, "output_basis", "retained-member-compressed-upper-bound-v1"
        )
        return cls(
            source_event_count=_strict_int(
                data.get("source_event_count"), "source_event_count"
            ),
            source_size_bytes=_strict_int(
                data.get("source_size_bytes"), "source_size_bytes"
            ),
            planned_window_count=_strict_int(
                data.get("planned_window_count"), "planned_window_count"
            ),
            executable_window_count=_strict_int(
                data.get("executable_window_count"), "executable_window_count"
            ),
            refused_window_count=_strict_int(
                data.get("refused_window_count"), "refused_window_count"
            ),
            ensemble_member_count=_strict_int(
                data.get("ensemble_member_count"), "ensemble_member_count"
            ),
            retained_member_count=_strict_int(
                data.get("retained_member_count"), "retained_member_count"
            ),
            workflow_request_count=_strict_int(
                data.get("workflow_request_count"), "workflow_request_count"
            ),
            estimated_input_event_count=_strict_int(
                data.get(
                    "estimated_input_event_count",
                    data.get("source_event_count"),
                ),
                "estimated_input_event_count",
            ),
            estimated_candidate_event_count=_strict_int(
                data.get("estimated_candidate_event_count"),
                "estimated_candidate_event_count",
            ),
            estimated_candidate_bytes=_strict_int(
                data.get("estimated_candidate_bytes"),
                "estimated_candidate_bytes",
            ),
            estimated_peak_memory_bytes=_strict_int(
                data.get("estimated_peak_memory_bytes"),
                "estimated_peak_memory_bytes",
            ),
            estimated_peak_scratch_bytes=_strict_int(
                data.get("estimated_peak_scratch_bytes"),
                "estimated_peak_scratch_bytes",
            ),
            estimated_output_bytes=_strict_int(
                data.get("estimated_output_bytes"), "estimated_output_bytes"
            ),
            estimated_partition_count=_strict_int(
                data.get("estimated_partition_count"),
                "estimated_partition_count",
            ),
            summary_id=str(data.get("summary_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPlanExecutionManifestV1:
    """Strong artifact graph and execution roots consumed by every stage."""

    run_id: str
    configuration_id: str
    source_inventory_id: str
    information_manifest_id: str
    information_audit_id: str
    ensemble_plan_id: str
    retention_plan_id: str
    delivery_mode: ReconstructionDeliveryMode
    artifacts: Mapping[str, ArtifactRef]
    output_root: str
    checkpoint_root: str
    scratch_root: str
    planned_window_count: int
    executable_window_count: int
    refusal_ids: tuple[str, ...] = ()
    manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction execution manifest schema"
            )
        for name in (
            "run_id",
            "configuration_id",
            "source_inventory_id",
            "information_manifest_id",
            "information_audit_id",
            "ensemble_plan_id",
            "retention_plan_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "delivery_mode",
            ReconstructionDeliveryMode.from_value(self.delivery_mode),
        )
        artifacts = _artifact_mapping(self.artifacts)
        required = {
            "configuration",
            "source_inventory",
            "feed_epochs",
            "observation_operator",
            "market_context",
            "cftc_positioning",
            "benchmark_manifest",
            "motif_manifest",
            "motif_index",
            "motif_qualification",
            "motif_leakage_audit",
            "information_manifest",
            "information_audit",
            "ensemble_plan",
            "retention_plan",
        }
        if self.delivery_mode is ReconstructionDeliveryMode.BROKER_CONDITIONED:
            required.add("broker_delivery")
        allowed = required | {
            "evidence_policy",
            "cross_series_constraint_policy",
        }
        if not required.issubset(artifacts) or not set(artifacts).issubset(
            allowed
        ):
            raise ValueError(
                "execution artifact graph is incomplete or contains extras"
            )
        object.__setattr__(self, "artifacts", artifacts)
        output = _resolved_directory(self.output_root)
        checkpoint = _resolved_directory(self.checkpoint_root)
        scratch = _resolved_directory(self.scratch_root)
        durable_roots = {
            "output": Path(output),
            "checkpoint": Path(checkpoint),
            "scratch": Path(scratch),
        }
        root_items = tuple(durable_roots.items())
        for index, (left_name, left) in enumerate(root_items):
            for right_name, right in root_items[index + 1 :]:
                if not _paths_overlap(left, right):
                    continue
                raise ValueError(
                    f"execution {left_name} and {right_name} roots overlap"
                )
        for name, ref in artifacts.items():
            if Path(ref.path).is_relative_to(Path(scratch)):
                raise ValueError(
                    f"execution artifact {name} is inside the scratch root"
                )
        object.__setattr__(self, "output_root", output)
        object.__setattr__(self, "checkpoint_root", checkpoint)
        object.__setattr__(self, "scratch_root", scratch)
        planned = _positive_int(
            self.planned_window_count, "planned_window_count"
        )
        executable = _nonnegative_int(
            self.executable_window_count, "executable_window_count"
        )
        if executable > planned:
            raise ValueError("executable windows exceed planned windows")
        object.__setattr__(self, "planned_window_count", planned)
        object.__setattr__(self, "executable_window_count", executable)
        refusals = tuple(
            sorted({_required_text(value) for value in self.refusal_ids})
        )
        if len(refusals) != planned - executable:
            raise ValueError("execution refusal IDs do not reconcile")
        object.__setattr__(self, "refusal_ids", refusals)
        expected = _stable_id(
            "reconstruction-plan-execution", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("execution manifest_id differs")
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "configuration_id": self.configuration_id,
            "source_inventory_id": self.source_inventory_id,
            "information_manifest_id": self.information_manifest_id,
            "information_audit_id": self.information_audit_id,
            "ensemble_plan_id": self.ensemble_plan_id,
            "retention_plan_id": self.retention_plan_id,
            "delivery_mode": self.delivery_mode.value,
            "artifacts": {
                name: ref.to_dict() for name, ref in self.artifacts.items()
            },
            "output_root": self.output_root,
            "checkpoint_root": self.checkpoint_root,
            "scratch_root": self.scratch_root,
            "planned_window_count": self.planned_window_count,
            "executable_window_count": self.executable_window_count,
            "refusal_ids": list(self.refusal_ids),
            "large_rows_in_artifacts_only": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPlanExecutionManifestV1:
        _require_schema(
            data, RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_SCHEMA_VERSION
        )
        _require_derived(data, "large_rows_in_artifacts_only", True)
        return cls(
            run_id=str(data.get("run_id", "")),
            configuration_id=str(data.get("configuration_id", "")),
            source_inventory_id=str(data.get("source_inventory_id", "")),
            information_manifest_id=str(
                data.get("information_manifest_id", "")
            ),
            information_audit_id=str(data.get("information_audit_id", "")),
            ensemble_plan_id=str(data.get("ensemble_plan_id", "")),
            retention_plan_id=str(data.get("retention_plan_id", "")),
            delivery_mode=ReconstructionDeliveryMode.from_value(
                str(data.get("delivery_mode", ""))
            ),
            artifacts={
                str(name): ArtifactRef.from_dict(_mapping(value))
                for name, value in _mapping(data.get("artifacts")).items()
            },
            output_root=str(data.get("output_root", "")),
            checkpoint_root=str(data.get("checkpoint_root", "")),
            scratch_root=str(data.get("scratch_root", "")),
            planned_window_count=_strict_int(
                data.get("planned_window_count"), "planned_window_count"
            ),
            executable_window_count=_strict_int(
                data.get("executable_window_count"), "executable_window_count"
            ),
            refusal_ids=_string_tuple(data.get("refusal_ids")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ReconstructionPlanExecutionManifestV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionStagePlanV1:
    """Public, fully resolved planning context consumed by one stage handler."""

    command: ReconstructionStageCommandV1
    execution_manifest_ref: ArtifactRef
    execution_manifest: ReconstructionPlanExecutionManifestV1
    configuration: ReconstructionPlanConfigurationV1
    source_inventory: ReconstructionSourceInventoryV1

    def __post_init__(self) -> None:
        if not isinstance(self.command, ReconstructionStageCommandV1):
            raise TypeError(
                "stage plan requires a reconstruction stage command"
            )
        execution_ref = _strong_ref(self.execution_manifest_ref)
        if execution_ref.kind != PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND:
            raise ReconstructionPlanCompatibilityError(
                "stage command configuration is not an execution manifest"
            )
        object.__setattr__(self, "execution_manifest_ref", execution_ref)
        if self.command.configuration_refs != (execution_ref,):
            raise ReconstructionPlanCompatibilityError(
                "stage command lacks the exact execution manifest"
            )
        if (
            self.command.handler_name
            != FIRST_PARTY_RECONSTRUCTION_HANDLERS[self.command.stage]
        ):
            raise ReconstructionPlanCompatibilityError(
                "stage command does not use the first-party handler contract"
            )
        if (
            self.execution_manifest.configuration_id
            != self.configuration.configuration_id
        ):
            raise ReconstructionPlanCompatibilityError(
                "execution manifest configuration identity differs"
            )
        if (
            self.execution_manifest.source_inventory_id
            != self.source_inventory.inventory_id
        ):
            raise ReconstructionPlanCompatibilityError(
                "execution manifest source inventory identity differs"
            )
        if (
            self.execution_manifest.delivery_mode
            is not self.configuration.delivery_mode
        ):
            raise ReconstructionPlanCompatibilityError(
                "execution delivery mode differs from configuration"
            )
        _validate_stage_inputs(
            self.command, self.execution_manifest.delivery_mode
        )
        graph_refs = {
            canonical_contract_json(ref.to_dict())
            for ref in self.execution_manifest.artifacts.values()
        }
        source_refs = {
            canonical_contract_json(item.artifact.to_dict())
            for item in self.source_inventory.partitions
        }
        for ref in self.command.input_manifest_refs:
            ref_key = canonical_contract_json(ref.to_dict())
            if ref.kind == ASCII_TICK_SOURCE_KIND:
                if ref_key not in source_refs:
                    raise ReconstructionPlanCompatibilityError(
                        "stage source reference is absent from inventory"
                    )
            elif ref_key not in graph_refs:
                raise ReconstructionPlanCompatibilityError(
                    "stage input reference is absent from execution artifact graph"
                )


@dataclass(frozen=True, slots=True)
class SyntheticInfillPlanV1:
    """Executable, bounded, first-party ASCII tick reconstruction plan."""

    run: ReconstructionRunV1
    configuration_id: str
    execution_manifest_id: str
    information_mode: InformationMode
    delivery_mode: ReconstructionDeliveryMode
    requested_start_ns: int
    requested_end_ns: int
    workflow_requests: tuple[ReconstructionWorkflowRequestV1, ...]
    artifact_graph: Mapping[str, ArtifactRef]
    resources: ReconstructionPlanResourceSummaryV1
    refusals: tuple[ReconstructionPlanRefusalV1, ...] = ()
    plan_id: str = ""
    schema_version: str = SYNTHETIC_INFILL_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != SYNTHETIC_INFILL_PLAN_SCHEMA_VERSION:
            raise ValueError("unsupported synthetic infill plan schema")
        if not isinstance(self.run, ReconstructionRunV1):
            raise TypeError(
                "synthetic infill plan requires a reconstruction run"
            )
        object.__setattr__(
            self, "configuration_id", _required_text(self.configuration_id)
        )
        object.__setattr__(
            self,
            "execution_manifest_id",
            _required_text(self.execution_manifest_id),
        )
        object.__setattr__(
            self,
            "information_mode",
            InformationMode.from_value(self.information_mode),
        )
        object.__setattr__(
            self,
            "delivery_mode",
            ReconstructionDeliveryMode.from_value(self.delivery_mode),
        )
        start = _int64(self.requested_start_ns, "requested_start_ns")
        end = _int64(self.requested_end_ns, "requested_end_ns")
        if end <= start:
            raise ValueError(
                "synthetic infill plan requested interval is empty"
            )
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        requests = tuple(
            sorted(self.workflow_requests, key=lambda item: item.request_id)
        )
        if len(requests) > MAX_RECONSTRUCTION_PLAN_REQUESTS:
            raise ValueError(
                "synthetic infill workflow request count is outside limits"
            )
        if len({item.request_id for item in requests}) != len(requests):
            raise ValueError(
                "synthetic infill workflow request IDs are duplicated"
            )
        if any(item.run.run_id != self.run.run_id for item in requests):
            raise ValueError(
                "workflow request run differs from synthetic infill plan"
            )
        tasks = tuple(task for request in requests for task in request.tasks)
        if len({item.task_id for item in tasks}) != len(tasks):
            raise ValueError("synthetic infill workflow tasks are duplicated")
        object.__setattr__(self, "workflow_requests", requests)
        graph = _artifact_mapping(self.artifact_graph)
        if "execution_manifest" not in graph or "configuration" not in graph:
            raise ValueError(
                "synthetic infill artifact graph lacks execution contracts"
            )
        object.__setattr__(self, "artifact_graph", graph)
        if not isinstance(self.resources, ReconstructionPlanResourceSummaryV1):
            raise TypeError("synthetic infill plan requires a resource summary")
        if self.resources.workflow_request_count != len(requests):
            raise ValueError("resource workflow request count differs")
        if (
            self.resources.executable_window_count
            * self.resources.ensemble_member_count
            != len(tasks)
        ):
            raise ValueError(
                "resource executable window count differs from tasks"
            )
        refusals = tuple(
            sorted(
                self.refusals, key=lambda item: (item.start_ns, item.refusal_id)
            )
        )
        if len(refusals) > MAX_RECONSTRUCTION_PLAN_REFUSALS:
            raise ValueError("synthetic infill refusal count exceeds limit")
        if len(refusals) != self.resources.refused_window_count:
            raise ValueError("resource refusal count differs")
        object.__setattr__(self, "refusals", refusals)
        expected = _stable_id("synthetic-infill-plan", self.identity_payload())
        if self.plan_id and self.plan_id != expected:
            raise ValueError("synthetic infill plan_id differs")
        object.__setattr__(self, "plan_id", expected)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_SYNTHETIC_INFILL_PLAN_BYTES
        ):
            raise ValueError(
                "synthetic infill plan exceeds bounded artifact size"
            )

    @property
    def status(self) -> str:
        return "ready_with_refusals" if self.refusals else "ready"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "run": self.run.to_dict(),
            "configuration_id": self.configuration_id,
            "execution_manifest_id": self.execution_manifest_id,
            "information_mode": self.information_mode.value,
            "delivery_mode": self.delivery_mode.value,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "workflow_requests": [
                item.to_dict() for item in self.workflow_requests
            ],
            "artifact_graph": {
                name: ref.to_dict() for name, ref in self.artifact_graph.items()
            },
            "resources": self.resources.to_dict(),
            "refusals": [item.to_dict() for item in self.refusals],
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
            "immutable_anchor_policy": IMMUTABLE_ANCHOR_POLICY,
            "input_policy": TICK_ONLY_INPUT_POLICY,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "plan_id": self.plan_id,
            "status": self.status,
        }

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    def dry_run_payload(self) -> dict[str, JSONValue]:
        """Return a bounded graph summary without expanding task payloads."""
        batches: list[JSONValue] = []
        for request in self.workflow_requests:
            tasks = request.tasks
            batches.append(
                {
                    "request_id": request.request_id,
                    "request_fingerprint": request.request_fingerprint,
                    "ensemble_member_id": tasks[0].window.ensemble_member_id,
                    "window_count": len(tasks),
                    "core_start_ns": tasks[0].window.core_start_ns,
                    "core_end_ns": tasks[-1].window.core_end_ns,
                    "task_ids_sha256": hashlib.sha256(
                        "\n".join(item.task_id for item in tasks).encode(
                            "utf-8"
                        )
                    ).hexdigest(),
                }
            )
        return {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "status": self.status,
            "run_id": self.run.run_id,
            "symbols": list(self.run.symbols),
            "information_mode": self.information_mode.value,
            "delivery_mode": self.delivery_mode.value,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "workflow_batches": batches,
            "artifact_graph": {
                name: {
                    "kind": ref.kind,
                    "sha256": ref.sha256,
                    "size_bytes": ref.size_bytes,
                }
                for name, ref in self.artifact_graph.items()
            },
            "resources": self.resources.to_dict(),
            "refusals": [item.to_dict() for item in self.refusals],
            "scientific_nonclaim": SCIENTIFIC_NONCLAIM,
        }

    def to_dry_run_json(self) -> str:
        return str(canonical_contract_json(self.dry_run_payload()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SyntheticInfillPlanV1:
        _require_schema(data, SYNTHETIC_INFILL_PLAN_SCHEMA_VERSION)
        _require_derived(data, "scientific_nonclaim", SCIENTIFIC_NONCLAIM)
        _require_derived(
            data, "immutable_anchor_policy", IMMUTABLE_ANCHOR_POLICY
        )
        _require_derived(data, "input_policy", TICK_ONLY_INPUT_POLICY)
        return cls(
            run=ReconstructionRunV1.from_dict(_mapping(data.get("run"))),
            configuration_id=str(data.get("configuration_id", "")),
            execution_manifest_id=str(data.get("execution_manifest_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            delivery_mode=ReconstructionDeliveryMode.from_value(
                str(data.get("delivery_mode", ""))
            ),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            workflow_requests=tuple(
                ReconstructionWorkflowRequestV1.from_dict(_mapping(value))
                for value in _sequence(data.get("workflow_requests"))
            ),
            artifact_graph={
                str(name): ArtifactRef.from_dict(_mapping(value))
                for name, value in _mapping(data.get("artifact_graph")).items()
            },
            resources=ReconstructionPlanResourceSummaryV1.from_dict(
                _mapping(data.get("resources"))
            ),
            refusals=tuple(
                ReconstructionPlanRefusalV1.from_dict(_mapping(value))
                for value in _sequence(data.get("refusals"))
            ),
            plan_id=str(data.get("plan_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> SyntheticInfillPlanV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class _ResolvedPlanInputs:
    feed_epoch_definition: Any
    observation_operator: ObservationOperatorV1
    market_context: MarketContextCorpusV1
    cftc_positioning: CftcPositioningCorpusV1
    benchmark_corpus: Any
    motif_profile: ModernReferenceMotifProfileV1
    motif_index: Any
    artifacts: Mapping[str, ArtifactRef]
    motif_manifest: Mapping[str, Any]
    motif_qualification: Mapping[str, Any]
    motif_leakage_audit: Mapping[str, Any]


def build_synthetic_infill_plan(
    source_root: str | Path,
    *,
    feed_epoch_definition_path: str | Path,
    observation_operator_path: str | Path,
    market_context_corpus_path: str | Path,
    cftc_positioning_corpus_path: str | Path,
    benchmark_manifest_path: str | Path,
    motif_manifest_path: str | Path,
    motif_index_path: str | Path,
    motif_qualification_path: str | Path,
    motif_leakage_audit_path: str | Path,
    artifact_root: str | Path,
    output_root: str | Path,
    checkpoint_root: str | Path,
    scratch_root: str | Path,
    symbols: Iterable[str] = EURUSD_TRIANGLE_SYMBOLS,
    start_period: str | None = None,
    end_period: str | None = None,
    requested_start_ns: int | None = None,
    requested_end_ns: int | None = None,
    information_mode: InformationMode = InformationMode.EX_POST_RECONSTRUCTION,
    delivery_mode: ReconstructionDeliveryMode = (
        ReconstructionDeliveryMode.MODERN_REFERENCE
    ),
    broker_delivery_artifact: ArtifactRef | None = None,
    base_seed: int = DEFAULT_RECONSTRUCTION_BASE_SEED,
    window_size_ns: int = DEFAULT_RECONSTRUCTION_WINDOW_SIZE_NS,
    left_halo_ns: int | None = None,
    right_lookahead_ns: int = 0,
    max_parallel_windows: int = DEFAULT_RECONSTRUCTION_MAX_PARALLEL_WINDOWS,
    max_windows_per_request: int = DEFAULT_RECONSTRUCTION_REQUEST_WINDOW_LIMIT,
    storage_policy: ReconstructionStoragePolicyV1 | None = None,
    ensemble_config: EnsembleCalibrationConfigV1 | None = None,
    generator_config: EmpiricalMotifGeneratorConfigV1 | None = None,
    carving_constraints: HistoricalCarvingConstraintSetV1 | None = None,
    cross_currency_config: CrossCurrencyReconciliationConfigV1 | None = None,
    evidence_policy: ReconstructionEvidencePolicyV1 | None = None,
    cross_series_constraint_policy: CrossSeriesConstraintPolicyV1 | None = None,
) -> SyntheticInfillPlanV1:
    """Resolve real artifacts and build one executable first-party plan."""
    selected_symbols = _symbols(tuple(symbols))
    if selected_symbols != tuple(EURUSD_TRIANGLE_SYMBOLS):
        raise ReconstructionPlanCompatibilityError(
            "v2.1 requires the complete EURUSD/GBPUSD/EURGBP tick triangle"
        )
    source = Path(source_root).expanduser().resolve()
    if source.name.upper() != "T" or source.parent.name.upper() != "ASCII":
        raise ReconstructionPlanCompatibilityError(
            "reconstruction source_root must be the ASCII/T tick directory"
        )
    roots = _validated_plan_roots(
        source_root=source,
        artifact_root=artifact_root,
        output_root=output_root,
        checkpoint_root=checkpoint_root,
        scratch_root=scratch_root,
    )
    mode = InformationMode.from_value(information_mode)
    delivery = ReconstructionDeliveryMode.from_value(delivery_mode)
    if delivery is ReconstructionDeliveryMode.BROKER_CONDITIONED:
        if broker_delivery_artifact is None:
            raise ReconstructionPlanCompatibilityError(
                "broker-conditioned delivery requires a strong broker artifact"
            )
        broker_ref = _strong_ref(broker_delivery_artifact)
        if broker_ref.kind != "broker_delivery_artifact_v1":
            raise ReconstructionPlanCompatibilityError(
                "broker-conditioned delivery requires a broker delivery artifact"
            )
        verify_artifact_ref(broker_ref)
    elif broker_delivery_artifact is not None:
        raise ReconstructionPlanCompatibilityError(
            "modern-reference delivery rejects an unused broker artifact"
        )
    else:
        broker_ref = None

    resolved = _resolve_plan_inputs(
        feed_epoch_definition_path=feed_epoch_definition_path,
        observation_operator_path=observation_operator_path,
        market_context_corpus_path=market_context_corpus_path,
        cftc_positioning_corpus_path=cftc_positioning_corpus_path,
        benchmark_manifest_path=benchmark_manifest_path,
        motif_manifest_path=motif_manifest_path,
        motif_index_path=motif_index_path,
        motif_qualification_path=motif_qualification_path,
        motif_leakage_audit_path=motif_leakage_audit_path,
        symbols=selected_symbols,
    )
    all_common_periods = _common_source_periods(
        resolved.feed_epoch_definition, selected_symbols
    )
    if (requested_start_ns is None) != (requested_end_ns is None):
        raise ReconstructionPlanCompatibilityError(
            "requested_start_ns and requested_end_ns must be supplied together"
        )
    if requested_start_ns is None:
        first_period = _period(start_period or all_common_periods[0])
        last_period = _period(end_period or all_common_periods[-1])
        requested_start = _month_start_ns(first_period)
        requested_end = _month_start_ns(_next_period(last_period))
    else:
        requested_start = _int64(requested_start_ns, "requested_start_ns")
        requested_end = _int64(requested_end_ns, "requested_end_ns")
        if requested_end <= requested_start:
            raise ReconstructionPlanCompatibilityError(
                "requested nanosecond interval is empty"
            )
        first_period = _period_for_ns(requested_start)
        last_period = _period_for_ns(requested_end - 1)
        if start_period is not None and _period(start_period) != first_period:
            raise ReconstructionPlanCompatibilityError(
                "start_period differs from requested_start_ns"
            )
        if end_period is not None and _period(end_period) != last_period:
            raise ReconstructionPlanCompatibilityError(
                "end_period differs from requested_end_ns"
            )
    if first_period > last_period:
        raise ReconstructionPlanCompatibilityError(
            "start_period follows end_period"
        )
    selected_periods = tuple(
        value
        for value in all_common_periods
        if first_period <= value <= last_period
    )
    expected_periods = _period_range(first_period, last_period)
    if selected_periods != expected_periods:
        raise ReconstructionPlanCompatibilityError(
            "requested source periods are not a complete common triangle range"
        )
    _reject_ex_ante_artifact_leakage(
        mode=mode,
        requested_start_ns=requested_start,
        definition=resolved.feed_epoch_definition,
        motif_profile=resolved.motif_profile,
    )
    inventory = _build_source_inventory(
        source,
        definition=resolved.feed_epoch_definition,
        symbols=selected_symbols,
        periods=selected_periods,
        requested_start_ns=requested_start,
        requested_end_ns=requested_end,
    )
    artifacts_dir = roots["artifact"]
    inventory_ref = _write_contract_artifact(
        inventory,
        artifacts_dir,
        prefix="reconstruction-source-inventory",
        kind=SOURCE_INVENTORY_ARTIFACT_KIND,
        metadata={"inventory_id": inventory.inventory_id},
    )
    selected_evidence_policy = (
        evidence_policy or ReconstructionEvidencePolicyV1()
    )
    if selected_evidence_policy.supported_provider_ids != (
        CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
    ):
        raise ReconstructionPlanCompatibilityError(
            "the current reconstruction evidence policy supports only HistData.com"
        )
    evidence_policy_ref = _write_contract_artifact(
        selected_evidence_policy,
        artifacts_dir,
        prefix="reconstruction-evidence-policy",
        kind=RECONSTRUCTION_EVIDENCE_POLICY_ARTIFACT_KIND,
        metadata={"policy_id": selected_evidence_policy.policy_id},
    )
    selected_cross_series_policy = (
        cross_series_constraint_policy or CrossSeriesConstraintPolicyV1()
    )
    if selected_cross_series_policy.supported_provider_ids != (
        CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
    ):
        raise ReconstructionPlanCompatibilityError(
            "the current cross-series evidence adapter supports only HistData.com"
        )
    if selected_cross_series_policy.required_symbols != selected_symbols:
        raise ReconstructionPlanCompatibilityError(
            "cross-series evidence policy does not cover the complete triangle"
        )
    cross_series_policy_ref = _write_contract_artifact(
        selected_cross_series_policy,
        artifacts_dir,
        prefix="cross-series-constraint-policy",
        kind=CROSS_SERIES_CONSTRAINT_POLICY_ARTIFACT_KIND,
        metadata={"policy_id": selected_cross_series_policy.policy_id},
    )

    selected_storage = storage_policy or ReconstructionStoragePolicyV1()
    selected_ensemble = ensemble_config or EnsembleCalibrationConfigV1()
    selected_generator = generator_config or EmpiricalMotifGeneratorConfigV1()
    selected_cross = (
        cross_currency_config or eurusd_triangle_reconciliation_config()
    )
    if tuple(selected_cross.symbols) != selected_symbols:
        raise ReconstructionPlanCompatibilityError(
            "cross-currency policy does not cover the complete triangle"
        )
    selected_carving = carving_constraints or HistoricalCarvingConstraintSetV1(
        fingerprint_constraint_id=(
            "modern-reference:" + str(resolved.motif_manifest["library_id"])
        ),
        # The qualified first-party corpus intentionally declares its static
        # holiday calendar advisory.  Coverage preflight still fails closed
        # for unsupported currencies/events; this permits that explicit,
        # non-exchange-specific limitation instead of rejecting every modern
        # reference candidate at the handler seam.
        require_complete_calendar_profile=False,
        require_fingerprint_validation=False,
    )
    selected_left_halo = max(
        _nonnegative_int(left_halo_ns or 0, "left_halo_ns"),
        resolved.observation_operator.required_left_halo_ns,
    )
    selected_right = _nonnegative_int(right_lookahead_ns, "right_lookahead_ns")
    information_policy = ReconstructionInformationPolicyV1(
        information_mode=mode,
        max_allowed_lookahead_ns=selected_right,
    )
    configuration = ReconstructionPlanConfigurationV1(
        delivery_mode=delivery,
        information_policy=information_policy,
        generator_config=selected_generator,
        carving_constraints=selected_carving,
        cross_currency_config=selected_cross,
        ensemble_config=selected_ensemble,
        storage_policy=selected_storage,
        window_size_ns=window_size_ns,
        left_halo_ns=selected_left_halo,
        right_lookahead_ns=selected_right,
        max_parallel_windows=max_parallel_windows,
    )
    configuration_ref = _write_contract_artifact(
        configuration,
        artifacts_dir,
        prefix="reconstruction-plan-configuration",
        kind=PLAN_CONFIGURATION_ARTIFACT_KIND,
        metadata={"configuration_id": configuration.configuration_id},
    )

    configuration_hashes = {
        configuration.configuration_id: configuration_ref.sha256,
        information_policy.policy_id: _contract_sha256(information_policy),
        selected_generator.config_id: _contract_sha256(selected_generator),
        selected_carving.constraint_set_id: _contract_sha256(selected_carving),
        selected_cross.config_id: _contract_sha256(selected_cross),
        selected_evidence_policy.policy_id: evidence_policy_ref.sha256,
        selected_cross_series_policy.policy_id: (
            cross_series_policy_ref.sha256
        ),
        resolved.feed_epoch_definition.definition_id: resolved.artifacts[
            "feed_epochs"
        ].sha256,
        resolved.observation_operator.operator_id: resolved.artifacts[
            "observation_operator"
        ].sha256,
        resolved.market_context.corpus_id: resolved.artifacts[
            "market_context"
        ].sha256,
        resolved.cftc_positioning.corpus_id: resolved.artifacts[
            "cftc_positioning"
        ].sha256,
        resolved.benchmark_corpus.corpus_id: resolved.artifacts[
            "benchmark_manifest"
        ].sha256,
        str(resolved.motif_manifest["library_id"]): resolved.artifacts[
            "motif_manifest"
        ].sha256,
        resolved.motif_index.index_id: resolved.artifacts["motif_index"].sha256,
    }
    ensemble_plan = plan_reconstruction_ensemble(
        symbols=selected_symbols,
        source_artifact_hashes={inventory.inventory_id: inventory_ref.sha256},
        configuration_artifact_hashes=configuration_hashes,
        base_seed=base_seed,
        config=selected_ensemble,
        storage_policy=selected_storage,
    )
    ensemble_ref = _write_contract_artifact(
        ensemble_plan,
        artifacts_dir,
        prefix="reconstruction-ensemble-plan",
        kind="reconstruction_ensemble_plan_v1",
        metadata={
            "plan_id": ensemble_plan.plan_id,
            "run_id": ensemble_plan.run.run_id,
        },
    )

    coverages = _source_coverages(inventory)
    member_window_plans = tuple(
        plan_cross_currency_windows(
            ensemble_plan.run,
            ensemble_member_id=member.member_id,
            requested_start_ns=requested_start,
            requested_end_ns=requested_end,
            window_size_ns=configuration.window_size_ns,
            coverages=coverages,
            left_halo_ns=configuration.left_halo_ns,
            right_lookahead_ns=configuration.right_lookahead_ns,
        )
        for member in ensemble_plan.members
    )
    if any(
        item.status is not CrossCurrencyWindowPlanStatus.PLANNED
        for item in member_window_plans
    ):
        raise ReconstructionPlanCompatibilityError(
            "complete synchronized cross-currency coverage could not be planned"
        )
    planned_windows = tuple(
        window for plan in member_window_plans for window in plan.windows
    )
    boundary_windows = member_window_plans[0].windows
    refusals, executable_boundaries = _preflight_window_support(
        boundary_windows,
        definition=resolved.feed_epoch_definition,
        context=resolved.market_context,
        positioning=resolved.cftc_positioning,
        mode=mode,
    )
    executable_keys = {
        (item.core_start_ns, item.core_end_ns) for item in executable_boundaries
    }

    information_manifest, information_audit = _build_information_evidence(
        run=ensemble_plan.run,
        policy=information_policy,
        windows=planned_windows,
        artifacts={
            **resolved.artifacts,
            "source_inventory": inventory_ref,
            "configuration": configuration_ref,
            "evidence_policy": evidence_policy_ref,
            "cross_series_constraint_policy": cross_series_policy_ref,
        },
        motif_profile=resolved.motif_profile,
        requested_start_ns=requested_start,
        requested_end_ns=requested_end,
    )
    information_manifest_ref = _write_contract_artifact(
        information_manifest,
        artifacts_dir,
        prefix="reconstruction-information-manifest",
        kind="reconstruction_information_manifest_v1",
        metadata={"manifest_id": information_manifest.manifest_id},
    )
    information_audit_ref = _write_contract_artifact(
        information_audit,
        artifacts_dir,
        prefix="reconstruction-information-audit",
        kind="reconstruction_information_audit_v1",
        metadata={"audit_id": information_audit.audit_id},
    )

    estimates_by_boundary = {
        (window.core_start_ns, window.core_end_ns): _window_resource_estimate(
            window,
            inventory=inventory,
            configuration=configuration,
        )
        for window in executable_boundaries
    }
    candidate_events_per_member = sum(
        item.candidate_event_count for item in estimates_by_boundary.values()
    )
    input_events_per_member = sum(
        item.input_event_count for item in estimates_by_boundary.values()
    )
    retained_ids = tuple(
        item.member_id
        for item in ensemble_plan.members[
            : selected_ensemble.retained_member_count
        ]
    )
    estimated_partition_count = (
        len(executable_boundaries) * len(retained_ids) * len(selected_symbols)
        if candidate_events_per_member
        else 0
    )
    retention_plan = estimate_reconstruction_retention(
        run_id=ensemble_plan.run.run_id,
        primary_member_id=retained_ids[0],
        retained_member_event_counts={
            member_id: candidate_events_per_member for member_id in retained_ids
        },
        estimated_partition_count=estimated_partition_count,
        storage_policy=selected_storage,
        estimated_bytes_per_event=selected_ensemble.estimated_bytes_per_event,
    )
    retention_ref = _write_contract_artifact(
        retention_plan,
        artifacts_dir,
        prefix="reconstruction-retention-plan",
        kind="reconstruction_retention_plan_v1",
        metadata={"plan_id": retention_plan.plan_id},
    )

    graph: dict[str, ArtifactRef] = {
        **resolved.artifacts,
        "source_inventory": inventory_ref,
        "configuration": configuration_ref,
        "evidence_policy": evidence_policy_ref,
        "cross_series_constraint_policy": cross_series_policy_ref,
        "information_manifest": information_manifest_ref,
        "information_audit": information_audit_ref,
        "ensemble_plan": ensemble_ref,
        "retention_plan": retention_ref,
    }
    if broker_ref is not None:
        graph["broker_delivery"] = broker_ref
    execution_manifest = ReconstructionPlanExecutionManifestV1(
        run_id=ensemble_plan.run.run_id,
        configuration_id=configuration.configuration_id,
        source_inventory_id=inventory.inventory_id,
        information_manifest_id=information_manifest.manifest_id,
        information_audit_id=information_audit.audit_id,
        ensemble_plan_id=ensemble_plan.plan_id,
        retention_plan_id=retention_plan.plan_id,
        delivery_mode=delivery,
        artifacts=graph,
        output_root=str(roots["output"]),
        checkpoint_root=str(roots["checkpoint"]),
        scratch_root=str(roots["scratch"]),
        planned_window_count=len(boundary_windows),
        executable_window_count=len(executable_boundaries),
        refusal_ids=tuple(item.refusal_id for item in refusals),
    )
    execution_ref = _write_contract_artifact(
        execution_manifest,
        artifacts_dir,
        prefix="reconstruction-plan-execution",
        kind=PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND,
        metadata={"manifest_id": execution_manifest.manifest_id},
    )
    graph["execution_manifest"] = execution_ref

    workflows = _build_workflow_requests(
        run=ensemble_plan.run,
        member_window_plans=member_window_plans,
        executable_keys=executable_keys,
        estimates_by_boundary=estimates_by_boundary,
        inventory=inventory,
        execution_manifest=execution_manifest,
        execution_ref=execution_ref,
        max_windows_per_request=max_windows_per_request,
    )
    maximum_estimate = max(
        estimates_by_boundary.values(),
        key=lambda item: item.estimated_memory_bytes,
        default=None,
    )
    total_candidate_events = candidate_events_per_member * len(
        ensemble_plan.members
    )
    resources = ReconstructionPlanResourceSummaryV1(
        source_event_count=inventory.total_row_count,
        source_size_bytes=inventory.total_size_bytes,
        planned_window_count=len(boundary_windows),
        executable_window_count=len(executable_boundaries),
        refused_window_count=len(refusals),
        ensemble_member_count=len(ensemble_plan.members),
        retained_member_count=len(retained_ids),
        workflow_request_count=len(workflows),
        estimated_input_event_count=(
            input_events_per_member * len(ensemble_plan.members)
        ),
        estimated_candidate_event_count=total_candidate_events,
        estimated_candidate_bytes=(
            total_candidate_events * selected_ensemble.estimated_bytes_per_event
        ),
        estimated_peak_memory_bytes=(
            (maximum_estimate.estimated_memory_bytes if maximum_estimate else 0)
            * configuration.max_parallel_windows
        ),
        estimated_peak_scratch_bytes=(
            (
                maximum_estimate.estimated_scratch_bytes
                if maximum_estimate
                else 0
            )
            * configuration.max_parallel_windows
        ),
        estimated_output_bytes=retention_plan.estimated_total_output_bytes,
        estimated_partition_count=estimated_partition_count,
    )
    plan = SyntheticInfillPlanV1(
        run=ensemble_plan.run,
        configuration_id=configuration.configuration_id,
        execution_manifest_id=execution_manifest.manifest_id,
        information_mode=mode,
        delivery_mode=delivery,
        requested_start_ns=requested_start,
        requested_end_ns=requested_end,
        workflow_requests=workflows,
        artifact_graph=graph,
        resources=resources,
        refusals=refusals,
    )
    validate_synthetic_infill_plan_for_execution(plan, verify_artifacts=False)
    return plan


def validate_synthetic_infill_plan_for_execution(
    plan: SyntheticInfillPlanV1,
    *,
    verify_artifacts: bool = True,
    verify_source_partitions: bool = True,
) -> SyntheticInfillPlanV1:
    """Validate the exact public plan shape consumed by first-party handlers."""
    if not isinstance(plan, SyntheticInfillPlanV1):
        raise TypeError("execution validation requires SyntheticInfillPlanV1")
    execution_ref = plan.artifact_graph["execution_manifest"]
    configuration_ref = plan.artifact_graph["configuration"]
    inventory_ref = plan.artifact_graph["source_inventory"]
    verified_refs: set[tuple[str, str, int | None, str]] = set()

    def verify_once(ref: ArtifactRef) -> None:
        key = (ref.kind, ref.path, ref.size_bytes, ref.sha256)
        if key not in verified_refs:
            verify_artifact_ref(ref)
            verified_refs.add(key)

    if verify_artifacts:
        for ref in plan.artifact_graph.values():
            verify_once(ref)
    execution = read_reconstruction_plan_execution_manifest(execution_ref.path)
    configuration = read_reconstruction_plan_configuration(
        configuration_ref.path
    )
    inventory = read_reconstruction_source_inventory(inventory_ref.path)
    if execution.manifest_id != plan.execution_manifest_id:
        raise ReconstructionPlanCompatibilityError(
            "plan execution manifest identity differs"
        )
    if configuration.configuration_id != plan.configuration_id:
        raise ReconstructionPlanCompatibilityError(
            "plan configuration identity differs"
        )
    if inventory.inventory_id != plan.run.source_version_ids[0]:
        raise ReconstructionPlanCompatibilityError(
            "plan source inventory is not run-bound"
        )
    if execution.artifacts != {
        name: ref
        for name, ref in plan.artifact_graph.items()
        if name != "execution_manifest"
    }:
        raise ReconstructionPlanCompatibilityError(
            "plan artifact graph differs from execution manifest"
        )
    for request in plan.workflow_requests:
        for task in request.tasks:
            for command in task.commands:
                ReconstructionStagePlanV1(
                    command=command,
                    execution_manifest_ref=execution_ref,
                    execution_manifest=execution,
                    configuration=configuration,
                    source_inventory=inventory,
                )
                if verify_artifacts:
                    for ref in command.input_manifest_refs:
                        if (
                            ref.kind == ASCII_TICK_SOURCE_KIND
                            and not verify_source_partitions
                        ):
                            continue
                        verify_once(ref)
    return plan


def write_synthetic_infill_plan(
    plan: SyntheticInfillPlanV1, root: str | Path
) -> ArtifactRef:
    """Persist one content-addressed top-level plan artifact."""
    validate_synthetic_infill_plan_for_execution(plan, verify_artifacts=False)
    return _write_contract_artifact(
        plan,
        Path(root).expanduser().resolve(),
        prefix="synthetic-infill-plan",
        kind=SYNTHETIC_INFILL_PLAN_ARTIFACT_KIND,
        metadata={"plan_id": plan.plan_id, "run_id": plan.run.run_id},
    )


def read_synthetic_infill_plan(path: str | Path) -> SyntheticInfillPlanV1:
    """Hash-verify and restore a content-addressed top-level plan."""
    payload = _read_content_addressed_json(path, "synthetic-infill-plan")
    return SyntheticInfillPlanV1.from_dict(payload)


def read_reconstruction_source_inventory(
    path: str | Path,
) -> ReconstructionSourceInventoryV1:
    payload = _read_content_addressed_json(
        path, "reconstruction-source-inventory"
    )
    return ReconstructionSourceInventoryV1.from_dict(payload)


def read_reconstruction_plan_configuration(
    path: str | Path,
) -> ReconstructionPlanConfigurationV1:
    payload = _read_content_addressed_json(
        path, "reconstruction-plan-configuration"
    )
    return ReconstructionPlanConfigurationV1.from_dict(payload)


def read_reconstruction_plan_execution_manifest(
    path: str | Path,
) -> ReconstructionPlanExecutionManifestV1:
    payload = _read_content_addressed_json(
        path, "reconstruction-plan-execution"
    )
    return ReconstructionPlanExecutionManifestV1.from_dict(payload)


def load_reconstruction_stage_plan(
    command: ReconstructionStageCommandV1,
    *,
    verify_artifacts: bool = True,
) -> ReconstructionStagePlanV1:
    """Resolve and validate the public artifact context for one stage command."""
    if not isinstance(command, ReconstructionStageCommandV1):
        raise TypeError("stage planning requires ReconstructionStageCommandV1")
    if len(command.configuration_refs) != 1:
        raise ReconstructionPlanCompatibilityError(
            "stage command requires exactly one execution manifest"
        )
    execution_ref = _strong_ref(command.configuration_refs[0])
    if verify_artifacts:
        verify_artifact_ref(execution_ref)
    execution = read_reconstruction_plan_execution_manifest(execution_ref.path)
    configuration_ref = execution.artifacts["configuration"]
    inventory_ref = execution.artifacts["source_inventory"]
    if verify_artifacts:
        for ref in execution.artifacts.values():
            verify_artifact_ref(ref)
        for ref in command.input_manifest_refs:
            verify_artifact_ref(ref)
    return ReconstructionStagePlanV1(
        command=command,
        execution_manifest_ref=execution_ref,
        execution_manifest=execution,
        configuration=read_reconstruction_plan_configuration(
            configuration_ref.path
        ),
        source_inventory=read_reconstruction_source_inventory(
            inventory_ref.path
        ),
    )


def _resolve_plan_inputs(
    *,
    feed_epoch_definition_path: str | Path,
    observation_operator_path: str | Path,
    market_context_corpus_path: str | Path,
    cftc_positioning_corpus_path: str | Path,
    benchmark_manifest_path: str | Path,
    motif_manifest_path: str | Path,
    motif_index_path: str | Path,
    motif_qualification_path: str | Path,
    motif_leakage_audit_path: str | Path,
    symbols: tuple[str, ...],
) -> _ResolvedPlanInputs:
    """Resolve qualified inputs once per unchanged stat-identity set."""
    identities = tuple(
        _file_stat_identity(path)
        for path in (
            feed_epoch_definition_path,
            observation_operator_path,
            market_context_corpus_path,
            cftc_positioning_corpus_path,
            benchmark_manifest_path,
            motif_manifest_path,
            motif_index_path,
            motif_qualification_path,
            motif_leakage_audit_path,
        )
    )
    return _resolve_plan_inputs_for_identity(identities, symbols)


@lru_cache(maxsize=4)
def _resolve_plan_inputs_for_identity(
    identities: tuple[tuple[str, int, int, int, int, int], ...],
    symbols: tuple[str, ...],
) -> _ResolvedPlanInputs:
    paths = tuple(item[0] for item in identities)
    return _resolve_plan_inputs_uncached(
        feed_epoch_definition_path=paths[0],
        observation_operator_path=paths[1],
        market_context_corpus_path=paths[2],
        cftc_positioning_corpus_path=paths[3],
        benchmark_manifest_path=paths[4],
        motif_manifest_path=paths[5],
        motif_index_path=paths[6],
        motif_qualification_path=paths[7],
        motif_leakage_audit_path=paths[8],
        symbols=symbols,
    )


def _resolve_plan_inputs_uncached(
    *,
    feed_epoch_definition_path: str | Path,
    observation_operator_path: str | Path,
    market_context_corpus_path: str | Path,
    cftc_positioning_corpus_path: str | Path,
    benchmark_manifest_path: str | Path,
    motif_manifest_path: str | Path,
    motif_index_path: str | Path,
    motif_qualification_path: str | Path,
    motif_leakage_audit_path: str | Path,
    symbols: tuple[str, ...],
) -> _ResolvedPlanInputs:
    feed_ref = artifact_ref_for_file(
        feed_epoch_definition_path, kind="feed_epoch_definition_v2"
    )
    definition = read_active_time_feed_epoch_definition(feed_ref.path)
    if not definition.valid_for_observation_models:
        raise ReconstructionPlanCompatibilityError(
            "feed epoch definition is unstable"
        )
    if _symbols(definition.symbols) != symbols:
        raise ReconstructionPlanCompatibilityError(
            "feed epoch definition symbols differ from requested triangle"
        )

    observation_path = Path(observation_operator_path).expanduser().resolve()
    observation_payload = _json_mapping(
        observation_path.read_text(encoding="utf-8")
    )
    observation_ref = artifact_ref_for_file(
        observation_path,
        kind="observation-operator",
        metadata={
            "schema_version": str(
                observation_payload.get("schema_version", "")
            ),
            "operator_id": str(observation_payload.get("operator_id", "")),
            "feed_epoch_definition_id": str(
                observation_payload.get("feed_epoch_definition_id", "")
            ),
        },
    )
    observation = read_observation_operator_artifact(observation_ref)
    if not observation.valid_for_application:
        raise ReconstructionPlanCompatibilityError(
            "observation operator is unqualified"
        )
    if observation.feed_epoch_definition_id != definition.definition_id:
        raise ReconstructionPlanCompatibilityError(
            "observation operator feed epoch definition differs"
        )

    context_ref = artifact_ref_for_file(
        market_context_corpus_path, kind="market_context_corpus_v1"
    )
    context = read_market_context_corpus(context_ref.path)
    positioning_ref = artifact_ref_for_file(
        cftc_positioning_corpus_path, kind="cftc_positioning_corpus_v1"
    )
    positioning = read_cftc_positioning_corpus(positioning_ref.path)
    benchmark_ref = artifact_ref_for_file(
        benchmark_manifest_path, kind="reverse_degradation_manifest_v1"
    )
    benchmark = read_reverse_degradation_benchmark_corpus(benchmark_ref.path)
    gate_policy = load_default_benchmark_promotion_gate_policy()
    compatibility = {
        "feed_epoch_definition_id": definition.definition_id,
        "observation_operator_id": observation.operator_id,
        "market_context_corpus_id": context.corpus_id,
        "cftc_positioning_corpus_id": positioning.corpus_id,
        "gate_policy_id": gate_policy.policy_id,
        "gate_policy_commit": PREDECLARED_GATE_COMMIT,
    }
    for name, expected in compatibility.items():
        if getattr(benchmark, name) != expected:
            raise ReconstructionPlanCompatibilityError(
                f"benchmark {name} differs from the resolved qualified artifact"
            )

    motif_manifest_ref = artifact_ref_for_file(
        motif_manifest_path, kind="modern_reference_motif_manifest_v1"
    )
    motif_manifest = read_modern_reference_motif_artifact(
        motif_manifest_ref.path, kind="manifest"
    )
    motif_index_ref = artifact_ref_for_file(
        motif_index_path, kind="modern_reference_motif_index_v1"
    )
    motif_index = read_modern_reference_motif_index(motif_index_ref.path)
    profile = ModernReferenceMotifProfileV1.from_dict(
        _mapping(motif_manifest.get("profile"))
    )
    if _symbols(profile.symbols) != symbols:
        raise ReconstructionPlanCompatibilityError(
            "motif profile symbols differ from requested triangle"
        )
    index_artifact = ArtifactRef.from_dict(
        _mapping(motif_manifest.get("index_artifact"))
    )
    if (
        index_artifact.sha256 != motif_index_ref.sha256
        or index_artifact.size_bytes != motif_index_ref.size_bytes
        or index_artifact.metadata.get("index_id") != motif_index.index_id
    ):
        raise ReconstructionPlanCompatibilityError(
            "motif index binding differs"
        )
    stable_epoch = _mapping(motif_manifest.get("stable_feed_epoch"))
    if not any(
        epoch.epoch_id == stable_epoch.get("epoch_id")
        and epoch.label == stable_epoch.get("label")
        for epoch in definition.epochs
    ):
        raise ReconstructionPlanCompatibilityError(
            "motif stable epoch differs from feed epoch definition"
        )
    dependencies = _mapping(motif_manifest.get("dependencies"))
    expected_dependencies = {
        "benchmark_manifest": benchmark_ref,
        "feed_epoch_definition": feed_ref,
        "market_context_corpus": context_ref,
        "cftc_positioning_corpus": positioning_ref,
    }
    for name, expected_ref in expected_dependencies.items():
        resolved_ref = ArtifactRef.from_dict(_mapping(dependencies.get(name)))
        if (
            resolved_ref.sha256 != expected_ref.sha256
            or resolved_ref.size_bytes != expected_ref.size_bytes
        ):
            raise ReconstructionPlanCompatibilityError(
                f"motif dependency {name} differs from resolved artifact"
            )

    qualification_ref = artifact_ref_for_file(
        motif_qualification_path, kind="modern_reference_motif_qualification_v1"
    )
    qualification = read_modern_reference_motif_artifact(
        qualification_ref.path, kind="qualification"
    )
    campaign = _mapping(qualification.get("campaign"))
    campaign_gate = _mapping(campaign.get("campaign_gate_decision"))
    if (
        qualification.get("library_id") != motif_manifest.get("library_id")
        or qualification.get("candidate_promotion_eligible") is not True
        or qualification.get("candidate_provisional") is not False
        or qualification.get("frozen_gate_policy_commit")
        != PREDECLARED_GATE_COMMIT
        or campaign.get("corpus_id") != benchmark.corpus_id
        or campaign.get("source_replay_verified") is not True
        or campaign_gate.get("promotion_eligible") is not True
    ):
        raise ReconstructionPlanCompatibilityError(
            "modern reference motif qualification is missing or stale"
        )
    leakage_ref = artifact_ref_for_file(
        motif_leakage_audit_path,
        kind="modern_reference_motif_leakage_audit_v1",
    )
    leakage = read_modern_reference_motif_artifact(
        leakage_ref.path, kind="leakage-audit"
    )
    if (
        leakage.get("library_id") != motif_manifest.get("library_id")
        or tuple(leakage.get("indexed_splits", ())) != ("train",)
        or leakage.get("post_exclusion_cross_split_finding_count") != 0
        or leakage.get("retained_holdout_fragment_count") != 0
        or leakage.get("retained_nontrain_fragment_count") != 0
    ):
        raise ReconstructionPlanCompatibilityError(
            "modern reference motif leakage audit failed"
        )
    return _ResolvedPlanInputs(
        feed_epoch_definition=definition,
        observation_operator=observation,
        market_context=context,
        cftc_positioning=positioning,
        benchmark_corpus=benchmark,
        motif_profile=profile,
        motif_index=motif_index,
        artifacts={
            "feed_epochs": feed_ref,
            "observation_operator": observation_ref,
            "market_context": context_ref,
            "cftc_positioning": positioning_ref,
            "benchmark_manifest": benchmark_ref,
            "motif_manifest": motif_manifest_ref,
            "motif_index": motif_index_ref,
            "motif_qualification": qualification_ref,
            "motif_leakage_audit": leakage_ref,
        },
        motif_manifest=motif_manifest,
        motif_qualification=qualification,
        motif_leakage_audit=leakage,
    )


def _file_stat_identity(
    path: str | Path,
) -> tuple[str, int, int, int, int, int]:
    target = Path(path).expanduser().resolve()
    stat = target.stat()
    return (
        str(target),
        stat.st_dev,
        stat.st_ino,
        stat.st_size,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
    )


def _build_source_inventory(
    source_root: Path,
    *,
    definition: Any,
    symbols: tuple[str, ...],
    periods: tuple[str, ...],
    requested_start_ns: int,
    requested_end_ns: int,
) -> ReconstructionSourceInventoryV1:
    adapter = HistDataProviderAdapter()
    lineage = {
        (
            _period(str(item.get("period", ""))),
            _symbol(str(item.get("symbol", ""))),
        ): item
        for item in _sequence(_mapping(definition.lineage).get("sources"))
    }
    partitions: list[ReconstructionSourcePartitionV1] = []
    for period in periods:
        for symbol in symbols:
            evidence = lineage.get((period, symbol))
            if evidence is None:
                raise ReconstructionPlanCompatibilityError(
                    f"feed epoch lineage omits {symbol} {period}"
                )
            expected_hash = str(
                evidence.get("source_artifact_sha256", "")
            ).removeprefix("sha256:")
            if not _SHA256_RE.fullmatch(expected_hash):
                raise ReconstructionPlanCompatibilityError(
                    f"feed epoch lineage hash is invalid for {symbol} {period}"
                )
            try:
                provider_partition = adapter.inspect_partition(
                    source_root,
                    symbol=symbol,
                    period=period,
                    expected_sha256=expected_hash,
                )
            except DatasetContractError as err:
                if err.code is DatasetFailureCode.ARTIFACT_HASH_MISMATCH:
                    message = f"source hash differs for {symbol} {period}"
                else:
                    message = (
                        f"HistData adapter rejected {symbol} {period}: {err}"
                    )
                raise ReconstructionPlanCompatibilityError(message) from err
            path = Path(provider_partition.artifact.path)
            actual_hash = provider_partition.artifact.sha256
            rows = provider_partition.row_count
            first_ms = _strict_int(
                provider_partition.artifact.metadata.get("first_timestamp_ms"),
                "first_timestamp_ms",
            )
            last_ms = _strict_int(
                provider_partition.artifact.metadata.get("last_timestamp_ms"),
                "last_timestamp_ms",
            )
            ref = ArtifactRef(
                kind=ASCII_TICK_SOURCE_KIND,
                path=str(path.resolve()),
                size_bytes=path.stat().st_size,
                sha256=actual_hash,
                metadata={
                    "symbol": symbol,
                    "period": period,
                    "row_count": rows,
                    "row_identity_basis": "zero-based-arrow-row-ordinal-v1",
                },
            )
            partitions.append(
                ReconstructionSourcePartitionV1(
                    symbol=symbol,
                    period=period,
                    artifact=ref,
                    row_count=rows,
                    coverage_start_ns=_month_start_ns(period),
                    coverage_end_ns=_month_start_ns(_next_period(period)),
                    first_timestamp_ms=first_ms,
                    last_timestamp_ms=last_ms,
                    feed_epoch_evidence_id=str(evidence.get("evidence_id", "")),
                )
            )
    return ReconstructionSourceInventoryV1(
        source_root=str(source_root),
        symbols=symbols,
        periods=periods,
        partitions=tuple(partitions),
        requested_start_ns=requested_start_ns,
        requested_end_ns=requested_end_ns,
        total_row_count=sum(item.row_count for item in partitions),
        total_size_bytes=sum(
            cast(int, item.artifact.size_bytes) for item in partitions
        ),
    )


def _preflight_window_support(
    windows: Sequence[ReconstructionWindowV1],
    *,
    definition: Any,
    context: MarketContextCorpusV1,
    positioning: CftcPositioningCorpusV1,
    mode: InformationMode,
) -> tuple[
    tuple[ReconstructionPlanRefusalV1, ...], tuple[ReconstructionWindowV1, ...]
]:
    refusals: list[ReconstructionPlanRefusalV1] = []
    executable: list[ReconstructionWindowV1] = []
    required_context = (
        ("EUR", MarketContextKind.POLICY_RATE_CHANGE),
        ("GBP", MarketContextKind.POLICY_RATE_CHANGE),
        ("USD", MarketContextKind.CENTRAL_BANK_DECISION),
    )
    for window in windows:
        midpoint_ms = (
            (window.core_start_ns + window.core_end_ns) // 2
        ) // 1_000_000
        assignments = [
            definition.assign(symbol=symbol, timestamp_utc_ms=midpoint_ms)
            for symbol in window.symbols
        ]
        if any(item.assignment_kind == "out_of_scope" for item in assignments):
            refusals.append(
                ReconstructionPlanRefusalV1(
                    start_ns=window.core_start_ns,
                    end_ns=window.core_end_ns,
                    code=ReconstructionPlanRefusalCode.FEED_EPOCH_UNSUPPORTED,
                    reason="window midpoint lies outside qualified feed epoch coverage",
                )
            )
            continue
        context_reasons: list[str] = []
        for currency, kind in required_context:
            decision = preflight_market_context_corpus(
                context,
                start_ns=window.core_start_ns,
                end_ns=window.core_end_ns,
                currencies=(currency,),
                kinds=(kind,),
            )
            context_reasons.extend(decision.reasons)
        if context_reasons:
            refusals.append(
                ReconstructionPlanRefusalV1(
                    start_ns=window.core_start_ns,
                    end_ns=window.core_end_ns,
                    code=ReconstructionPlanRefusalCode.MARKET_CONTEXT_UNSUPPORTED,
                    reason="; ".join(sorted(set(context_reasons))),
                )
            )
            continue
        positioning_decision = preflight_cftc_positioning_corpus(
            positioning,
            start_ns=window.core_start_ns,
            end_ns=window.core_end_ns,
            information_mode=mode,
            as_of_ns=(
                window.core_start_ns
                if mode is InformationMode.EX_ANTE_SIMULATION
                else None
            ),
            symbols=window.symbols,
            report_families=(CftcReportFamily.LEGACY,),
            report_scopes=(CftcReportScope.FUTURES_ONLY,),
        )
        if not positioning_decision.ready:
            refusals.append(
                ReconstructionPlanRefusalV1(
                    start_ns=window.core_start_ns,
                    end_ns=window.core_end_ns,
                    code=ReconstructionPlanRefusalCode.CFTC_POSITIONING_UNSUPPORTED,
                    reason="; ".join(positioning_decision.reasons),
                )
            )
            continue
        executable.append(window)
    return tuple(refusals), tuple(executable)


def _build_information_evidence(
    *,
    run: ReconstructionRunV1,
    policy: ReconstructionInformationPolicyV1,
    windows: Sequence[ReconstructionWindowV1],
    artifacts: Mapping[str, ArtifactRef],
    motif_profile: ModernReferenceMotifProfileV1,
    requested_start_ns: int,
    requested_end_ns: int,
) -> tuple[ReconstructionInformationManifestV1, InformationAuditReportV1]:
    splits = _information_splits(motif_profile)
    used_at = (
        max(requested_end_ns, max(split.end_ns for split in splits))
        if policy.information_mode is InformationMode.EX_POST_RECONSTRUCTION
        else requested_start_ns
    )
    train_end = splits[0].end_ns - 1
    inputs: list[ReconstructionInformationInputV1] = []
    declarations = (
        (
            "source_inventory",
            InformationStage.SOURCE,
            InformationScope.POINT_IN_TIME,
            requested_start_ns,
            None,
        ),
        (
            "evidence_policy",
            InformationStage.FEATURE,
            InformationScope.POINT_IN_TIME,
            requested_start_ns,
            None,
        ),
        (
            "cross_series_constraint_policy",
            InformationStage.FEATURE,
            InformationScope.POINT_IN_TIME,
            requested_start_ns,
            None,
        ),
        (
            "feed_epochs",
            InformationStage.FEATURE,
            (
                InformationScope.GLOBAL_NORMALIZATION
                if policy.information_mode
                is InformationMode.EX_POST_RECONSTRUCTION
                else InformationScope.POINT_IN_TIME
            ),
            min(train_end, used_at),
            None,
        ),
        (
            "observation_operator",
            InformationStage.FEATURE,
            InformationScope.POINT_IN_TIME,
            min(train_end, used_at),
            None,
        ),
        (
            "market_context",
            InformationStage.CALENDAR_CONTEXT,
            InformationScope.POINT_IN_TIME,
            min(requested_start_ns, used_at),
            None,
        ),
        (
            "cftc_positioning",
            InformationStage.FEATURE,
            InformationScope.POINT_IN_TIME,
            min(requested_start_ns, used_at),
            None,
        ),
        (
            "benchmark_manifest",
            InformationStage.FEATURE,
            InformationScope.POINT_IN_TIME,
            min(train_end, used_at),
            None,
        ),
        (
            "motif_index",
            InformationStage.MOTIF_SELECTION,
            InformationScope.EMPIRICAL_MOTIF,
            train_end,
            InformationSplitKind.TRAIN,
        ),
    )
    for role, stage, scope, event_time, split_kind in declarations:
        ref = artifacts[role]
        inputs.append(
            ReconstructionInformationInputV1(
                run_id=run.run_id,
                artifact_id=f"{ref.kind}:sha256:{ref.sha256}",
                information_mode=policy.information_mode,
                input_kind=InformationInputKind.EXTERNAL,
                stage=stage,
                scope=scope,
                event_time_ns=event_time,
                available_at_ns=event_time,
                used_at_ns=used_at,
                observation_start_ns=event_time,
                observation_end_ns=event_time,
                vintage_id=f"sha256:{ref.sha256}",
                reason=f"resolved reconstruction planning artifact: {role}",
                allowed_lookahead_ns=0,
                split_kind=split_kind,
            )
        )
    manifest = ReconstructionInformationManifestV1(
        run_id=run.run_id,
        policy_id=policy.policy_id,
        information_mode=policy.information_mode,
        window_plan_id=reconstruction_information_window_plan_id(windows),
        inputs=tuple(inputs),
        splits=splits,
    )
    audit = require_reconstruction_information_audit(
        manifest,
        policy,
        run=run,
        windows=windows,
    )
    return manifest, audit


def _window_resource_estimate(
    window: ReconstructionWindowV1,
    *,
    inventory: ReconstructionSourceInventoryV1,
    configuration: ReconstructionPlanConfigurationV1,
) -> ReconstructionResourceEstimateV1:
    partitions = inventory.partitions_for_window(window)
    input_count = sum(
        _estimated_window_partition_rows(window, item) for item in partitions
    )
    interval_count = max(0, input_count - len(inventory.symbols))
    generator_limit = (
        configuration.generator_config.max_events_per_interval * interval_count
    )
    amplification_limit = math.floor(
        input_count * configuration.storage_policy.max_candidate_amplification
    )
    candidates = min(generator_limit, amplification_limit)
    batch_limit = configuration.storage_policy.max_events_per_batch
    batch_count = interval_count
    inflight = min(
        batch_count, configuration.storage_policy.max_inflight_batches
    )
    peak = min(candidates, batch_limit)
    bytes_per_event = configuration.generator_config.estimated_bytes_per_event
    estimated_memory = (
        _RESOURCE_FIXED_OVERHEAD_BYTES
        + (input_count * bytes_per_event)
        + (peak * bytes_per_event * max(1, inflight))
    )
    ledger_bytes = batch_count * _RESOURCE_LEDGER_BYTES_PER_INTERVAL
    estimate = ReconstructionResourceEstimateV1(
        input_event_count=input_count,
        candidate_event_count=candidates,
        retained_ensemble_members=1,
        inflight_batches=inflight,
        peak_events_per_batch=peak,
        estimated_memory_bytes=estimated_memory,
        estimated_scratch_bytes=(candidates * bytes_per_event) + ledger_bytes,
        estimated_output_bytes=candidates * bytes_per_event,
        estimated_batch_count=batch_count,
    )
    configuration.storage_policy.preflight(estimate)
    return estimate


def _estimated_window_partition_rows(
    window: ReconstructionWindowV1,
    partition: ReconstructionSourcePartitionV1,
) -> int:
    """Conservatively scale monthly rows to a bounded execution window."""
    first_ns = partition.first_timestamp_ms * 1_000_000
    last_exclusive_ns = (partition.last_timestamp_ms + 1) * 1_000_000
    overlap_start = max(window.input_start_ns, first_ns)
    overlap_end = min(window.input_end_ns, last_exclusive_ns)
    if overlap_end <= overlap_start:
        return 0
    observed_span = max(1, last_exclusive_ns - first_ns)
    overlap_span = overlap_end - overlap_start
    proportional = math.ceil(partition.row_count * overlap_span / observed_span)
    return int(
        min(
            partition.row_count,
            max(2, proportional * _SOURCE_ROW_DENSITY_SAFETY_FACTOR),
        )
    )


def _build_workflow_requests(
    *,
    run: ReconstructionRunV1,
    member_window_plans: Sequence[Any],
    executable_keys: set[tuple[int, int]],
    estimates_by_boundary: Mapping[
        tuple[int, int], ReconstructionResourceEstimateV1
    ],
    inventory: ReconstructionSourceInventoryV1,
    execution_manifest: ReconstructionPlanExecutionManifestV1,
    execution_ref: ArtifactRef,
    max_windows_per_request: int,
) -> tuple[ReconstructionWorkflowRequestV1, ...]:
    limit = _positive_int(max_windows_per_request, "max_windows_per_request")
    if limit > 512:
        raise ValueError("max_windows_per_request exceeds orchestration limit")
    requests: list[ReconstructionWorkflowRequestV1] = []
    for member_ordinal, window_plan in enumerate(member_window_plans, start=1):
        tasks: list[ReconstructionWindowTaskV1] = []
        for window in window_plan.windows:
            key = (window.core_start_ns, window.core_end_ns)
            if key not in executable_keys:
                continue
            scratch = (
                Path(execution_manifest.scratch_root)
                / run.run_id.replace(":", "-")
                / window.ensemble_member_id.replace(":", "-")
                / window.window_id.replace(":", "-")
            )
            commands = _stage_commands(
                window,
                scratch=scratch,
                inventory=inventory,
                execution_manifest=execution_manifest,
                execution_ref=execution_ref,
            )
            tasks.append(
                ReconstructionWindowTaskV1(
                    window=window,
                    resource_estimate=estimates_by_boundary[key],
                    commands=commands,
                    scratch_directory=str(scratch),
                )
            )
        for chunk_ordinal, offset in enumerate(
            range(0, len(tasks), limit), start=1
        ):
            chunk = tuple(tasks[offset : offset + limit])
            request_id = (
                f"reconstruction-{run.run_id.rsplit(':', maxsplit=1)[-1][:16]}-"
                f"m{member_ordinal:02d}-c{chunk_ordinal:03d}"
            )
            requests.append(
                ReconstructionWorkflowRequestV1(
                    request_id=request_id,
                    run=run,
                    tasks=chunk,
                    manifest_store_root=str(
                        Path(execution_manifest.checkpoint_root) / "manifests"
                    ),
                    report_root=str(
                        Path(execution_manifest.output_root) / "reports"
                    ),
                    task_queues={
                        "orchestration": "histdatacom.reconstruction.orchestration",
                        "cpu_file": "histdatacom.reconstruction.cpu-file",
                    },
                    max_parallel_windows=configuration_parallelism(
                        execution_manifest
                    ),
                    max_inflight_memory_bytes=run.storage_policy.max_memory_bytes,
                )
            )
    return tuple(requests)


def configuration_parallelism(
    execution_manifest: ReconstructionPlanExecutionManifestV1,
) -> int:
    """Read the public configuration artifact for request construction."""
    configuration = read_reconstruction_plan_configuration(
        execution_manifest.artifacts["configuration"].path
    )
    return configuration.max_parallel_windows


def _stage_commands(
    window: ReconstructionWindowV1,
    *,
    scratch: Path,
    inventory: ReconstructionSourceInventoryV1,
    execution_manifest: ReconstructionPlanExecutionManifestV1,
    execution_ref: ArtifactRef,
) -> tuple[ReconstructionStageCommandV1, ...]:
    graph = execution_manifest.artifacts
    source_inputs = tuple(
        item.artifact for item in inventory.partitions_for_window(window)
    ) + (
        graph["feed_epochs"],
        graph["observation_operator"],
        graph["market_context"],
        graph["cftc_positioning"],
    )
    stage_inputs: Mapping[ReconstructionStage, tuple[ArtifactRef, ...]] = {
        ReconstructionStage.SOURCE_ENRICHMENT: source_inputs,
        ReconstructionStage.PROPOSAL: (
            graph["motif_manifest"],
            graph["motif_index"],
        ),
        ReconstructionStage.CARVING: (
            graph["market_context"],
            graph["cftc_positioning"],
        ),
        ReconstructionStage.CROSS_SERIES_RECONCILIATION: (),
        ReconstructionStage.BROKER_TRANSFER: (
            (graph["broker_delivery"],)
            if execution_manifest.delivery_mode
            is ReconstructionDeliveryMode.BROKER_CONDITIONED
            else ()
        ),
        ReconstructionStage.VALIDATION: (
            graph["benchmark_manifest"],
            graph["motif_qualification"],
            graph["motif_leakage_audit"],
            graph["information_audit"],
        ),
        ReconstructionStage.ATOMIC_PARTITION_COMMIT: (
            graph["source_inventory"],
            graph["retention_plan"],
        ),
    }
    commands: list[ReconstructionStageCommandV1] = []
    for stage in RECONSTRUCTION_STAGE_ORDER:
        inputs = stage_inputs[stage]
        if len(inputs) > MAX_STAGE_ARTIFACT_REFS:
            raise ReconstructionPlanCompatibilityError(
                f"{stage.value} input artifact count exceeds orchestration limit"
            )
        commands.append(
            ReconstructionStageCommandV1(
                stage=stage,
                handler_name=FIRST_PARTY_RECONSTRUCTION_HANDLERS[stage],
                receipt_path=str(scratch / "receipts" / f"{stage.value}.json"),
                input_manifest_refs=inputs,
                configuration_refs=(execution_ref,),
            )
        )
    return tuple(commands)


def _validate_stage_inputs(
    command: ReconstructionStageCommandV1,
    delivery_mode: ReconstructionDeliveryMode,
) -> None:
    kinds = {ref.kind for ref in command.input_manifest_refs}
    required: Mapping[ReconstructionStage, set[str]] = {
        ReconstructionStage.SOURCE_ENRICHMENT: {
            ASCII_TICK_SOURCE_KIND,
            "feed_epoch_definition_v2",
            "observation-operator",
            "market_context_corpus_v1",
            "cftc_positioning_corpus_v1",
        },
        ReconstructionStage.PROPOSAL: {
            "modern_reference_motif_manifest_v1",
            "modern_reference_motif_index_v1",
        },
        ReconstructionStage.CARVING: {
            "market_context_corpus_v1",
            "cftc_positioning_corpus_v1",
        },
        ReconstructionStage.CROSS_SERIES_RECONCILIATION: set(),
        ReconstructionStage.BROKER_TRANSFER: (
            {"broker_delivery_artifact_v1"}
            if delivery_mode is ReconstructionDeliveryMode.BROKER_CONDITIONED
            else set()
        ),
        ReconstructionStage.VALIDATION: {
            "reverse_degradation_manifest_v1",
            "modern_reference_motif_qualification_v1",
            "modern_reference_motif_leakage_audit_v1",
            "reconstruction_information_audit_v1",
        },
        ReconstructionStage.ATOMIC_PARTITION_COMMIT: {
            SOURCE_INVENTORY_ARTIFACT_KIND,
            "reconstruction_retention_plan_v1",
        },
    }
    if not required[command.stage].issubset(kinds):
        raise ReconstructionPlanCompatibilityError(
            f"{command.stage.value} command lacks required artifact kinds"
        )
    if (
        command.stage is ReconstructionStage.BROKER_TRANSFER
        and delivery_mode is ReconstructionDeliveryMode.MODERN_REFERENCE
        and command.input_manifest_refs
    ):
        raise ReconstructionPlanCompatibilityError(
            "modern-reference delivery command contains broker inputs"
        )


def _source_coverages(
    inventory: ReconstructionSourceInventoryV1,
) -> tuple[CrossCurrencySymbolCoverageV1, ...]:
    result: list[CrossCurrencySymbolCoverageV1] = []
    for symbol in inventory.symbols:
        selected = [
            item for item in inventory.partitions if item.symbol == symbol
        ]
        result.append(
            CrossCurrencySymbolCoverageV1(
                symbol=symbol,
                start_ns=min(item.coverage_start_ns for item in selected),
                end_ns=max(item.coverage_end_ns for item in selected),
                source_periods=tuple(item.period for item in selected),
            )
        )
    return tuple(result)


def _information_splits(
    profile: ModernReferenceMotifProfileV1,
) -> tuple[ReconstructionInformationSplitV1, ...]:
    train = profile.split_periods["train"]
    calibration = profile.split_periods["calibration"]
    validation = (
        *profile.split_periods["validation"],
        *profile.split_periods["final_holdout"],
    )
    return (
        ReconstructionInformationSplitV1(
            InformationSplitKind.TRAIN,
            _month_start_ns(train[0]),
            _month_start_ns(_next_period(train[-1])),
        ),
        ReconstructionInformationSplitV1(
            InformationSplitKind.CALIBRATION,
            _month_start_ns(calibration[0]),
            _month_start_ns(_next_period(calibration[-1])),
        ),
        ReconstructionInformationSplitV1(
            InformationSplitKind.VALIDATION,
            _month_start_ns(validation[0]),
            _month_start_ns(_next_period(validation[-1])),
        ),
    )


def _reject_ex_ante_artifact_leakage(
    *,
    mode: InformationMode,
    requested_start_ns: int,
    definition: Any,
    motif_profile: ModernReferenceMotifProfileV1,
) -> None:
    if mode is not InformationMode.EX_ANTE_SIMULATION:
        return
    latest_training_ns = max(
        definition.coverage_end_utc_ms * 1_000_000,
        _month_start_ns(_next_period(motif_profile.split_periods["train"][-1])),
    )
    if latest_training_ns >= requested_start_ns:
        raise ReconstructionPlanCompatibilityError(
            "ex-ante plan refused: fitted epoch/motif artifacts observe the requested future"
        )


def _common_source_periods(
    definition: Any, symbols: tuple[str, ...]
) -> tuple[str, ...]:
    by_symbol: dict[str, set[str]] = {symbol: set() for symbol in symbols}
    for item in _sequence(_mapping(definition.lineage).get("sources")):
        if not isinstance(item, Mapping):
            continue
        symbol = _symbol(str(item.get("symbol", "")))
        if symbol in by_symbol:
            by_symbol[symbol].add(_period(str(item.get("period", ""))))
    common = tuple(
        sorted(set.intersection(*(values for values in by_symbol.values())))
    )
    if not common:
        raise ReconstructionPlanCompatibilityError(
            "feed epoch lineage has no common triangle periods"
        )
    if common != _period_range(common[0], common[-1]):
        raise ReconstructionPlanCompatibilityError(
            "feed epoch lineage common triangle periods are discontinuous"
        )
    return common


def _inspect_tick_cache(path: Path) -> tuple[int, int, int]:
    try:
        import pyarrow as pa  # pylint: disable=import-outside-toplevel
        from pyarrow import ipc  # pylint: disable=import-outside-toplevel
    except ImportError as err:
        raise RuntimeError("reconstruction planning requires pyarrow") from err
    if not path.is_file():
        raise ReconstructionPlanCompatibilityError(
            f"source partition is missing: {path}"
        )
    try:
        with pa.memory_map(str(path), "r") as source:
            reader = ipc.open_file(source)
            names = set(reader.schema.names)
            if not {"datetime", "bid", "ask"}.issubset(names):
                raise ReconstructionPlanCompatibilityError(
                    f"source partition lacks tick bid/ask schema: {path}"
                )
            if names.intersection({"open", "high", "low", "close"}):
                raise ReconstructionPlanCompatibilityError(
                    f"source partition contains forbidden OHLC fields: {path}"
                )
            if not reader.num_record_batches:
                raise ReconstructionPlanCompatibilityError(
                    f"source partition contains no record batches: {path}"
                )
            row_count = sum(
                reader.get_batch(index).num_rows
                for index in range(reader.num_record_batches)
            )
            first_batch = reader.get_batch(0)
            last_batch = reader.get_batch(reader.num_record_batches - 1)
            column_index = reader.schema.get_field_index("datetime")
            first_ms = int(first_batch.column(column_index)[0].as_py())
            last_ms = int(last_batch.column(column_index)[-1].as_py())
    except ReconstructionPlanCompatibilityError:
        raise
    except Exception as err:
        raise ReconstructionPlanCompatibilityError(
            f"source partition cannot be read as Arrow IPC: {path}"
        ) from err
    if row_count <= 0:
        raise ReconstructionPlanCompatibilityError(
            f"source partition is empty: {path}"
        )
    return row_count, first_ms, last_ms


def _write_contract_artifact(
    contract: Any,
    root: Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    serializer = getattr(contract, "to_json", None)
    if not callable(serializer):
        raise TypeError("content-addressed contract must provide to_json()")
    encoded = str(serializer()).encode("utf-8") + b"\n"
    digest = hashlib.sha256(encoded).hexdigest()
    root.mkdir(parents=True, exist_ok=True)
    path = root / f"{prefix}-{digest}.json"
    if path.exists():
        if path.read_bytes() != encoded:
            raise ReconstructionPlanCompatibilityError(
                f"content-addressed artifact collision: {path}"
            )
    else:
        temporary = root / f".{path.name}.{os.getpid()}.tmp"
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    return ArtifactRef(
        kind=kind,
        path=str(path.resolve()),
        size_bytes=len(encoded),
        sha256=digest,
        metadata=dict(metadata),
    )


def _read_content_addressed_json(
    path: str | Path, prefix: str
) -> Mapping[str, Any]:
    source = Path(path).expanduser().resolve()
    match = re.fullmatch(
        rf"{re.escape(prefix)}-([0-9a-f]{{64}})\.json", source.name
    )
    if match is None:
        raise ValueError(f"{prefix} artifact name is not content addressed")
    content = source.read_bytes()
    if len(content) > MAX_SYNTHETIC_INFILL_PLAN_BYTES:
        raise ValueError(f"{prefix} artifact exceeds plan byte limit")
    if hashlib.sha256(content).hexdigest() != match.group(1):
        raise ValueError(f"{prefix} artifact hash differs from name")
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError(f"{prefix} artifact is invalid JSON") from err
    return _mapping(payload)


def _relative_tick_path(symbol: str, period: str) -> Path:
    return Path(symbol) / str(int(period[:4])) / str(int(period[4:])) / ".data"


def _period_range(start: str, end: str) -> tuple[str, ...]:
    values: list[str] = []
    current = _period(start)
    selected_end = _period(end)
    while current <= selected_end:
        values.append(current)
        current = _next_period(current)
    return tuple(values)


def _next_period(value: str) -> str:
    period = _period(value)
    year, month = int(period[:4]), int(period[4:])
    return f"{year + 1:04d}01" if month == 12 else f"{year:04d}{month + 1:02d}"


def _month_start_ns(value: str) -> int:
    period = _period(value)
    return int(
        datetime(
            int(period[:4]), int(period[4:]), 1, tzinfo=timezone.utc
        ).timestamp()
        * 1_000_000_000
    )


def _period_for_ns(value: int) -> str:
    timestamp = _int64(value, "requested timestamp")
    selected = datetime.fromtimestamp(timestamp // 1_000_000_000, timezone.utc)
    return f"{selected.year:04d}{selected.month:02d}"


def _period(value: str) -> str:
    normalized = str(value).strip()
    if _PERIOD_RE.fullmatch(normalized) is None:
        raise ValueError("period must use YYYYMM")
    month = int(normalized[4:])
    if not 1 <= month <= 12:
        raise ValueError("period month is outside 01-12")
    return normalized


def _symbols(values: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({_symbol(value) for value in values}))
    if not normalized:
        raise ValueError("at least one symbol is required")
    return normalized


def _symbol(value: Any) -> str:
    normalized = "".join(
        character for character in str(value).lower() if character.isalnum()
    )
    if not re.fullmatch(r"[a-z]{6}", normalized):
        raise ValueError("FX symbol must contain six letters")
    return normalized


def _artifact_mapping(
    values: Mapping[str, ArtifactRef],
) -> dict[str, ArtifactRef]:
    if not values or len(values) > MAX_RECONSTRUCTION_PLAN_ARTIFACTS:
        raise ValueError("plan artifact graph count is outside limits")
    result = {
        _required_text(name): _strong_ref(ref)
        for name, ref in sorted(values.items())
    }
    if len(
        {(ref.kind, ref.path, ref.sha256) for ref in result.values()}
    ) != len(result):
        raise ValueError("plan artifact graph contains duplicate references")
    return result


def _strong_ref(value: ArtifactRef) -> ArtifactRef:
    if not isinstance(value, ArtifactRef):
        raise TypeError("artifact reference must be ArtifactRef")
    if value.size_bytes is None:
        raise ValueError("artifact reference requires size_bytes")
    size = _nonnegative_int(value.size_bytes, "artifact.size_bytes")
    sha256 = str(value.sha256).strip().lower()
    if _SHA256_RE.fullmatch(sha256) is None:
        raise ValueError("artifact reference requires a sha256 digest")
    metadata = _json_value_mapping(value.metadata)
    return ArtifactRef(
        kind=_required_text(value.kind),
        path=str(Path(_required_text(value.path)).expanduser().resolve()),
        size_bytes=size,
        sha256=sha256,
        metadata=metadata,
    )


def _contract_sha256(contract: Any) -> str:
    serializer = getattr(contract, "to_json", None)
    if callable(serializer):
        encoded = str(serializer()).encode("utf-8")
    else:
        mapper = getattr(contract, "to_dict", None)
        if not callable(mapper):
            raise TypeError("contract must provide to_json() or to_dict()")
        encoded = canonical_contract_json(mapper()).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    target = path.resolve()
    try:
        stat = target.stat()
    except OSError as err:
        raise ReconstructionPlanCompatibilityError(
            f"source artifact cannot be hashed: {target}"
        ) from err
    return _file_sha256_for_identity(
        str(target),
        stat.st_dev,
        stat.st_ino,
        stat.st_size,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
    )


@lru_cache(maxsize=8192)
def _file_sha256_for_identity(
    path: str,
    device: int,
    inode: int,
    size: int,
    modified_ns: int,
    changed_ns: int,
) -> str:
    del device, inode, size, modified_ns, changed_ns
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as err:
        raise ReconstructionPlanCompatibilityError(
            f"source artifact cannot be hashed: {path}"
        ) from err
    return digest.hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _resolved_directory(value: str | Path) -> str:
    return str(Path(_required_text(value)).expanduser().resolve())


def _validated_plan_roots(
    *,
    source_root: Path,
    artifact_root: str | Path,
    output_root: str | Path,
    checkpoint_root: str | Path,
    scratch_root: str | Path,
) -> dict[str, Path]:
    roots = {
        "artifact": Path(_resolved_directory(artifact_root)),
        "output": Path(_resolved_directory(output_root)),
        "checkpoint": Path(_resolved_directory(checkpoint_root)),
        "scratch": Path(_resolved_directory(scratch_root)),
    }
    for name, root in roots.items():
        if _paths_overlap(source_root, root):
            raise ReconstructionPlanCompatibilityError(
                f"plan {name} root overlaps the immutable source tree"
            )
    root_items = tuple(roots.items())
    for index, (left_name, left) in enumerate(root_items):
        for right_name, right in root_items[index + 1 :]:
            if _paths_overlap(left, right):
                raise ReconstructionPlanCompatibilityError(
                    f"plan {left_name} and {right_name} roots overlap"
                )
    return roots


def _paths_overlap(left: Path, right: Path) -> bool:
    return (
        left == right
        or left.is_relative_to(right)
        or right.is_relative_to(left)
    )


def _required_text(value: Any) -> str:
    normalized = str(value).strip() if value is not None else ""
    if not normalized:
        raise ValueError("required text value is empty")
    return normalized


def _bounded_text(value: Any, maximum: int) -> str:
    normalized = _required_text(value)
    if len(normalized) > maximum:
        raise ValueError("text value exceeds bounded length")
    return normalized


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _positive_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ValueError(f"{name} must be non-negative")
    return result


def _int64(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if not -(2**63) <= result <= 2**63 - 1:
        raise ValueError(f"{name} is outside signed int64")
    return result


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("expected a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        return _mapping(json.loads(text))
    except json.JSONDecodeError as err:
        raise ValueError("artifact contains invalid JSON") from err


def _json_value_mapping(value: Mapping[str, Any]) -> dict[str, JSONValue]:
    return {str(key): _json_value(item, depth=0) for key, item in value.items()}


def _json_value(value: Any, *, depth: int) -> JSONValue:
    if depth > 8:
        raise ValueError("artifact metadata nesting exceeds limit")
    if value is None or isinstance(value, (str, int, float, bool)):
        return cast(JSONValue, value)
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(item, depth=depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [_json_value(item, depth=depth + 1) for item in value]
    raise ValueError("artifact metadata must be JSON-compatible")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError(f"unsupported schema; expected {expected}")


def _require_derived(data: Mapping[str, Any], key: str, expected: Any) -> None:
    if data.get(key) != expected:
        raise ValueError(f"derived field {key} differs")


__all__ = [
    "ASCII_TICK_SOURCE_KIND",
    "DEFAULT_RECONSTRUCTION_BASE_SEED",
    "DEFAULT_RECONSTRUCTION_MAX_PARALLEL_WINDOWS",
    "DEFAULT_RECONSTRUCTION_REQUEST_WINDOW_LIMIT",
    "DEFAULT_RECONSTRUCTION_WINDOW_SIZE_NS",
    "FIRST_PARTY_RECONSTRUCTION_HANDLERS",
    "IMMUTABLE_ANCHOR_POLICY",
    "PLAN_CONFIGURATION_ARTIFACT_KIND",
    "PLAN_EXECUTION_MANIFEST_ARTIFACT_KIND",
    "RECONSTRUCTION_PLAN_CONFIGURATION_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_EXECUTION_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_REFUSAL_SCHEMA_VERSION",
    "RECONSTRUCTION_PLAN_RESOURCE_SUMMARY_SCHEMA_VERSION",
    "RECONSTRUCTION_SOURCE_INVENTORY_SCHEMA_VERSION",
    "RECONSTRUCTION_SOURCE_PARTITION_SCHEMA_VERSION",
    "SCIENTIFIC_NONCLAIM",
    "SOURCE_INVENTORY_ARTIFACT_KIND",
    "SYNTHETIC_INFILL_PLAN_ARTIFACT_KIND",
    "SYNTHETIC_INFILL_PLAN_SCHEMA_VERSION",
    "TICK_ONLY_INPUT_POLICY",
    "ReconstructionDeliveryMode",
    "ReconstructionPlanCompatibilityError",
    "ReconstructionPlanConfigurationV1",
    "ReconstructionPlanExecutionManifestV1",
    "ReconstructionPlanRefusalCode",
    "ReconstructionPlanRefusalV1",
    "ReconstructionPlanResourceSummaryV1",
    "ReconstructionSourceInventoryV1",
    "ReconstructionSourcePartitionV1",
    "ReconstructionStagePlanV1",
    "SyntheticInfillPlanV1",
    "build_synthetic_infill_plan",
    "load_reconstruction_stage_plan",
    "read_reconstruction_plan_configuration",
    "read_reconstruction_plan_execution_manifest",
    "read_reconstruction_source_inventory",
    "read_synthetic_infill_plan",
    "validate_synthetic_infill_plan_for_execution",
    "write_synthetic_infill_plan",
]
