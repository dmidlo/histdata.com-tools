"""Content-addressed campaign receipt trees (additive SemVer 1.0.0).

These records describe an execution, not authority to skip current native
verification. In particular, a self-consistent checkpoint is not an independent
resume anchor. Structural readers cannot establish that mutable inputs remain
unchanged. Runtime measurements belong to this execution's identity; the native
product verification identity remains separately available for semantic replay.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
import re
from typing import ClassVar, TypeVar, cast

from histdatacom.broker_plugin_provenance._wire import (
    Artifact as _Artifact,
    Record,
    canonical_json,
    plain,
    sha256,
)
from histdatacom.campaign_index_contracts import (
    CampaignArtifactRefV1,
    CampaignProductVerificationV1,
    canonical,
    load_json,
)

PRODUCTS_PER_SHARD = 64
MAX_VERIFICATION_SHARDS = 4096
MAX_VERIFICATION_PRODUCTS = PRODUCTS_PER_SHARD * MAX_VERIFICATION_SHARDS
MAX_PRODUCT_INPUT_FILES = 4096
MAX_GLOBAL_CONTROL_RECEIPTS = 64
MAX_CONTROL_RECEIPTS = MAX_GLOBAL_CONTROL_RECEIPTS + MAX_VERIFICATION_SHARDS
RECEIPT_AUTHORITY = "historical_execution_only_current_integrity_required"
READ_SCOPE = "guarded_campaign_input_hashing_including_rereads_excludes_native_code_and_store_io"
ELAPSED_SCOPE = (
    "current_finalization_attempt_not_reused_historical_leaf_timings"
)
_T = TypeVar("_T", bound="CampaignReceipt")


def _digest(value: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("campaign receipt requires SHA-256")


def _text(value: str, limit: int = 4096) -> None:
    if (
        not value
        or len(value) > limit
        or value != value.strip()
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError("campaign receipt requires bounded text")


def _integer(value: int, maximum: int = 2**63 - 1) -> None:
    if not 0 <= value <= maximum:
        raise ValueError("campaign receipt count outside bound")


def _identifier(value: str, kind: str | None = None) -> None:
    if (
        re.fullmatch(r"campaign-receipt-[a-z-]+:sha256:[0-9a-f]{64}", value)
        is None
    ):
        raise ValueError("invalid campaign receipt identity")
    if kind is not None and not value.startswith(
        f"campaign-receipt-{kind}:sha256:"
    ):
        raise ValueError("foreign campaign receipt kind")


def _object(value: str) -> dict[str, object]:
    parsed = load_json(value)
    if type(parsed) is not dict or canonical(parsed) != value:
        raise ValueError("canonical campaign receipt object required")
    return cast(dict[str, object], parsed)


def _path(value: str) -> None:
    _text(value)
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts or str(path) != value:
        raise ValueError("canonical absolute campaign evidence path required")


class CampaignReceipt(_Artifact):
    """Exact detached records with a campaign-specific content domain."""

    __slots__ = ()

    @classmethod
    def schema_version(cls) -> str:
        return f"histdatacom.campaign-receipt-{cls.KIND}.v1"

    def to_dict(self) -> dict[str, object]:
        schema = self.schema_version()
        payload = plain(self)
        digest = sha256(
            canonical_json({"schema_version": schema, "payload": payload})
        )
        result = cast(
            dict[str, object],
            plain(
                {
                    "schema_version": schema,
                    "artifact_id": f"campaign-receipt-{self.KIND}:sha256:{digest}",
                    "payload": payload,
                }
            ),
        )
        canonical(result)
        return result

    @classmethod
    def from_json(cls: type[_T], value: str) -> _T:  # noqa: PYI019
        parsed = load_json(value)
        if type(parsed) is not dict or canonical(parsed) != value:
            raise ValueError("noncanonical campaign receipt JSON")
        return cls.from_dict(parsed)


@dataclass(frozen=True, slots=True)
class CampaignReceiptRefV1(Record):
    """Store-local closed reference; never accepts a caller-selected path."""

    kind: str
    artifact_id: str
    size_bytes: int
    sha256: str

    def _validate(self) -> None:
        if self.kind not in RECEIPT_KINDS:
            raise ValueError("unknown campaign receipt kind")
        _identifier(self.artifact_id, self.kind)
        _integer(self.size_bytes, 4 * 1024 * 1024)
        if self.size_bytes == 0:
            raise ValueError("empty campaign receipt")
        _digest(self.sha256)


@dataclass(frozen=True, slots=True)
class CampaignVerifiedFileV1(Record):
    path: str
    size_bytes: int
    sha256: str
    role: str

    def _validate(self) -> None:
        _path(self.path)
        _integer(self.size_bytes)
        _digest(self.sha256)
        if self.role not in (
            "control",
            "source",
            "product",
            "parquet",
            "input",
        ):
            raise ValueError("unknown campaign input role")


def _files(values: tuple[CampaignVerifiedFileV1, ...]) -> None:
    if not values or len(values) > MAX_PRODUCT_INPUT_FILES:
        raise ValueError("campaign input inventory bound")
    paths = tuple(item.path for item in values)
    if paths != tuple(sorted(set(paths))):
        raise ValueError("campaign input paths must be sorted unique")


def _refs(
    values: tuple[CampaignReceiptRefV1, ...], kind: str, limit: int
) -> None:
    if len(values) > limit or any(item.kind != kind for item in values):
        raise ValueError("campaign child kind/cardinality differs")
    if len({item.artifact_id for item in values}) != len(values):
        raise ValueError("duplicate campaign child receipt")


@dataclass(frozen=True, slots=True)
class CampaignVerificationRunV1(CampaignReceipt):
    index_path: str
    index_id: str
    index_sha256: str
    implementation_sha256: str
    environment_json: str
    forbidden_roots: tuple[str, ...]
    products_per_shard: int = PRODUCTS_PER_SHARD
    authority: str = RECEIPT_AUTHORITY
    read_scope: str = READ_SCOPE
    KIND: ClassVar[str] = "run"

    def _validate(self) -> None:
        _path(self.index_path)
        _text(self.index_id, 256)
        _digest(self.index_sha256)
        _digest(self.implementation_sha256)
        if not _object(self.environment_json):
            raise ValueError("execution environment cannot be empty")
        for value in self.forbidden_roots:
            _path(value)
        if (
            not self.forbidden_roots
            or self.forbidden_roots != tuple(sorted(set(self.forbidden_roots)))
            or not 1 <= self.products_per_shard <= PRODUCTS_PER_SHARD
            or self.authority != RECEIPT_AUTHORITY
            or self.read_scope != READ_SCOPE
        ):
            raise ValueError("campaign verification run policy differs")

    @property
    def environment_id(self) -> str:
        return "campaign-environment:sha256:" + sha256(self.environment_json)


@dataclass(frozen=True, slots=True)
class CampaignControlReceiptV1(CampaignReceipt):
    """Measured global-control chunk or one complete native plan closure."""

    run_id: str
    ordinal: int
    scope: str
    plan_id: str | None
    shard_id: str | None
    input_files: tuple[CampaignVerifiedFileV1, ...]
    inputs_sha256: str
    elapsed_ns: int
    read_bytes: int
    KIND: ClassVar[str] = "control"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        _integer(self.ordinal, MAX_CONTROL_RECEIPTS - 1)
        if self.scope == "global":
            if self.plan_id is not None or self.shard_id is not None:
                raise ValueError("global control cannot claim a plan")
        elif self.scope == "plan":
            if self.plan_id is None or self.shard_id is None:
                raise ValueError("plan control requires exact coordinates")
            _text(self.plan_id, 256)
            _text(self.shard_id, 256)
        else:
            raise ValueError("unknown campaign control scope")
        _files(self.input_files)
        _digest(self.inputs_sha256)
        _integer(self.elapsed_ns)
        _integer(self.read_bytes)
        if self.read_bytes < sum(item.size_bytes for item in self.input_files):
            raise ValueError("measured control reads omit input bytes")


@dataclass(frozen=True, slots=True)
class CampaignProductReceiptV1(CampaignReceipt):
    run_id: str
    ordinal: int
    core_json: str
    input_files: tuple[CampaignVerifiedFileV1, ...]
    parquet_paths: tuple[str, ...]
    lineage_json: str
    elapsed_ns: int
    read_bytes: int
    KIND: ClassVar[str] = "product"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        _integer(self.ordinal, MAX_VERIFICATION_PRODUCTS - 1)
        core = CampaignProductVerificationV1.from_json(self.core_json)
        _files(self.input_files)
        by_path = {item.path: item for item in self.input_files}
        manifest = by_path.get(core.product_ref.path)
        if manifest is None or (
            manifest.size_bytes != core.product_ref.size_bytes
            or manifest.sha256 != core.product_ref.sha256
        ):
            raise ValueError("product manifest absent from verified inventory")
        if (
            not self.parquet_paths
            or self.parquet_paths != tuple(sorted(set(self.parquet_paths)))
            or not set(self.parquet_paths) <= set(by_path)
        ):
            raise ValueError("product Parquet inventory differs")
        if not _object(self.lineage_json):
            raise ValueError("product lineage cannot be empty")
        _integer(self.elapsed_ns)
        _integer(self.read_bytes)
        if self.read_bytes < sum(item.size_bytes for item in self.input_files):
            raise ValueError("measured reads omit verified input bytes")

    @property
    def verification(self) -> CampaignProductVerificationV1:
        return CampaignProductVerificationV1.from_json(self.core_json)


@dataclass(frozen=True, slots=True)
class CampaignVerificationShardV1(CampaignReceipt):
    run_id: str
    ordinal: int
    first_product_ordinal: int
    products: tuple[CampaignReceiptRefV1, ...]
    observed_event_count: int
    synthetic_event_count: int
    elapsed_ns: int
    read_bytes: int
    products_per_shard: int = PRODUCTS_PER_SHARD
    KIND: ClassVar[str] = "shard"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        _integer(self.ordinal, MAX_VERIFICATION_SHARDS - 1)
        if not 1 <= self.products_per_shard <= PRODUCTS_PER_SHARD:
            raise ValueError("campaign shard grouping bound")
        if self.first_product_ordinal != self.ordinal * self.products_per_shard:
            raise ValueError("campaign shard ordinal geometry differs")
        _refs(self.products, "product", self.products_per_shard)
        if not self.products:
            raise ValueError("empty verification shard")
        for value in (
            self.observed_event_count,
            self.synthetic_event_count,
            self.elapsed_ns,
            self.read_bytes,
        ):
            _integer(value)


@dataclass(frozen=True, slots=True)
class CampaignVerificationCheckpointV1(CampaignReceipt):
    run_id: str
    journal_head: CampaignReceiptRefV1 | None
    shards: tuple[CampaignReceiptRefV1, ...]
    pending_products: tuple[CampaignReceiptRefV1, ...]
    next_product_ordinal: int
    inventory_sha256: str
    products_per_shard: int = PRODUCTS_PER_SHARD
    KIND: ClassVar[str] = "checkpoint"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        if not 1 <= self.products_per_shard <= PRODUCTS_PER_SHARD:
            raise ValueError("checkpoint shard grouping bound")
        if (
            self.journal_head is not None
            and self.journal_head.kind != "journal"
        ):
            raise ValueError("checkpoint journal head kind differs")
        _refs(self.shards, "shard", MAX_VERIFICATION_SHARDS)
        _refs(self.pending_products, "product", self.products_per_shard - 1)
        _integer(self.next_product_ordinal, MAX_VERIFICATION_PRODUCTS)
        if self.next_product_ordinal != (
            len(self.shards) * self.products_per_shard
            + len(self.pending_products)
        ):
            raise ValueError("checkpoint product prefix geometry differs")
        _digest(self.inventory_sha256)


@dataclass(frozen=True, slots=True)
class CampaignVerificationSummaryV1(CampaignReceipt):
    """Bounded traversal summary; not the legacy global-file-set digest."""

    index_ref_json: str
    index_id: str
    plan_set_id: str
    support_artifact_id: str
    status: str
    shard_count: int
    support_window_count: int
    product_count: int
    missing_product_count: int
    empty_window_count: int
    refused_window_count: int
    observed_event_count: int
    synthetic_event_count: int
    shard_rows_sha256: str
    control_inputs_sha256: str
    product_verifications_sha256: str
    product_inputs_sha256: str
    out_of_plan_json: str = "[]"
    KIND: ClassVar[str] = "summary"

    def _validate(self) -> None:
        ref = CampaignArtifactRefV1.from_dict(_object(self.index_ref_json))
        if ref.kind != "reconstruction_campaign_product_index_v1":
            raise ValueError("summary index reference kind differs")
        for value in (
            self.index_id,
            self.plan_set_id,
            self.support_artifact_id,
        ):
            _text(value, 256)
        _integer(self.shard_count, MAX_VERIFICATION_SHARDS)
        if not self.shard_count:
            raise ValueError("summary requires at least one native shard")
        for count in (
            self.support_window_count,
            self.product_count,
            self.missing_product_count,
            self.empty_window_count,
            self.refused_window_count,
            self.observed_event_count,
            self.synthetic_event_count,
        ):
            _integer(count)
        if (
            self.product_count + self.missing_product_count
            > MAX_VERIFICATION_PRODUCTS
        ):
            raise ValueError("summary product denominator exceeds bound")
        expected = "incomplete" if self.missing_product_count else "complete"
        if self.status != expected:
            raise ValueError("summary completion status differs")
        for value in (
            self.shard_rows_sha256,
            self.control_inputs_sha256,
            self.product_verifications_sha256,
            self.product_inputs_sha256,
        ):
            _digest(value)
        outside = load_json(self.out_of_plan_json)
        if (
            type(outside) is not list
            or len(outside) > MAX_VERIFICATION_SHARDS
            or canonical(outside) != self.out_of_plan_json
        ):
            raise ValueError("bounded canonical out-of-plan inventory required")
        refs = tuple(CampaignArtifactRefV1.from_dict(item) for item in outside)
        keys = tuple((item.path, item.sha256) for item in refs)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("out-of-plan summary paths must be sorted unique")


@dataclass(frozen=True, slots=True)
class CampaignVerificationRootV1(CampaignReceipt):
    run_id: str
    shards: tuple[CampaignReceiptRefV1, ...]
    summary_json: str
    initial_inventory_sha256: str
    final_inventory_sha256: str
    product_count: int
    elapsed_ns: int
    read_bytes: int
    products_per_shard: int = PRODUCTS_PER_SHARD
    global_controls: tuple[CampaignReceiptRefV1, ...] = ()
    plan_controls: tuple[CampaignReceiptRefV1, ...] = ()
    reused_checkpoint: CampaignReceiptRefV1 | None = None
    elapsed_scope: str = ELAPSED_SCOPE
    KIND: ClassVar[str] = "root"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        if not 1 <= self.products_per_shard <= PRODUCTS_PER_SHARD:
            raise ValueError("root shard grouping bound")
        _refs(self.shards, "shard", MAX_VERIFICATION_SHARDS)
        deep = CampaignVerificationSummaryV1.from_json(self.summary_json)
        _refs(self.global_controls, "control", MAX_GLOBAL_CONTROL_RECEIPTS)
        _refs(self.plan_controls, "control", MAX_VERIFICATION_SHARDS)
        if self.elapsed_scope != ELAPSED_SCOPE or (
            self.reused_checkpoint is not None
            and self.reused_checkpoint.kind != "checkpoint"
        ):
            raise ValueError("root recovery/elapsed measurement scope differs")
        if (
            not self.global_controls
            or len(self.plan_controls) != deep.shard_count
            or set(self.global_controls) & set(self.plan_controls)
        ):
            raise ValueError("root control coverage differs")
        _digest(self.initial_inventory_sha256)
        _digest(self.final_inventory_sha256)
        _integer(self.product_count, MAX_VERIFICATION_PRODUCTS)
        _integer(self.elapsed_ns)
        _integer(self.read_bytes)
        if (
            deep.status != "complete"
            or deep.missing_product_count
            or deep.out_of_plan_json != "[]"
            or self.initial_inventory_sha256 != self.final_inventory_sha256
            or self.product_count != deep.product_count
            or len(self.shards)
            != (self.product_count + self.products_per_shard - 1)
            // self.products_per_shard
        ):
            raise ValueError(
                "successful campaign root requires exact complete inventory"
            )


@dataclass(frozen=True, slots=True)
class CampaignVerificationJournalV1(CampaignReceipt):
    run_id: str
    ordinal: int
    previous: CampaignReceiptRefV1 | None
    event: str
    subject: CampaignReceiptRefV1
    product_ordinal: int | None
    KIND: ClassVar[str] = "journal"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        _integer(self.ordinal, 4 * MAX_VERIFICATION_PRODUCTS + 16)
        if (self.ordinal == 0) != (self.previous is None):
            raise ValueError("campaign journal predecessor differs")
        if self.previous is not None and self.previous.kind != "journal":
            raise ValueError("campaign journal predecessor kind differs")
        if self.event not in (
            "started",
            "resumed",
            "product",
            "shard",
            "checkpoint",
            "failed",
            "completed",
        ):
            raise ValueError("unknown campaign journal event")
        expected = {
            "started": "run",
            "resumed": "checkpoint",
            "failed": "failure",
            "completed": "root",
        }.get(self.event, self.event)
        if self.subject.kind != expected:
            raise ValueError("campaign journal subject kind differs")
        if self.event == "resumed" and self.ordinal == 0:
            raise ValueError(
                "resumed journal requires a prior committed checkpoint"
            )
        if (self.event == "product") != (self.product_ordinal is not None):
            raise ValueError("campaign journal product coordinate differs")
        if self.product_ordinal is not None:
            _integer(self.product_ordinal, MAX_VERIFICATION_PRODUCTS - 1)


@dataclass(frozen=True, slots=True)
class CampaignVerificationFailureV1(CampaignReceipt):
    run_id: str
    product_ordinal: int | None
    checkpoint: CampaignReceiptRefV1 | None
    reason_code: str
    message: str
    elapsed_ns: int
    read_bytes: int
    KIND: ClassVar[str] = "failure"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        if self.product_ordinal is not None:
            _integer(self.product_ordinal, MAX_VERIFICATION_PRODUCTS - 1)
        if self.checkpoint is not None and self.checkpoint.kind != "checkpoint":
            raise ValueError("failure checkpoint kind differs")
        _text(self.reason_code, 128)
        _text(self.message, 1024)
        _integer(self.elapsed_ns)
        _integer(self.read_bytes)


@dataclass(frozen=True, slots=True)
class CampaignVerificationSampleV1(CampaignReceipt):
    run_id: str
    root_id: str
    product_ordinals: tuple[int, ...]
    products: tuple[CampaignReceiptRefV1, ...]
    current_inventory_sha256: str
    verified_inputs_sha256: str
    elapsed_ns: int
    read_bytes: int
    authority: str = "sampled_products_only_not_full_campaign_verification"
    KIND: ClassVar[str] = "sample"

    def _validate(self) -> None:
        _identifier(self.run_id, "run")
        _identifier(self.root_id, "root")
        if (
            not self.product_ordinals
            or len(self.product_ordinals) > PRODUCTS_PER_SHARD
            or self.product_ordinals
            != tuple(sorted(set(self.product_ordinals)))
            or len(self.products) != len(self.product_ordinals)
            or self.authority
            != "sampled_products_only_not_full_campaign_verification"
        ):
            raise ValueError("campaign sample scope differs")
        for ordinal in self.product_ordinals:
            _integer(ordinal, MAX_VERIFICATION_PRODUCTS - 1)
        _refs(self.products, "product", PRODUCTS_PER_SHARD)
        _digest(self.current_inventory_sha256)
        _digest(self.verified_inputs_sha256)
        _integer(self.elapsed_ns)
        _integer(self.read_bytes)


RECEIPT_TYPES = (
    CampaignVerificationRunV1,
    CampaignControlReceiptV1,
    CampaignProductReceiptV1,
    CampaignVerificationShardV1,
    CampaignVerificationCheckpointV1,
    CampaignVerificationSummaryV1,
    CampaignVerificationRootV1,
    CampaignVerificationJournalV1,
    CampaignVerificationFailureV1,
    CampaignVerificationSampleV1,
)
RECEIPT_KINDS = tuple(cls.KIND for cls in RECEIPT_TYPES)
