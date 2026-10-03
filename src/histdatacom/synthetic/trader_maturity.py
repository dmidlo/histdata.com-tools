"""Non-authoritative trader claims alongside, never inside, capability V1.

References, retained-availability declarations and recorded PASSED states are
not verified scientific evidence. This module performs no IO, execution,
certification, promotion, or inference from one maturity stage to another.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
from dataclasses import dataclass, fields
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, TypeVar, cast

from histdatacom.synthetic.capability_matrix import (
    CapabilityEvidenceScopeV1,
    CapabilityMatrixV1,
    CapabilityStateV1,
)

MAX_WIRE_BYTES = 256 * 1024
MAX_TEXT_LENGTH = 1024
MAX_ITEMS = 16
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_ID = re.compile(r"[a-z][a-z0-9._-]{0,95}:sha256:[0-9a-f]{64}\Z")
_UTC = "%Y-%m-%dT%H:%M:%SZ"

# Order is presentation only, not a succession rule or authority hierarchy.
TRADER_MATURITY_CATALOG: tuple[tuple[str, str], ...] = (
    ("archive_integrity", "v33 archive integrity"),
    ("catalog_identity", "1,000 canonical strategy IDs and catalog identity"),
    ("dsl_compiler", "Typed DSL/operator compiler"),
    ("point_in_time_snapshot", "Shared point-in-time TraderFeatureSnapshotV1"),
    ("execution_account_ledger", "Jurisdiction, execution and account ledger"),
    (
        "production_catalog_compilation",
        "All 1,000 strategies compile in production",
    ),
    (
        "portfolio_exposure_equivalence",
        "Post-compiler portfolio exposure duplicate gate",
    ),
    ("strategy_response_features", "Dynamic strategy-response feature bank"),
    ("synthetic_flow_aggregation", "Reconciled synthetic-flow aggregates"),
    ("population_fingerprint", "SyntheticTraderPopulationFingerprintV1"),
    (
        "atomic_persistence_cli_replay",
        "Atomic trader persistence, CLI and replay",
    ),
    ("branch_integration", "Branch and input integration gate"),
    ("representative_campaign", "Bounded representative historical campaign"),
    (
        "complete_population_campaign",
        "Complete historical 1,000-strategy campaign",
    ),
    ("ml_incremental_value", "X-versus-X+Z and negative-control qualification"),
    (
        "wide_corpus_population_columns",
        "Verified strategy/population columns in final wide ML corpus",
    ),
)
TRADER_MATURITY_STAGE_IDS = tuple(item[0] for item in TRADER_MATURITY_CATALOG)


def _pass_scopes(stage_id: str) -> tuple[CapabilityEvidenceScopeV1, ...]:
    if stage_id == "representative_campaign":
        return (CapabilityEvidenceScopeV1.BOUNDED,)
    if stage_id in (
        "complete_population_campaign",
        "wide_corpus_population_columns",
    ):
        return (CapabilityEvidenceScopeV1.COMPLETE,)
    if stage_id == "ml_incremental_value":
        return (
            CapabilityEvidenceScopeV1.BOUNDED,
            CapabilityEvidenceScopeV1.COMPLETE,
        )
    return (CapabilityEvidenceScopeV1.SOFTWARE,)


TRADER_MATURITY_PASS_SCOPES: tuple[
    tuple[str, tuple[CapabilityEvidenceScopeV1, ...]], ...
] = tuple((name, _pass_scopes(name)) for name in TRADER_MATURITY_STAGE_IDS)
_CATALOG_REQUIRED_FOR_PASS = (
    "catalog_identity",
    "production_catalog_compilation",
    "representative_campaign",
    "complete_population_campaign",
    "ml_incremental_value",
    "wide_corpus_population_columns",
)
_CAMPAIGN_REQUIRED_FOR_PASS = (
    "representative_campaign",
    "complete_population_campaign",
    "ml_incremental_value",
)
_PRODUCT_DATASET_REQUIRED_FOR_PASS = ("wide_corpus_population_columns",)
TRADER_MATURITY_CATALOG_ID = (
    "trader-maturity-catalog:sha256:"
    + hashlib.sha256(
        json.dumps(
            {
                "schema_version": "histdatacom.trader-maturity-catalog.v1",
                "stages": TRADER_MATURITY_CATALOG,
                "authority": "none",
                "stage_inference": "none",
                "waivers": "not_admitted",
                "pass_scopes": TRADER_MATURITY_PASS_SCOPES,
                "retained_catalog_required_for_pass": _CATALOG_REQUIRED_FOR_PASS,
                "campaign_id_required_for_pass": _CAMPAIGN_REQUIRED_FOR_PASS,
                "product_and_dataset_required_for_pass": _PRODUCT_DATASET_REQUIRED_FOR_PASS,
                "pass_evidence": "own-implementation-retained-execution-distinct-verification-time-no-blockers-v1",
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
    ).hexdigest()
)


class TraderReferenceAvailabilityV1(str, Enum):
    """A declared availability state, not a file check or proof result."""

    DECLARED_UNAVAILABLE = "declared_unavailable"
    RETAINED_REFERENCE = "retained_reference"


def _text(value: Any, name: str) -> None:
    if (
        type(value) is not str
        or not 1 <= len(value) <= MAX_TEXT_LENGTH
        or value.strip() != value
        or any(ord(char) < 32 for char in value)
    ):
        raise ValueError(f"{name} requires bounded nonempty text")


def _identifier(value: Any, name: str, *, optional: bool = False) -> None:
    if optional and value is None:
        return
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValueError(f"{name} requires an exact content-addressed ID")


def _timestamp(value: Any, name: str) -> None:
    if type(value) is not str or len(value) != 20:
        raise ValueError(f"{name} requires canonical UTC seconds")
    try:
        parsed = datetime.strptime(value, _UTC)
    except ValueError as exc:
        raise ValueError(f"{name} requires canonical UTC seconds") from exc
    if parsed.strftime(_UTC) != value:
        raise ValueError(f"{name} requires canonical UTC seconds")


def _strings(value: Any, name: str) -> None:
    if type(value) is not tuple or len(value) > MAX_ITEMS:
        raise ValueError(f"{name} requires a bounded exact tuple")
    for item in value:
        _text(item, name)
    if len(set(value)) != len(value):
        raise ValueError(f"{name} contains duplicates")


def _tree_bound(value: Any) -> None:
    pending = [(value, 0)]
    nodes = 0
    budget = 0
    while pending:
        item, depth = pending.pop()
        nodes += 1
        if nodes > 4096 or depth > 12:
            raise ValueError("trader maturity tree exceeds bounds")
        budget += 4
        if type(item) is dict:
            if len(item) > 24 or any(type(key) is not str for key in item):
                raise ValueError("trader maturity mapping exceeds bounds")
            pending.extend((child, depth + 1) for child in item.values())
            pending.extend((key, depth + 1) for key in item)
        elif type(item) in (list, tuple):
            if len(item) > MAX_ITEMS:
                raise ValueError("trader maturity sequence exceeds bounds")
            pending.extend((child, depth + 1) for child in item)
        elif type(item) is str:
            if len(item) > MAX_TEXT_LENGTH:
                raise ValueError("trader maturity string exceeds bounds")
            budget += sum(
                (
                    12
                    if ord(char) > 0xFFFF
                    else (
                        6
                        if ord(char) >= 127
                        else (
                            2
                            if char in '\\"\n\r\t\b\f' or ord(char) < 32
                            else 1
                        )
                    )
                )
                for char in item
            )
        elif item is None or type(item) is bool:
            budget += 5
        elif type(item) is int and abs(item) <= 2**31 - 1:
            budget += 12
        else:
            raise ValueError("trader maturity wire contains unsupported value")
        if budget > MAX_WIRE_BYTES:
            raise ValueError("trader maturity escaped wire exceeds bound")


def _canonical(value: Any) -> str:
    _tree_bound(value)
    text = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )
    if len(text) > MAX_WIRE_BYTES:
        raise ValueError("trader maturity wire exceeds bound")
    return text


def _pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate trader maturity JSON key")
        result[key] = value
    return result


def _load(text: str) -> dict[str, Any]:
    if (
        type(text) is not str
        or len(text) > MAX_WIRE_BYTES
        or not text.isascii()
    ):
        raise ValueError("trader maturity JSON requires bounded ASCII text")
    try:
        value = json.loads(text, object_pairs_hook=_pairs)
    except (RecursionError, json.JSONDecodeError) as exc:
        raise ValueError("invalid trader maturity JSON") from exc
    _tree_bound(value)
    if type(value) is not dict:
        raise ValueError("trader maturity JSON requires an object")
    return value


T = TypeVar("T", bound="_Record")


def _readmit(value: T, cls: type[T]) -> T:
    if cls not in _RECORD_TYPES or type(value) is not cls:
        raise TypeError("trader maturity requires an exact record type")
    return cls(
        **{
            field.name: getattr(value, field.name)
            for field in fields(cast(Any, cls))
        }
    )


def _payload(value: _Record, *, identity: bool = False) -> dict[str, Any]:
    def wire(item: Any) -> Any:
        if type(item) in _RECORD_TYPES:
            return _payload(item)
        if type(item) is tuple:
            return [wire(child) for child in item]
        if type(item) in (
            CapabilityStateV1,
            CapabilityEvidenceScopeV1,
            TraderReferenceAvailabilityV1,
        ):
            return item.value
        return item

    result = {
        field.name: wire(getattr(value, field.name))
        for field in fields(cast(Any, value))
        if not identity or field.name != value._ID_FIELD
    }
    result["schema_version"] = value.schema_version
    return result


def _derive(value: _Record, prefix: str) -> None:
    expected = (
        prefix
        + ":sha256:"
        + hashlib.sha256(
            _canonical(_payload(value, identity=True)).encode("ascii")
        ).hexdigest()
    )
    supplied = getattr(value, value._ID_FIELD)
    if type(supplied) is not str or supplied not in ("", expected):
        raise ValueError(
            "trader maturity identity differs from canonical content"
        )
    object.__setattr__(value, value._ID_FIELD, expected)


def _array(value: Any) -> tuple[Any, ...]:
    if type(value) is not list or len(value) > MAX_ITEMS:
        raise ValueError("trader maturity wire requires a bounded JSON array")
    return tuple(value)


class _Record:
    """Local strict immutable wire behavior; no scientific authority."""

    schema_version: ClassVar[str]
    _ID_FIELD: ClassVar[str]

    def to_dict(self) -> dict[str, Any]:
        """Return detached wire after exact nested structural readmission."""
        return _payload(_readmit(self, type(self)))

    def to_json(self) -> str:
        """Return canonical ASCII JSON without a record terminator."""
        return _canonical(self.to_dict())

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any]) -> T:
        """Reject unknown schemas/fields and rederive exact identities."""
        if cls not in _RECORD_TYPES or type(data) is not dict:
            raise TypeError("trader maturity requires an exact mapping")
        _tree_bound(data)
        if (
            set(data)
            != {field.name for field in fields(cast(Any, cls))}
            | {"schema_version"}
            or data["schema_version"] != cls.schema_version
        ):
            raise ValueError("trader maturity schema or fields differ")
        return cls(
            **cls._decode(
                {
                    key: value
                    for key, value in data.items()
                    if key != "schema_version"
                }
            )
        )

    @classmethod
    def from_json(cls: type[T], text: str) -> T:
        """Read normalized structure, not an independently verified claim."""
        result = cls.from_dict(_load(text))
        if result.to_json() != text:
            raise ValueError("trader maturity JSON is not canonical")
        return result

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        return values


@dataclass(frozen=True, slots=True)
class TraderCatalogIdentityV1(_Record):
    """Keep declared source-byte and canonical identities distinct."""

    source_json_sha256: str
    canonical_catalog_sha256: str
    availability: TraderReferenceAvailabilityV1
    source_artifact_id: str | None = None
    identity_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.trader-catalog-identity.v1"
    _ID_FIELD: ClassVar[str] = "identity_id"

    def __post_init__(self) -> None:
        for digest in (self.source_json_sha256, self.canonical_catalog_sha256):
            if type(digest) is not str or _SHA.fullmatch(digest) is None:
                raise ValueError(
                    "catalog identities require exact SHA256 values"
                )
        if type(self.availability) is not TraderReferenceAvailabilityV1:
            raise TypeError("catalog availability requires the closed enum")
        _identifier(
            self.source_artifact_id, "source_artifact_id", optional=True
        )
        if (
            self.availability
            is TraderReferenceAvailabilityV1.DECLARED_UNAVAILABLE
        ):
            if self.source_artifact_id is not None:
                raise ValueError(
                    "unavailable catalog cannot claim a retained source"
                )
        elif (
            self.source_artifact_id is None
            or self.source_artifact_id.rsplit(":", 1)[-1]
            != self.source_json_sha256
        ):
            raise ValueError(
                "retained catalog reference must bind exact source bytes"
            )
        _derive(self, "trader-catalog-identity")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["availability"] = TraderReferenceAvailabilityV1(
            values["availability"]
        )
        return values


@dataclass(frozen=True, slots=True)
class TraderMaturityEvidenceV1(_Record):
    """Recorded references only; availability is not independently checked."""

    implementation_commit: str | None
    execution_artifact_id: str | None
    independent_verification_artifact_id: str | None
    campaign_id: str | None
    product_id: str | None
    availability: TraderReferenceAvailabilityV1
    references: tuple[str, ...]
    evidence_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.trader-maturity-evidence.v1"
    _ID_FIELD: ClassVar[str] = "evidence_id"

    def __post_init__(self) -> None:
        if self.implementation_commit is not None and (
            type(self.implementation_commit) is not str
            or _COMMIT.fullmatch(self.implementation_commit) is None
        ):
            raise ValueError(
                "implementation_commit requires a full immutable ID"
            )
        for name in (
            "execution_artifact_id",
            "independent_verification_artifact_id",
            "campaign_id",
            "product_id",
        ):
            _identifier(getattr(self, name), name, optional=True)
        if type(self.availability) is not TraderReferenceAvailabilityV1:
            raise TypeError("evidence availability requires the closed enum")
        _strings(self.references, "references")
        if self.independent_verification_artifact_id is not None and (
            self.execution_artifact_id is None
            or self.independent_verification_artifact_id
            == self.execution_artifact_id
        ):
            raise ValueError(
                "independent verification requires distinct execution"
            )
        if (
            self.availability
            is TraderReferenceAvailabilityV1.RETAINED_REFERENCE
            and self.execution_artifact_id is None
        ):
            raise ValueError(
                "retained evidence requires an execution reference"
            )
        _derive(self, "trader-maturity-evidence")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["availability"] = TraderReferenceAvailabilityV1(
            values["availability"]
        )
        values["references"] = _array(values["references"])
        return values


@dataclass(frozen=True, slots=True)
class TraderMaturityRowV1(_Record):
    """One independent recorded claim; no waiver policy is admitted."""

    stage_id: str
    state: CapabilityStateV1
    evidence_scope: CapabilityEvidenceScopeV1
    scope: str
    limitations: tuple[str, ...]
    blocking_issues: tuple[int, ...]
    last_verified_at_utc: str | None
    evidence: TraderMaturityEvidenceV1
    row_id: str = ""
    schema_version: ClassVar[str] = "histdatacom.trader-maturity-row.v1"
    _ID_FIELD: ClassVar[str] = "row_id"

    def __post_init__(self) -> None:
        if (
            type(self.stage_id) is not str
            or self.stage_id not in TRADER_MATURITY_STAGE_IDS
        ):
            raise ValueError("unknown trader maturity stage")
        if (
            type(self.state) is not CapabilityStateV1
            or type(self.evidence_scope) is not CapabilityEvidenceScopeV1
        ):
            raise TypeError("trader state and scope require exact enums")
        if self.state is CapabilityStateV1.WAIVED_WITH_LIMITATION:
            raise ValueError("trader supplement admits no waiver policy")
        _text(self.scope, "scope")
        _strings(self.limitations, "limitations")
        if (
            type(self.blocking_issues) is not tuple
            or len(self.blocking_issues) > MAX_ITEMS
            or any(
                type(item) is not int or not 1 <= item <= 2**31 - 1
                for item in self.blocking_issues
            )
        ):
            raise ValueError(
                "blocking_issues requires bounded exact issue integers"
            )
        if tuple(sorted(set(self.blocking_issues))) != self.blocking_issues:
            raise ValueError("blocking issues must be sorted and unique")
        if self.last_verified_at_utc is not None:
            _timestamp(self.last_verified_at_utc, "last_verified_at_utc")
        evidence = _readmit(self.evidence, TraderMaturityEvidenceV1)
        object.__setattr__(self, "evidence", evidence)
        executed = self.state in (
            CapabilityStateV1.EXECUTED_FAILED,
            CapabilityStateV1.EXECUTED_INSUFFICIENT_EVIDENCE,
            CapabilityStateV1.EXECUTED_PASSED,
        )
        if (
            executed or self.state is CapabilityStateV1.IMPLEMENTED_UNEXECUTED
        ) and evidence.implementation_commit is None:
            raise ValueError(
                "implemented state requires a full implementation commit"
            )
        if (
            self.state is CapabilityStateV1.NOT_IMPLEMENTED
            and evidence.implementation_commit is not None
        ):
            raise ValueError("not_implemented cannot claim implementation")
        if executed and (
            evidence.execution_artifact_id is None
            or self.evidence_scope is CapabilityEvidenceScopeV1.NONE
            or evidence.availability
            is not TraderReferenceAvailabilityV1.RETAINED_REFERENCE
        ):
            raise ValueError(
                "executed state requires retained scoped execution"
            )
        if self.state in (
            CapabilityStateV1.NOT_IMPLEMENTED,
            CapabilityStateV1.IMPLEMENTED_UNEXECUTED,
        ) and (
            evidence.execution_artifact_id is not None
            or evidence.independent_verification_artifact_id is not None
        ):
            raise ValueError("unexecuted state cannot carry executed evidence")
        if (
            evidence.independent_verification_artifact_id is not None
            and self.last_verified_at_utc is None
        ):
            raise ValueError(
                "verification reference requires verification time"
            )
        if evidence.execution_artifact_id is not None and (
            evidence.implementation_commit is None
            or self.evidence_scope is CapabilityEvidenceScopeV1.NONE
        ):
            raise ValueError(
                "execution reference requires implementation and scope"
            )
        if (
            self.last_verified_at_utc is not None
            and evidence.independent_verification_artifact_id is None
        ):
            raise ValueError(
                "verification time requires a verification reference"
            )
        if self.state is CapabilityStateV1.EXECUTED_PASSED:
            if (
                self.evidence_scope
                not in dict(TRADER_MATURITY_PASS_SCOPES)[self.stage_id]
            ):
                raise ValueError(
                    "passed stage has an inadmissible evidence scope"
                )
            if (
                evidence.independent_verification_artifact_id is None
                or self.blocking_issues
            ):
                raise ValueError(
                    "passed claim requires own verification and no blockers"
                )
            if (
                self.stage_id in _CAMPAIGN_REQUIRED_FOR_PASS
                and evidence.campaign_id is None
            ):
                raise ValueError(
                    "passed campaign stage requires its campaign identity"
                )
            if (
                self.stage_id in _PRODUCT_DATASET_REQUIRED_FOR_PASS
                and evidence.product_id is None
            ):
                raise ValueError(
                    "passed wide-corpus stage requires its product identity"
                )
        if (
            self.state is CapabilityStateV1.DEFERRED_BLOCKED
            and not self.blocking_issues
        ):
            raise ValueError("deferred_blocked requires issue blockers")
        _derive(self, "trader-maturity-row")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["state"] = CapabilityStateV1(values["state"])
        values["evidence_scope"] = CapabilityEvidenceScopeV1(
            values["evidence_scope"]
        )
        values["limitations"] = _array(values["limitations"])
        values["blocking_issues"] = _array(values["blocking_issues"])
        values["evidence"] = TraderMaturityEvidenceV1.from_dict(
            values["evidence"]
        )
        return values


@dataclass(frozen=True, slots=True)
class TraderMaturityMatrixV1(_Record):
    """Exactly sixteen claims bound to, but never extending, capability V1."""

    parent_matrix_id: str
    release_id: str
    dataset_id: str | None
    as_of_utc: str
    catalog: TraderCatalogIdentityV1
    rows: tuple[TraderMaturityRowV1, ...]
    matrix_id: str = ""
    catalog_id: str = TRADER_MATURITY_CATALOG_ID
    claim_kind: str = "non_authoritative_supplement"
    schema_version: ClassVar[str] = "histdatacom.trader-maturity-matrix.v1"
    _ID_FIELD: ClassVar[str] = "matrix_id"

    def __post_init__(self) -> None:
        _identifier(self.parent_matrix_id, "parent_matrix_id")
        _text(self.release_id, "release_id")
        _identifier(self.dataset_id, "dataset_id", optional=True)
        _timestamp(self.as_of_utc, "as_of_utc")
        if (
            type(self.catalog_id) is not str
            or self.catalog_id != TRADER_MATURITY_CATALOG_ID
        ):
            raise ValueError("unsupported trader maturity catalog identity")
        if (
            type(self.claim_kind) is not str
            or self.claim_kind != "non_authoritative_supplement"
        ):
            raise ValueError(
                "trader supplement cannot claim certification authority"
            )
        catalog = _readmit(self.catalog, TraderCatalogIdentityV1)
        object.__setattr__(self, "catalog", catalog)
        if type(self.rows) is not tuple or len(self.rows) != 16:
            raise ValueError("trader matrix requires all sixteen stages")
        rows = tuple(_readmit(row, TraderMaturityRowV1) for row in self.rows)
        if {row.stage_id for row in rows} != set(TRADER_MATURITY_STAGE_IDS):
            raise ValueError("trader matrix duplicates or omits stages")
        for row in rows:
            if (
                row.last_verified_at_utc is not None
                and row.last_verified_at_utc > self.as_of_utc
            ):
                raise ValueError(
                    "verification time is after supplement snapshot"
                )
            if (
                row.state is CapabilityStateV1.EXECUTED_PASSED
                and row.stage_id in _CATALOG_REQUIRED_FOR_PASS
                and catalog.availability
                is not TraderReferenceAvailabilityV1.RETAINED_REFERENCE
            ):
                raise ValueError(
                    "passed catalog-dependent claim requires retained catalog reference"
                )
            if (
                row.state is CapabilityStateV1.EXECUTED_PASSED
                and row.stage_id in _PRODUCT_DATASET_REQUIRED_FOR_PASS
                and self.dataset_id is None
            ):
                raise ValueError(
                    "passed wide-corpus claim requires dataset identity"
                )
        by_id = {row.stage_id: row for row in rows}
        object.__setattr__(
            self,
            "rows",
            tuple(by_id[name] for name in TRADER_MATURITY_STAGE_IDS),
        )
        _derive(self, "trader-maturity-matrix")

    @classmethod
    def _decode(cls, values: dict[str, Any]) -> dict[str, Any]:
        values["catalog"] = TraderCatalogIdentityV1.from_dict(values["catalog"])
        values["rows"] = tuple(
            TraderMaturityRowV1.from_dict(item)
            for item in _array(values["rows"])
        )
        return values


_RECORD_TYPES = (
    TraderCatalogIdentityV1,
    TraderMaturityEvidenceV1,
    TraderMaturityRowV1,
    TraderMaturityMatrixV1,
)


def validate_parent(
    supplement: TraderMaturityMatrixV1, parent: CapabilityMatrixV1
) -> tuple[TraderMaturityMatrixV1, CapabilityMatrixV1]:
    """Readmit exact records and bind identity/scope; confer no authority."""
    supplement = _readmit(supplement, TraderMaturityMatrixV1)
    if type(parent) is not CapabilityMatrixV1:
        raise TypeError("trader supplement requires exact capability V1 parent")
    parent = CapabilityMatrixV1(
        **{field.name: getattr(parent, field.name) for field in fields(parent)}
    )
    if (
        supplement.parent_matrix_id,
        supplement.release_id,
        supplement.dataset_id,
    ) != (parent.matrix_id, parent.release_id, parent.dataset_id):
        raise ValueError("trader supplement parent/release/dataset differs")
    if supplement.as_of_utc < parent.as_of_utc:
        raise ValueError("trader supplement snapshot predates its parent")
    return supplement, parent


def render_trader_maturity_markdown(
    supplement: TraderMaturityMatrixV1, parent: CapabilityMatrixV1
) -> str:
    """Render sixteen declared claims, never a merged certification matrix."""
    supplement, parent = validate_parent(supplement, parent)

    def escape(value: str) -> str:
        return html.escape(value).replace("|", "&#124;")

    lines = [
        "## Trader maturity supplement",
        "",
        "Non-authoritative recorded claims: 16 independent stages alongside the unchanged 93-row capability matrix. No earlier state implies a later pass. No waivers are admitted. References, hashes and retained-availability declarations do not independently verify evidence or authorize certification, promotion or publication.",
        "",
        f"Parent matrix: `{parent.matrix_id}`. Supplement: `{supplement.matrix_id}`.",
        f"Release: {escape(supplement.release_id)}. Dataset: {escape(supplement.dataset_id or 'unmaterialized')}.",
        f"Snapshot as of: `{supplement.as_of_utc}`.",
        f"Catalog source-byte SHA256: `{supplement.catalog.source_json_sha256}`; canonical catalog SHA256: `{supplement.catalog.canonical_catalog_sha256}`.",
        f"Catalog availability: `{supplement.catalog.availability.value}`; source reference: `{supplement.catalog.source_artifact_id or 'absent'}`.",
        "",
        "| Stage | Recorded state | Evidence scope | Exact identity references | Scope and limitations | Blocking issues | Verification time |",
        "|---|---|---|---|---|---|---|",
    ]
    titles = dict(TRADER_MATURITY_CATALOG)
    for row in supplement.rows:
        evidence = row.evidence
        bindings = [f"availability={evidence.availability.value}"]
        bindings.extend(
            f"{name}={getattr(evidence, name)}"
            for name in (
                "implementation_commit",
                "execution_artifact_id",
                "independent_verification_artifact_id",
                "campaign_id",
                "product_id",
            )
            if getattr(evidence, name) is not None
        )
        bindings.extend(evidence.references)
        values = (
            row.stage_id + ": " + titles[row.stage_id],
            row.state.value,
            row.evidence_scope.value,
            "; ".join(bindings),
            "; ".join((row.scope, *row.limitations)),
            ", ".join(f"#{number}" for number in row.blocking_issues)
            or "none declared",
            row.last_verified_at_utc or "absent",
        )
        lines.append(
            "| " + " | ".join(escape(value) for value in values) + " |"
        )
    return "\n".join(lines) + "\n"
