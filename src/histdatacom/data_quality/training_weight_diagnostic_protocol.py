"""Separate diagnostic protocol and explicit, plan-bound workflow approval.

All input constructors are metadata-only. Neither an installed asset nor a
structurally valid receipt proves source bytes, Git ancestry or authorization.
The coordinator must review and record the actual committed protocol and final
runtime separately. Fresh package hashing is an on-disk operational checkpoint,
not proof of loaded bytecode or a hostile same-user security boundary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib
from importlib import resources
import os
from pathlib import Path, PurePosixPath
import re
import stat
import subprocess
from typing import ClassVar

from .training_contracts import TrainingContract, _check, training_json
from .training_contracts import training_load
from .training_weight_approval import _stamp, training_weight_source_fingerprint
from .training_weight_candidates import TrainingWeightFileV1
from .training_weight_contracts import (
    PREREGISTRATION_FILE_SHA256,
    _array,
    _object,
    _preregistered_input_hashes,
    _text,
    read_training_weight_preregistration,
)

DIAGNOSTIC_PROTOCOL_FILENAME = "training_weight_diagnostic_protocol_v1.json"
DIAGNOSTIC_PROTOCOL_FILE_SHA256 = (
    "ac4f4be728cf000b789bff057778cda44bfabef84d1a10b2548018f5ad21d098"
)
DIAGNOSTIC_PROTOCOL_CANONICAL_SHA256 = (
    "c6452d829a91afa16d2fce5a9ef75094e59ba9a80cf38579c95a95ab2b40c1df"
)
DIAGNOSTIC_PROTOCOL_GIT_BLOB_SHA1 = "23f4e7a6b3216462d1d2cf2b92d6695a49696328"
DIAGNOSTIC_PROTOCOL_ID = (
    "training-weight-diagnostic-protocol:sha256:"
    + DIAGNOSTIC_PROTOCOL_CANONICAL_SHA256
)
DIAGNOSTIC_ALLOWED_OPERATIONS = (
    "diagnostic-artifact-replay",
    "diagnostic-generator-trace",
    "diagnostic-input-verification",
    "diagnostic-source-census",
)
MAX_DIAGNOSTIC_BYTES = 8 * 1024 * 1024
MAX_DIAGNOSTIC_NODES = 131_072
MAX_DIAGNOSTIC_METADATA_BYTES = 64 * 1024
DIAGNOSTIC_FIXTURE_COMPONENT = "training-weight-diagnostic-fixture"
DIAGNOSTIC_SYMBOLS = ("EURGBP", "EURUSD", "GBPUSD")
DIAGNOSTIC_PERIODS = ("201001", "201101", "201102")
_ASSET_GIT_PATH = (
    "src/histdatacom/data_quality/assets/" + DIAGNOSTIC_PROTOCOL_FILENAME
)


def diagnostic_json(value: object) -> str:
    """Canonical JSON, including one shared 131072-node traversal budget.

    Embedded JSON strings count as strings here. A producer retaining encoded
    native artifacts must additionally charge their decoded/reconstructed nodes.
    """
    _check(value, budget=[MAX_DIAGNOSTIC_BYTES, MAX_DIAGNOSTIC_NODES])
    return training_json(value)


def diagnostic_load(text: str) -> dict[str, object]:
    """Read bounded strict JSON; callers still validate their closed schema."""
    value = training_load(text)
    diagnostic_json(value)
    return value


class TrainingWeightDiagnosticEvidenceKind(str, Enum):
    DIAGNOSTIC = "exploratory_diagnostic_not_attempt001_replay_or_confirmation"
    FIXTURE = "synthetic_diagnostic_fixture_not_empirical_evidence"


class TrainingWeightDiagnosticInputRole(str, Enum):
    HISTORICAL_SOURCE = "historical-source"
    MODEL_INDEX = "fixed-index"
    MODEL_TRAINING_SOURCE = "model-training-source"
    EPOCH = "epoch-model"


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticProtocolV1:
    """Exact frozen diagnostic design; never a default execution grant."""

    protocol_json: str
    _canonical: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.protocol_json) is not str
            or len(self.protocol_json) > MAX_DIAGNOSTIC_METADATA_BYTES
        ):
            raise ValueError("diagnostic protocol exceeds metadata bound")
        canonical = diagnostic_json(diagnostic_load(self.protocol_json))
        if (
            hashlib.sha256(canonical.encode("ascii")).hexdigest()
            != DIAGNOSTIC_PROTOCOL_CANONICAL_SHA256
        ):
            raise ValueError("diagnostic protocol differs from frozen design")
        object.__setattr__(self, "protocol_json", canonical)
        object.__setattr__(self, "_canonical", canonical)

    @property
    def artifact_id(self) -> str:
        return DIAGNOSTIC_PROTOCOL_ID

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def execution_approved(self) -> bool:
        return False

    def to_json(self) -> str:
        return self._canonical

    def to_dict(self) -> dict[str, object]:
        return diagnostic_load(self._canonical)

    @classmethod
    def from_dict(
        cls, value: dict[str, object]
    ) -> TrainingWeightDiagnosticProtocolV1:
        return cls(diagnostic_json(value))

    @classmethod
    def from_json(cls, text: str) -> TrainingWeightDiagnosticProtocolV1:
        return cls(text)

    def scheduled_dates(self, period: str | None = None) -> tuple[str, ...]:
        if period is not None and period not in DIAGNOSTIC_PERIODS:
            raise ValueError("unknown diagnostic period")
        return tuple(
            _text(day)
            for item in _array(self.to_dict()["schedule"])
            if period is None or _object(item)["period"] == period
            for day in _array(_object(item)["dates"])
        )

    def selected_dates(self) -> tuple[str, ...]:
        generation = _object(self.to_dict()["generator_diagnosis"])
        return tuple(
            _text(_object(item)["date"])
            for item in _array(generation["selected_days"])
        )


def read_training_weight_diagnostic_protocol() -> (
    TrainingWeightDiagnosticProtocolV1
):
    """Read package assets only, never any declared source/model locator."""
    asset = (
        resources.files("histdatacom.data_quality")
        .joinpath("assets")
        .joinpath(DIAGNOSTIC_PROTOCOL_FILENAME)
    )
    with asset.open("rb") as stream:
        data = stream.read(MAX_DIAGNOSTIC_METADATA_BYTES + 1)
    if (
        len(data) > MAX_DIAGNOSTIC_METADATA_BYTES
        or hashlib.sha256(data).hexdigest() != DIAGNOSTIC_PROTOCOL_FILE_SHA256
    ):
        raise ValueError("installed diagnostic protocol file differs")
    original = read_training_weight_preregistration()
    result = TrainingWeightDiagnosticProtocolV1(data.decode("ascii"))
    if (
        _object(result.to_dict()["original_protocol"])["file_sha256"]
        != PREREGISTRATION_FILE_SHA256
        or original.execution_approved
    ):
        raise ValueError("original policy binding differs")
    return result


def _relative_path(value: str) -> PurePosixPath:
    result = PurePosixPath(value)
    if (
        not value
        or len(value) > 512
        or result.is_absolute()
        or result.as_posix() != value
        or any(part in ("", ".", "..") for part in result.parts)
        or "\\" in value
        or any(ord(c) < 32 for c in value)
    ):
        raise ValueError("diagnostic relative path is not canonical")
    return result


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticInputV1(TrainingContract):
    """Exact declared file identity; constructing this never reads the file."""

    KIND: ClassVar[str] = "weight-diagnostic-input"
    role: TrainingWeightDiagnosticInputRole
    relative_path: str
    file: TrainingWeightFileV1
    symbol: str = ""
    period: str = ""
    row_count: int = 0

    def _validate(self) -> None:
        relative = _relative_path(self.relative_path)
        locator = Path(self.file.path)
        if (
            not locator.is_absolute()
            or ".." in locator.parts
            or str(locator) != self.file.path
        ):
            raise ValueError("diagnostic locator must be canonical absolute")
        source = self.role in (
            TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE,
            TrainingWeightDiagnosticInputRole.MODEL_TRAINING_SOURCE,
        )
        if source:
            if (
                self.symbol not in DIAGNOSTIC_SYMBOLS
                or re.fullmatch(r"[0-9]{4}(0[1-9]|1[0-2])", self.period) is None
            ):
                raise ValueError("diagnostic source symbol or period differs")
            if self.role is TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE:
                expected = (
                    f"{self.symbol.lower()}/{self.period[:4]}/"
                    f"{int(self.period[4:])}/.data"
                )
                if (
                    self.period not in DIAGNOSTIC_PERIODS
                    or self.relative_path != expected
                    or locator.parts[-len(relative.parts) :] != relative.parts
                    or not 1 <= self.row_count <= 2_000_000
                ):
                    raise ValueError("historical source slot differs")
            elif self.row_count != 0:
                raise ValueError("TRAIN row count is not a declared claim")
        elif self.symbol or self.period or self.row_count:
            raise ValueError("model metadata has unexpected source fields")
        if (
            self.role is TrainingWeightDiagnosticInputRole.MODEL_INDEX
            and self.file.size_bytes > 2 * 1024 * 1024
        ):
            raise ValueError("diagnostic index exceeds fixed bound")


def _known_inputs() -> dict[tuple[str, str], dict[str, object]]:
    """Metadata-only allowlist from the original digest-checked asset."""
    policy = read_training_weight_preregistration().to_dict()
    result: dict[tuple[str, str], dict[str, object]] = {}
    for raw in _array(policy["source_partitions"]):
        item = _object(raw)
        result[("historical-source", _text(item["relative_path"]))] = item
    model = _object(policy["fixed_model"])
    for raw in _array(model["training_sources"]):
        item = _object(raw)
        result[("model-training-source", _text(item["path"]))] = {
            **item,
            **_object(item["metadata"]),
            "row_count": 0,
        }
    result[("fixed-index", _text(model["index_path"]))] = {
        "sha256": model["index_sha256"],
        "size_bytes": model["index_size_bytes"],
        "symbol": "",
        "period": "",
        "row_count": 0,
    }
    epoch = _object(policy["epoch_mapping"])
    result[("epoch-model", _text(epoch["path"]))] = {
        "sha256": epoch["sha256"],
        "size_bytes": 246353,
        "symbol": "",
        "period": "",
        "row_count": 0,
    }
    return result


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticPlanV1(TrainingContract):
    """Complete exact attempt inventory; not scientific input qualification.

    The attempt label is part of the identity. A verification replay needs another
    label, output directory and separately recorded approval, never implicit retry.
    """

    KIND: ClassVar[str] = "weight-diagnostic-plan"
    evidence_kind: TrainingWeightDiagnosticEvidenceKind
    attempt_label: str
    inputs: tuple[TrainingWeightDiagnosticInputV1, ...]
    protocol_id: str = DIAGNOSTIC_PROTOCOL_ID

    def _validate(self) -> None:
        if (
            self.protocol_id != DIAGNOSTIC_PROTOCOL_ID
            or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,95}", self.attempt_label)
            is None
        ):
            raise ValueError("diagnostic protocol or attempt label differs")
        keys = tuple(
            (item.role.value, item.relative_path) for item in self.inputs
        )
        if not keys or keys != tuple(sorted(set(keys))):
            raise ValueError("diagnostic inputs must be unique and sorted")
        if len({item.file.path for item in self.inputs}) != len(self.inputs):
            raise ValueError("diagnostic file locators must be distinct")
        historical = self.inputs_for(
            TrainingWeightDiagnosticInputRole.HISTORICAL_SOURCE
        )
        if {(item.period, item.symbol) for item in historical} != {
            (period, symbol)
            for period in DIAGNOSTIC_PERIODS
            for symbol in DIAGNOSTIC_SYMBOLS
        } or len(historical) != 9:
            raise ValueError("diagnostic requires all nine historical sources")
        if any(
            sum(item.row_count for item in historical if item.period == period)
            > 2_000_000
            for period in DIAGNOSTIC_PERIODS
        ):
            raise ValueError("diagnostic month row total exceeds bound")
        training = self.inputs_for(
            TrainingWeightDiagnosticInputRole.MODEL_TRAINING_SOURCE
        )
        if sum(item.file.size_bytes for item in training) > 2 * 1024**3:
            raise ValueError("diagnostic model-source total exceeds bound")
        if (
            self.evidence_kind
            is TrainingWeightDiagnosticEvidenceKind.DIAGNOSTIC
        ):
            expected = _known_inputs()
            if set(keys) != set(expected):
                raise ValueError("diagnostic input allowlist differs")
            for item in self.inputs:
                declared = expected[(item.role.value, item.relative_path)]
                if (
                    item.file.sha256 != declared["sha256"]
                    or item.file.size_bytes != declared["size_bytes"]
                    or item.symbol != declared["symbol"]
                    or item.period != declared["period"]
                    or item.row_count != declared["row_count"]
                ):
                    raise ValueError("diagnostic input identity differs")
        else:
            known = _preregistered_input_hashes()
            if (
                len(self.inputs) != 13
                or len(training) != 3
                or {item.symbol for item in training} != set(DIAGNOSTIC_SYMBOLS)
                or len(
                    self.inputs_for(
                        TrainingWeightDiagnosticInputRole.MODEL_INDEX
                    )
                )
                != 1
                or self.inputs_for(TrainingWeightDiagnosticInputRole.EPOCH)
                or any(
                    item.file.sha256 in known
                    or DIAGNOSTIC_FIXTURE_COMPONENT
                    not in Path(item.file.path).parts
                    for item in self.inputs
                )
            ):
                raise ValueError(
                    "fixture inventory or empirical relabel differs"
                )
        if len(diagnostic_json(self.to_dict())) > MAX_DIAGNOSTIC_METADATA_BYTES:
            raise ValueError("diagnostic plan exceeds metadata bound")

    def inputs_for(
        self, role: TrainingWeightDiagnosticInputRole
    ) -> tuple[TrainingWeightDiagnosticInputV1, ...]:
        if type(role) is not TrainingWeightDiagnosticInputRole:
            raise ValueError("diagnostic input lookup needs a typed role")
        return tuple(item for item in self.inputs if item.role is role)


@dataclass(frozen=True, slots=True)
class TrainingWeightDiagnosticApprovalV1(TrainingContract):
    """Explicit coordinator declaration, not an installed Git attestation.

    ``protocol_commit`` is supplied only after the new asset is committed. The
    local Git verifier is optional tooling for that coordinator review; the
    installed gate cannot establish ancestry from this declaration alone.
    """

    KIND: ClassVar[str] = "weight-diagnostic-approval"
    protocol_commit: str
    protocol_file_sha256: str
    protocol_git_blob_sha1: str
    source_fingerprint: str
    plan_id: str
    allowed_operations: tuple[str, ...]
    coordinator_label: str
    approved_at_utc: str
    protocol_id: str = DIAGNOSTIC_PROTOCOL_ID

    def _validate(self) -> None:
        if (
            re.fullmatch(r"[0-9a-f]{40}", self.protocol_commit) is None
            or self.protocol_commit == "0" * 40
            or self.protocol_file_sha256 != DIAGNOSTIC_PROTOCOL_FILE_SHA256
            or self.protocol_git_blob_sha1 != DIAGNOSTIC_PROTOCOL_GIT_BLOB_SHA1
            or self.protocol_id != DIAGNOSTIC_PROTOCOL_ID
            or re.fullmatch(r"[0-9a-f]{64}", self.source_fingerprint) is None
            or re.fullmatch(
                r"training-weight-diagnostic-plan:sha256:[0-9a-f]{64}",
                self.plan_id,
            )
            is None
        ):
            raise ValueError("diagnostic approval binding differs")
        if (
            not self.allowed_operations
            or self.allowed_operations
            != tuple(sorted(set(self.allowed_operations)))
            or not set(self.allowed_operations)
            <= set(DIAGNOSTIC_ALLOWED_OPERATIONS)
        ):
            raise ValueError(
                "diagnostic approval has unknown or unordered operations"
            )
        if (
            re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9._ -]{0,127}", self.coordinator_label
            )
            is None
            or self.coordinator_label != self.coordinator_label.strip()
            or re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z",
                self.approved_at_utc,
            )
            is None
        ):
            raise ValueError(
                "diagnostic coordinator or UTC time is not canonical"
            )
        try:
            datetime.strptime(self.approved_at_utc, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            raise ValueError(
                "diagnostic approval UTC time is invalid"
            ) from None
        if len(diagnostic_json(self.to_dict())) > MAX_DIAGNOSTIC_METADATA_BYTES:
            raise ValueError("diagnostic approval exceeds metadata bound")


def require_training_weight_diagnostic_approval(
    receipt: TrainingWeightDiagnosticApprovalV1 | None,
    operation: str,
    plan: TrainingWeightDiagnosticPlanV1,
) -> None:
    """Fail before input I/O; fixtures are explicitly nonempirical and unapproved.

    Real operations always require this new exact plan-specific receipt and a fresh
    whole-package/runtime fingerprint. No old weighting operation is admitted.
    """
    if (
        type(plan) is not TrainingWeightDiagnosticPlanV1
        or type(operation) is not str
        or operation not in DIAGNOSTIC_ALLOWED_OPERATIONS
    ):
        raise ValueError("unsupported diagnostic plan or operation")
    TrainingWeightDiagnosticPlanV1.from_json(plan.to_json())
    protocol = read_training_weight_diagnostic_protocol()
    if protocol.execution_approved:
        raise ValueError("diagnostic protocol must never contain approval")
    if plan.evidence_kind is TrainingWeightDiagnosticEvidenceKind.FIXTURE:
        if receipt is not None:
            raise ValueError(
                "synthetic diagnostic fixtures cannot claim approval"
            )
        return
    if type(receipt) is not TrainingWeightDiagnosticApprovalV1:
        raise ValueError("explicit diagnostic coordinator approval is required")
    TrainingWeightDiagnosticApprovalV1.from_json(receipt.to_json())
    if receipt.plan_id != plan.artifact_id:
        raise ValueError(
            "diagnostic approval belongs to another attempt or input plan"
        )
    if operation not in receipt.allowed_operations:
        raise ValueError("diagnostic operation is outside coordinator approval")
    if receipt.source_fingerprint != training_weight_source_fingerprint():
        raise ValueError("diagnostic approval package or runtime is stale")


def read_training_weight_diagnostic_approval(
    path: str | Path,
) -> TrainingWeightDiagnosticApprovalV1:
    """Read only a caller-supplied bounded no-follow canonical receipt."""
    target = Path(path)
    info = target.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or not 0 < info.st_size <= MAX_DIAGNOSTIC_METADATA_BYTES
    ):
        raise ValueError("diagnostic approval is not a bounded regular file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    with os.fdopen(os.open(target, flags), "rb") as stream:
        if _stamp(os.fstat(stream.fileno())) != _stamp(info):
            raise ValueError("diagnostic approval changed before read")
        data = stream.read(MAX_DIAGNOSTIC_METADATA_BYTES + 1)
        if _stamp(os.fstat(stream.fileno())) != _stamp(info):
            raise ValueError("diagnostic approval changed during read")
    if (
        _stamp(target.lstat()) != _stamp(info)
        or target.is_symlink()
        or len(data) != info.st_size
    ):
        raise ValueError("diagnostic approval changed after read")
    result = TrainingWeightDiagnosticApprovalV1.from_json(data.decode("ascii"))
    if result.to_json().encode("ascii") != data:
        raise ValueError("diagnostic approval file is not canonical")
    return result


def verify_training_weight_diagnostic_protocol_commit(
    repository: str | Path, commit: str
) -> None:
    """Optional read-only local Git content check; never creates an approval.

    This checks that the named commit contains the exact protocol blob. It does not
    prove review, a branch ancestry policy, a signature or installed-runtime origin.
    """
    if type(commit) is not str or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError(
            "diagnostic protocol commit must be a full Git object ID"
        )

    def git(*arguments: str) -> bytes:
        try:
            return subprocess.run(
                ["git", "-C", str(repository), *arguments],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                timeout=5,
                env={**os.environ, "GIT_NO_REPLACE_OBJECTS": "1"},
            ).stdout
        except (OSError, subprocess.SubprocessError):
            raise ValueError(
                "local diagnostic protocol Git verification failed"
            ) from None

    if git("cat-file", "-t", commit).strip() != b"commit":
        raise ValueError("diagnostic protocol reference is not a commit")
    blob = git("rev-parse", f"{commit}:{_ASSET_GIT_PATH}").strip()
    if blob != DIAGNOSTIC_PROTOCOL_GIT_BLOB_SHA1.encode("ascii"):
        raise ValueError("committed diagnostic protocol blob differs")
    size = git("cat-file", "-s", blob.decode("ascii")).strip()
    if not size.isdigit() or not 0 < int(size) <= MAX_DIAGNOSTIC_METADATA_BYTES:
        raise ValueError("committed diagnostic protocol exceeds bound")
    data = git("cat-file", "blob", blob.decode("ascii"))
    if (
        len(data) != int(size)
        or hashlib.sha256(data).hexdigest() != DIAGNOSTIC_PROTOCOL_FILE_SHA256
    ):
        raise ValueError("committed diagnostic protocol bytes differ")
