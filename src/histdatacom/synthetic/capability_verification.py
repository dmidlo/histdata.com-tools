"""Closed, fresh capability checks; stored claims and receipts are not authority.

The initial profiles admit mathematical, registry and bounded experiment
evidence. They do not attest external process history, powered eligibility,
product selection, dataset publication,
an untouched holdout, an era audit, or a complete release. Native readers and
recomputations remain authoritative; no caller callback or Boolean is accepted.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from dataclasses import dataclass, fields
from typing import Any, ClassVar

from histdatacom.synthetic.capability_matrix import (
    CAPABILITY_REQUIREMENT_IDS,
    CapabilityEvidenceScopeV1,
    CapabilityMatrixPolicyV1,
    CapabilityMatrixV1,
    required_verification_rows,
    structural_matrix_blockers,
)

MAX_FILES = 256
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_GRAPH_BYTES = 64 * 1024 * 1024
MAX_DECLARATIONS = 32
MAX_RECEIPT_BYTES = 8 * 1024 * 1024
_SUBJECT_FIELDS = {
    "histdatacom.reconstruction-math-verification-report.v1": "report_id",
    "histdatacom.proposal-engine-registry.v1": "registry_id",
    "histdatacom.reconstruction-experiment-manifest.v1": "experiment_id",
}
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_ID = re.compile(r"[a-z][a-z0-9._-]{0,95}:sha256:[0-9a-f]{64}\Z")
_PROFILES = {
    "reference-kernels.v1": "computational_reproducibility.reference_kernels",
    "model-registry.v1": "model_bank_registration",
    "experiment.v1": "source_experiment_identity",
}


def _text(value: Any, label: str, maximum: int = 1024) -> None:
    if (
        type(value) is not str
        or not 1 <= len(value) <= maximum
        or not value.isascii()
        or value.strip() != value
        or any(ord(c) < 32 for c in value)
    ):
        raise ValueError(f"invalid bounded {label}")


def _id(value: Any) -> None:
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValueError("expected content-addressed subject identity")


def _canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _digest(prefix: str, value: Any) -> str:
    return (
        prefix
        + ":sha256:"
        + hashlib.sha256(_canonical(value).encode()).hexdigest()
    )


def _readmit(value: Any, cls: type[Any]) -> Any:
    if type(value) is not cls:
        raise TypeError("capability evidence requires exact record types")
    return cls(**{f.name: getattr(value, f.name) for f in fields(cls)})


@dataclass(frozen=True, slots=True)
class CapabilityEvidenceRefV1:
    """An exact root-relative file; JSON subjects and opaque bytes differ."""

    path: str
    schema_version: str | None
    subject_id: str | None
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        _text(self.path, "relative file path")
        parts = PurePosixPath(self.path).parts
        if (
            not parts
            or self.path.startswith("/")
            or "\\" in self.path
            or any(p in (".", "..") for p in self.path.split("/"))
            or str(PurePosixPath(self.path)) != self.path
        ):
            raise ValueError(
                "evidence path must be canonical and root-relative"
            )
        if type(self.sha256) is not str or _SHA.fullmatch(self.sha256) is None:
            raise ValueError("invalid evidence SHA256")
        if (
            type(self.size_bytes) is not int
            or not 1 <= self.size_bytes <= MAX_FILE_BYTES
        ):
            raise ValueError("evidence byte size exceeds bound")
        if self.schema_version is not None:
            _text(self.schema_version, "schema", 256)
            if self.schema_version not in _SUBJECT_FIELDS:
                raise ValueError(
                    "unsupported typed subject schema; declare other inputs as opaque bytes"
                )
        if self.subject_id is not None:
            _id(self.subject_id)
        if (self.schema_version is None) != (self.subject_id is None):
            raise ValueError("typed JSON requires both schema and subject")

    def to_dict(self) -> dict[str, Any]:
        value = _readmit(self, CapabilityEvidenceRefV1)
        return {f.name: getattr(value, f.name) for f in fields(value)}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> CapabilityEvidenceRefV1:
        if type(value) is not dict or set(value) != {
            f.name for f in fields(cls)
        }:
            raise ValueError("evidence reference fields differ")
        return cls(**value)


@dataclass(frozen=True, slots=True)
class CapabilityEvidenceDeclarationV1:
    """Closed profile and all reachable input bytes, not a verified token."""

    requirement_id: str
    profile_version: str
    execution: CapabilityEvidenceRefV1
    inputs: tuple[CapabilityEvidenceRefV1, ...] = ()

    def __post_init__(self) -> None:
        if self.requirement_id not in CAPABILITY_REQUIREMENT_IDS:
            raise ValueError("unknown capability requirement")
        if _PROFILES.get(self.profile_version) != self.requirement_id:
            raise ValueError("unsupported capability/profile mapping")
        execution = _readmit(self.execution, CapabilityEvidenceRefV1)
        if execution.subject_id is None:
            raise ValueError("execution requires a typed subject")
        if type(self.inputs) is not tuple or len(self.inputs) >= MAX_FILES:
            raise ValueError("bounded exact evidence tuple required")
        inputs = tuple(
            _readmit(item, CapabilityEvidenceRefV1) for item in self.inputs
        )
        refs = (execution, *inputs)
        if len({item.path for item in refs}) != len(refs):
            raise ValueError("duplicate evidence paths")
        if sum(item.size_bytes for item in refs) > MAX_GRAPH_BYTES:
            raise ValueError("evidence graph exceeds byte budget")
        object.__setattr__(self, "execution", execution)
        object.__setattr__(self, "inputs", inputs)

    def to_dict(self) -> dict[str, Any]:
        value = _readmit(self, CapabilityEvidenceDeclarationV1)
        return {
            "requirement_id": value.requirement_id,
            "profile_version": value.profile_version,
            "execution": value.execution.to_dict(),
            "inputs": [item.to_dict() for item in value.inputs],
        }

    @classmethod
    def from_dict(
        cls, value: dict[str, Any]
    ) -> CapabilityEvidenceDeclarationV1:
        if type(value) is not dict or set(value) != {
            f.name for f in fields(cls)
        }:
            raise ValueError("evidence declaration fields differ")
        if type(value["inputs"]) is not list:
            raise TypeError("wire collections require exact lists")
        if len(value["inputs"]) >= MAX_FILES:
            raise ValueError("wire collections exceed bounds")
        return cls(
            value["requirement_id"],
            value["profile_version"],
            CapabilityEvidenceRefV1.from_dict(value["execution"]),
            tuple(
                CapabilityEvidenceRefV1.from_dict(item)
                for item in value["inputs"]
            ),
        )


@dataclass(frozen=True, slots=True)
class CapabilityVerificationOutcomeV1:
    """A fresh result. Constructing or deserializing it conveys no authority."""

    requirement_id: str
    profile_version: str
    execution_artifact_id: str
    independent_verification_artifact_id: str
    evidence_scope: CapabilityEvidenceScopeV1
    passed: bool
    reason: str
    result_json: str

    def __post_init__(self) -> None:
        if _PROFILES.get(self.profile_version) != self.requirement_id:
            raise ValueError("outcome capability/profile differs")
        _id(self.execution_artifact_id)
        _id(self.independent_verification_artifact_id)
        if (
            type(self.evidence_scope) is not CapabilityEvidenceScopeV1
            or type(self.passed) is not bool
        ):
            raise TypeError("outcome scope and state require exact types")
        allowed = (
            CapabilityEvidenceScopeV1.SOFTWARE,
            CapabilityEvidenceScopeV1.BOUNDED,
        )
        if self.passed != (self.evidence_scope in allowed):
            raise ValueError("outcome state and bounded scope differ")
        if (
            not self.passed
            and self.evidence_scope is not CapabilityEvidenceScopeV1.NONE
        ):
            raise ValueError("failed outcome cannot assert evidence scope")
        _text(self.reason, "outcome reason", 512)
        if (
            type(self.result_json) is not str
            or len(self.result_json) > MAX_RECEIPT_BYTES
        ):
            raise ValueError("outcome JSON exceeds bound")
        result = _json(self.result_json.encode("utf-8"))
        if type(result) is not dict or _canonical(result) != self.result_json:
            raise ValueError("outcome result requires canonical object JSON")

    def to_dict(self) -> dict[str, Any]:
        value = _readmit(self, CapabilityVerificationOutcomeV1)
        return {
            **{f.name: getattr(value, f.name) for f in fields(value)},
            "evidence_scope": value.evidence_scope.value,
        }

    @classmethod
    def from_dict(
        cls, value: dict[str, Any]
    ) -> CapabilityVerificationOutcomeV1:
        if type(value) is not dict or set(value) != {
            f.name for f in fields(cls)
        }:
            raise ValueError("outcome fields differ")
        if type(value["evidence_scope"]) is not str:
            raise TypeError("outcome scope wire must be text")
        return cls(
            **{
                **value,
                "evidence_scope": CapabilityEvidenceScopeV1(
                    value["evidence_scope"]
                ),
            }
        )


@dataclass(frozen=True, slots=True)
class CapabilityEvidenceVerificationV1:
    """Bounded fresh outcomes and blockers; never a certification dossier."""

    release_id: str
    dataset_id: str | None
    implementation_id: str
    outcomes: tuple[CapabilityVerificationOutcomeV1, ...]
    blockers: tuple[tuple[str, str], ...]
    verification_id: str
    schema_version: ClassVar[str] = (
        "histdatacom.capability-evidence-verification.v1"
    )

    def __post_init__(self) -> None:
        for item in (
            self.release_id,
            self.implementation_id,
            self.verification_id,
        ):
            _id(item)
        if self.dataset_id is not None:
            _id(self.dataset_id)
        if (
            type(self.outcomes) is not tuple
            or len(self.outcomes) > MAX_DECLARATIONS
        ):
            raise ValueError("receipt outcomes exceed bound")
        outcomes = tuple(
            _readmit(item, CapabilityVerificationOutcomeV1)
            for item in self.outcomes
        )
        names = tuple(item.requirement_id for item in outcomes)
        if names != tuple(sorted(set(names))):
            raise ValueError("receipt outcomes must be sorted and unique")
        if sum(len(item.result_json) for item in outcomes) > MAX_RECEIPT_BYTES:
            raise ValueError("aggregate result JSON exceeds bound")
        if type(self.blockers) is not tuple or len(self.blockers) > 512:
            raise ValueError("receipt blockers exceed bound")
        for pair in self.blockers:
            if type(pair) is not tuple or len(pair) != 2:
                raise TypeError("receipt blocker requires exact pair")
            _text(pair[0], "blocker requirement", 256)
            _text(pair[1], "blocker reason", 1024)
            if pair[0] not in CAPABILITY_REQUIREMENT_IDS and pair[0] not in (
                "__context__",
                "__matrix__",
            ):
                raise ValueError("unknown blocked requirement")
        if self.blockers != tuple(sorted(set(self.blockers))):
            raise ValueError("receipt blockers must be sorted and unique")
        for item in outcomes:
            expected = _row_identity(
                self.release_id,
                item.requirement_id,
                item.profile_version,
                item.execution_artifact_id,
                item.passed,
                item.evidence_scope,
                _json(item.result_json.encode()),
            )
            if expected != item.independent_verification_artifact_id:
                raise ValueError("row verification identity differs")
            if (
                not item.passed
                and (item.requirement_id, item.reason) not in self.blockers
            ):
                raise ValueError("failed verification is missing its blocker")
        object.__setattr__(self, "outcomes", outcomes)
        expected = _digest("capability-evidence-verification", self._payload())
        if expected != self.verification_id:
            raise ValueError("receipt verification identity differs")

    def _payload(self) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "dataset_id": self.dataset_id,
            "implementation_id": self.implementation_id,
            "outcomes": [item.to_dict() for item in self.outcomes],
            "blockers": [list(item) for item in self.blockers],
        }

    def to_dict(self) -> dict[str, Any]:
        value = _readmit(self, CapabilityEvidenceVerificationV1)
        return {
            "schema_version": self.schema_version,
            **value._payload(),
            "verification_id": value.verification_id,
            "full_certification": False,
            "external_execution_attested": False,
        }

    def to_json(self) -> str:
        """Serialize a structural receipt, never execution authority."""
        text = _canonical(self.to_dict())
        if len(text) > MAX_RECEIPT_BYTES:
            raise ValueError("receipt JSON exceeds bound")
        return text

    @classmethod
    def from_dict(
        cls, value: dict[str, Any]
    ) -> CapabilityEvidenceVerificationV1:
        """Re-admit structure and identities; do not verify any external file."""
        expected = {f.name for f in fields(cls)} | {
            "schema_version",
            "full_certification",
            "external_execution_attested",
        }
        if (
            type(value) is not dict
            or set(value) != expected
            or value["schema_version"] != cls.schema_version
        ):
            raise ValueError("receipt fields or schema differ")
        if (
            value["full_certification"] is not False
            or value["external_execution_attested"] is not False
        ):
            raise ValueError(
                "receipt cannot assert certification or external attestation"
            )
        if (
            type(value["outcomes"]) is not list
            or len(value["outcomes"]) > MAX_DECLARATIONS
        ):
            raise ValueError("receipt outcome wire exceeds bound")
        if type(value["blockers"]) is not list or len(value["blockers"]) > 512:
            raise ValueError("receipt blocker wire exceeds bound")
        if any(
            type(item) is not list or len(item) != 2
            for item in value["blockers"]
        ):
            raise ValueError("receipt blocker wire requires list pairs")
        return cls(
            value["release_id"],
            value["dataset_id"],
            value["implementation_id"],
            tuple(
                CapabilityVerificationOutcomeV1.from_dict(item)
                for item in value["outcomes"]
            ),
            tuple(tuple(item) for item in value["blockers"]),
            value["verification_id"],
        )

    @classmethod
    def from_json(cls, text: str) -> CapabilityEvidenceVerificationV1:
        """Parse bounded canonical JSON without conferring authority."""
        if type(text) is not str or len(text) > MAX_RECEIPT_BYTES:
            raise ValueError("receipt JSON exceeds bound")
        result = cls.from_dict(_json(text.encode("utf-8")))
        if result.to_json() != text:
            raise ValueError("receipt JSON is not canonical")
        return result


def _row_identity(
    release: str,
    requirement: str,
    profile: str,
    execution: str,
    passed: bool,
    scope: CapabilityEvidenceScopeV1,
    result: Any,
) -> str:
    return _digest(
        "capability-row-verification",
        {
            "release_id": release,
            "requirement_id": requirement,
            "profile_version": profile,
            "execution": execution,
            "passed": passed,
            "scope": scope.value,
            "result": result,
        },
    )


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _path(root: Path, name: str) -> Path:
    path = Path(name)
    target = path if path.is_absolute() else root / path
    if not target.is_relative_to(root):
        raise ValueError("nested evidence escapes declared root")
    current = root
    for part in target.relative_to(root).parts:
        if part in (".", ".."):
            raise ValueError("nested evidence has ambiguous path")
        current = current / part
        if current.is_symlink():
            raise ValueError("symlink evidence is not admitted")
    if target.resolve() != target:
        raise ValueError("evidence path is not canonical")
    return target


def _read(path: Path, maximum: int) -> tuple[bytes, tuple[int, ...]]:
    initial = path.lstat()
    if not stat.S_ISREG(initial.st_mode):
        raise ValueError("evidence is not a bounded regular file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_NONBLOCK", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            raise ValueError("evidence is not a bounded regular file")
        if _identity(initial) != _identity(before):
            raise ValueError("evidence changed before descriptor admission")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            payload = stream.read(maximum + 1)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        len(payload) > maximum
        or _identity(before) != _identity(after)
        or _identity(after) != _identity(path.stat())
    ):
        raise ValueError("evidence file changed while reading")
    return payload, _identity(after)


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate evidence JSON key")
        result[key] = value
    return result


def _json(payload: bytes) -> Any:
    if len(payload) > MAX_FILE_BYTES:
        raise ValueError("evidence JSON exceeds byte bound")
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(
                ValueError("nonfinite JSON")
            ),
        )
    except (ValueError, UnicodeError, RecursionError) as error:
        raise ValueError("invalid bounded evidence JSON") from error
    pending = [(value, 0)]
    count = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if count > 200000 or depth > 64:
            raise ValueError("evidence JSON traversal exceeds bound")
        if type(item) is dict:
            pending.extend((child, depth + 1) for child in item.values())
        elif type(item) is list:
            pending.extend((child, depth + 1) for child in item)
    return value


def _graph(
    root: Path, refs: tuple[CapabilityEvidenceRefV1, ...]
) -> tuple[dict[Path, Any], dict[Path, tuple[int, ...]]]:
    payloads: dict[Path, Any] = {}
    identities: dict[Path, tuple[int, ...]] = {}
    by_path = {_path(root, ref.path): ref for ref in refs}
    for path, ref in by_path.items():
        payload, identity = _read(path, ref.size_bytes)
        if (
            len(payload) != ref.size_bytes
            or hashlib.sha256(payload).hexdigest() != ref.sha256
        ):
            raise ValueError("declared evidence bytes differ")
        identities[path] = identity
        if ref.schema_version is not None or path.suffix == ".json":
            value = _json(payload)
            if ref.schema_version is not None and (
                type(value) is not dict
                or value.get("schema_version") != ref.schema_version
            ):
                raise ValueError("declared evidence schema differs")
            if (
                ref.schema_version is not None
                and value.get(_SUBJECT_FIELDS[ref.schema_version])
                != ref.subject_id
            ):
                raise ValueError("declared evidence subject differs")
            payloads[path] = value
    for path, value in payloads.items():
        pending = [value]
        while pending:
            item = pending.pop()
            if type(item) is list:
                pending.extend(item)
            elif type(item) is dict:
                pending.extend(item.values())
                nested: Path | None = None
                digest: Any = None
                if {"path", "sha256", "size_bytes"} <= item.keys():
                    if type(item["path"]) is not str:
                        raise ValueError("nested path is not text")
                    if not Path(item["path"]).is_absolute():
                        raise ValueError(
                            "native artifact paths must be canonical absolute paths"
                        )
                    nested = _path(root, item["path"])
                    digest = item["sha256"]
                elif {
                    "relative_path",
                    "byte_sha256",
                    "size_bytes",
                } <= item.keys():
                    if type(item["relative_path"]) is not str:
                        raise ValueError("nested relative path is not text")
                    if Path(item["relative_path"]).is_absolute():
                        raise ValueError(
                            "manifest member paths must be relative"
                        )
                    nested = _path(
                        root, str(path.parent / item["relative_path"])
                    )
                    digest = item["byte_sha256"]
                if nested is not None:
                    declared = by_path.get(nested)
                    if declared is None or (
                        declared.sha256,
                        declared.size_bytes,
                    ) != (
                        digest,
                        item["size_bytes"],
                    ):
                        raise ValueError(
                            "nested artifact is absent or differs from declared graph"
                        )
    return payloads, identities


def _implementation() -> str:
    """Bind current installed Python source bytes, not a caller Git label."""
    root = Path(__file__).resolve().parents[1]
    paths = sorted(root.rglob("*.py"))
    if not 1 <= len(paths) <= 2048:
        raise ValueError("implementation source inventory exceeds bound")
    inventory: dict[str, str] = {}
    total = 0
    for path in paths:
        if path.is_symlink():
            raise ValueError("implementation source symlink refused")
        payload, _ = _read(path, 8 * 1024 * 1024)
        total += len(payload)
        if total > 128 * 1024 * 1024:
            raise ValueError("implementation source bytes exceed bound")
        inventory[str(path.relative_to(root))] = hashlib.sha256(
            payload
        ).hexdigest()
    return _digest("capability-verifier-implementation", inventory)


def _profile(
    declaration: CapabilityEvidenceDeclarationV1,
    root: Path,
    payload: dict[str, Any],
    dataset_id: str | None,
) -> tuple[dict[str, Any], CapabilityEvidenceScopeV1]:
    profile = declaration.profile_version
    scope = CapabilityEvidenceScopeV1.BOUNDED
    subject: str
    result: dict[str, Any]
    if profile == "reference-kernels.v1":
        from histdatacom.reconstruction_math import (
            ReconstructionMathVerificationReportV1,
            current_reconstruction_math_verification_report,
        )

        report = ReconstructionMathVerificationReportV1.from_dict(payload)
        if _canonical(report.to_dict()) != _canonical(payload):
            raise ValueError(
                "math evidence contains noncanonical native fields"
            )
        expected = current_reconstruction_math_verification_report.__wrapped__()
        if report != expected or not expected.passed:
            raise ValueError(
                "math report differs from fresh reference verification"
            )
        subject = report.report_id
        result = report.to_dict()
        scope = CapabilityEvidenceScopeV1.SOFTWARE
    elif profile == "model-registry.v1":
        from histdatacom.synthetic.proposal_engines import (
            ProposalEngineRegistryV1,
            proposal_engine_registry,
        )

        registry = ProposalEngineRegistryV1.from_dict(payload)
        if _canonical(registry.to_dict()) != _canonical(payload):
            raise ValueError(
                "registry evidence contains noncanonical native fields"
            )
        if registry != proposal_engine_registry():
            raise ValueError(
                "model registry differs from current executable registry"
            )
        subject = registry.registry_id
        result = registry.to_dict()
        scope = CapabilityEvidenceScopeV1.SOFTWARE
    elif profile == "experiment.v1":
        from histdatacom.reconstruction_experiment import (
            ReconstructionExperimentManifestV1,
            verify_reconstruction_experiment,
        )

        experiment = ReconstructionExperimentManifestV1.from_dict(payload)
        if _canonical(experiment.to_dict()) != _canonical(payload):
            raise ValueError(
                "experiment evidence contains noncanonical native fields"
            )
        if dataset_id is None or set(experiment.dataset_version_ids) != {
            dataset_id
        }:
            raise ValueError("experiment immutable dataset identity differs")
        verification = verify_reconstruction_experiment(
            experiment, require_current_implementation=True
        )
        if not verification.verified:
            raise ValueError("experiment source verification failed")
        subject = experiment.experiment_id
        result = verification.to_dict()
    else:
        raise ValueError("no closed verifier for this profile")
    if subject != declaration.execution.subject_id:
        raise ValueError("typed execution subject identity differs")
    return result, scope


def verify_capability_execution(
    declarations: tuple[CapabilityEvidenceDeclarationV1, ...],
    *,
    evidence_root: str | Path,
    dataset_id: str | None,
) -> CapabilityEvidenceVerificationV1:
    """Perform real closed checks and derive fresh bounded evidence identities.

    This is also the preparation route for truthful matrix row identities.
    No result from a previous call is accepted as an input authority token.
    """
    if type(declarations) is not tuple or len(declarations) > MAX_DECLARATIONS:
        raise ValueError("declarations require a bounded exact tuple")
    items = tuple(
        _readmit(item, CapabilityEvidenceDeclarationV1) for item in declarations
    )
    if len({item.requirement_id for item in items}) != len(items):
        raise ValueError("duplicate capability evidence declarations")
    if dataset_id is not None:
        _id(dataset_id)
    root = Path(evidence_root).absolute()
    if root.is_symlink() or root.resolve() != root or not root.is_dir():
        raise ValueError(
            "evidence root must be an existing canonical directory"
        )
    refs_by_path: dict[str, CapabilityEvidenceRefV1] = {}
    for item in items:
        for ref in (item.execution, *item.inputs):
            prior = refs_by_path.setdefault(ref.path, ref)
            if prior != ref:
                raise ValueError("conflicting graph declarations")
    refs = tuple(refs_by_path[name] for name in sorted(refs_by_path))
    if (
        len(refs) > MAX_FILES
        or sum(ref.size_bytes for ref in refs) > MAX_GRAPH_BYTES
    ):
        raise ValueError("aggregate evidence graph exceeds budget")
    # Bind ancestor identities too: replacing a directory A->B->A can preserve
    # every original file's identity but still change native path consumption.
    directory_paths = {root}
    for ref in refs:
        current = _path(root, ref.path).parent
        while current.is_relative_to(root):
            directory_paths.add(current)
            if current == root:
                break
            current = current.parent
    directories = {path: _identity(path.stat()) for path in directory_paths}
    payloads, identities = _graph(root, refs)
    implementation = _implementation()
    release = _digest(
        "capability-evidence-release",
        {
            "implementation_id": implementation,
            "dataset_id": dataset_id,
            "declarations": [
                item.to_dict()
                for item in sorted(items, key=lambda x: x.requirement_id)
            ],
        },
    )
    outcomes: list[CapabilityVerificationOutcomeV1] = []
    blockers: list[tuple[str, str]] = []
    for item in sorted(items, key=lambda x: x.requirement_id):
        payload = payloads[_path(root, item.execution.path)]
        try:
            result, scope = _profile(item, root, payload, dataset_id)
            passed, reason = True, "fresh closed verification passed"
        except (ValueError, TypeError, OSError, KeyError) as error:
            result = {}
            scope = CapabilityEvidenceScopeV1.NONE
            passed, reason = (
                False,
                "closed_verification_failed:" + type(error).__name__,
            )
            blockers.append((item.requirement_id, reason))
        verification = _row_identity(
            release,
            item.requirement_id,
            item.profile_version,
            str(item.execution.subject_id),
            passed,
            scope,
            result,
        )
        outcomes.append(
            CapabilityVerificationOutcomeV1(
                item.requirement_id,
                item.profile_version,
                str(item.execution.subject_id),
                verification,
                scope,
                passed,
                reason,
                _canonical(result),
            )
        )
    for ref in refs:
        path = _path(root, ref.path)
        payload, identity = _read(path, ref.size_bytes)
        if (
            identity != identities[path]
            or hashlib.sha256(payload).hexdigest() != ref.sha256
        ):
            raise ValueError("evidence graph changed during fresh verification")
    if _implementation() != implementation:
        raise ValueError("verifier source changed during verification")
    if any(
        path.is_symlink() or _identity(path.stat()) != identity
        for path, identity in directories.items()
    ):
        raise ValueError(
            "evidence ancestor directory changed during verification"
        )
    return _result(
        release, dataset_id, implementation, tuple(outcomes), tuple(blockers)
    )


def _result(
    release: str,
    dataset: str | None,
    implementation: str,
    outcomes: tuple[CapabilityVerificationOutcomeV1, ...],
    blockers: tuple[tuple[str, str], ...],
) -> CapabilityEvidenceVerificationV1:
    identity = _digest(
        "capability-evidence-verification",
        {
            "release_id": release,
            "dataset_id": dataset,
            "implementation_id": implementation,
            "outcomes": [item.to_dict() for item in outcomes],
            "blockers": sorted(set(blockers)),
        },
    )
    return CapabilityEvidenceVerificationV1(
        release,
        dataset,
        implementation,
        outcomes,
        tuple(sorted(set(blockers))),
        identity,
    )


def verify_capability_evidence(
    matrix: CapabilityMatrixV1,
    policy: CapabilityMatrixPolicyV1,
    *,
    evidence_root: str | Path,
    declarations: tuple[CapabilityEvidenceDeclarationV1, ...],
) -> CapabilityEvidenceVerificationV1:
    """Reverify every required passed claim; unsupported evidence blocks it."""
    if (
        type(matrix) is not CapabilityMatrixV1
        or type(policy) is not CapabilityMatrixPolicyV1
    ):
        raise TypeError("exact matrix and policy required")
    matrix = CapabilityMatrixV1.from_dict(matrix.to_dict())
    policy = CapabilityMatrixPolicyV1.from_dict(policy.to_dict())
    structural = structural_matrix_blockers(matrix, policy)
    rows = required_verification_rows(matrix, policy)
    if type(declarations) is not tuple or len(declarations) > MAX_DECLARATIONS:
        raise ValueError("declarations require a bounded exact tuple")
    declarations = tuple(
        _readmit(item, CapabilityEvidenceDeclarationV1) for item in declarations
    )
    if any(
        item.requirement_id
        not in {req.requirement_id for req in policy.requirements}
        for item in declarations
    ):
        raise ValueError("declaration is outside selected policy")
    checked = verify_capability_execution(
        declarations, evidence_root=evidence_root, dataset_id=matrix.dataset_id
    )
    blockers = [
        *checked.blockers,
        *((item.requirement_id, item.reason) for item in structural),
    ]
    if (checked.release_id, checked.dataset_id) != (
        matrix.release_id,
        matrix.dataset_id,
    ):
        blockers.append(
            ("__context__", "current evidence release/dataset differs")
        )
    by_id = {item.requirement_id: item for item in checked.outcomes}
    for row in rows:
        proof = by_id.get(row.requirement_id)
        if proof is None:
            blockers.append(
                (
                    row.requirement_id,
                    "closed verifier or required evidence unavailable",
                )
            )
        elif not proof.passed or (
            proof.execution_artifact_id,
            proof.independent_verification_artifact_id,
            proof.evidence_scope,
        ) != (
            row.execution_artifact_id,
            row.independent_verification_artifact_id,
            row.evidence_scope,
        ):
            blockers.append(
                (
                    row.requirement_id,
                    "fresh evidence identity or scope differs from row",
                )
            )
    return _result(
        checked.release_id,
        checked.dataset_id,
        checked.implementation_id,
        checked.outcomes,
        tuple(blockers),
    )
