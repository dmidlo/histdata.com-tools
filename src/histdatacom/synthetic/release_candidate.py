"""Exact, installable reconstruction release-candidate governance.

This module lifts the scientific graph frozen by :mod:`release_holdout` into
one content-addressed release candidate.  The candidate binds source control,
build products, runtime dependencies, scientific artifacts, storage roots,
validation receipts, and branch governance.  Qualification evidence is
candidate-scoped so evidence from one candidate cannot certify another.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.release_holdout import (
    read_protected_release_holdout_manifest,
    read_release_candidate_freeze,
)

RELEASE_CANDIDATE_GIT_IDENTITY_SCHEMA_VERSION = (
    "histdatacom.release-candidate-git-identity.v1"
)
RELEASE_CANDIDATE_BUILD_SET_SCHEMA_VERSION = (
    "histdatacom.release-candidate-build-set.v1"
)
RELEASE_CANDIDATE_RUNTIME_IDENTITY_SCHEMA_VERSION = (
    "histdatacom.release-candidate-runtime-identity.v1"
)
RELEASE_CANDIDATE_FILESYSTEM_ROOT_SCHEMA_VERSION = (
    "histdatacom.release-candidate-filesystem-root.v1"
)
RELEASE_CANDIDATE_VALIDATION_GATE_SCHEMA_VERSION = (
    "histdatacom.release-candidate-validation-gate.v1"
)
RELEASE_CANDIDATE_BRANCH_GOVERNANCE_SCHEMA_VERSION = (
    "histdatacom.release-candidate-branch-governance.v1"
)
RELEASE_CANDIDATE_DEPENDENCY_SCHEMA_VERSION = (
    "histdatacom.release-candidate-dependency.v1"
)
RECONSTRUCTION_RELEASE_CANDIDATE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-release-candidate.v1"
)
RELEASE_CANDIDATE_ARTIFACT_BINDING_SCHEMA_VERSION = (
    "histdatacom.release-candidate-artifact-binding.v1"
)

MAX_RELEASE_CANDIDATE_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_RELEASE_CANDIDATE_ITEMS = 4096

REQUIRED_RELEASE_CANDIDATE_DEPENDENCIES = frozenset(
    {
        "adaptive_window_policy",
        "alignment_policy",
        "benchmark_corpus",
        "candidate_graph",
        "carving_policy",
        "certification_policy",
        "cftc_positioning",
        "dataset_catalog",
        "experiment_manifest",
        "feed_epoch_definition",
        "market_context",
        "observation_operator",
        "observation_scenario_registry",
        "product_selection_dossier",
        "proposal_evaluation",
        "protected_release_holdout",
        "powered_qualification_dossier",
        "reconciliation_policy",
        "schema_registry",
        "scientific_ledger",
        "selected_engine_config",
        "selected_engine_fit",
        "storage_policy",
    }
)
REQUIRED_RELEASE_CANDIDATE_ROOTS = frozenset(
    {"artifact", "output", "checkpoint", "scratch"}
)
REQUIRED_RELEASE_CANDIDATE_GATES = frozenset(
    {
        "build_metadata",
        "cli_api_schema_discovery",
        "critical_branch_coverage",
        "docs_warnings_as_errors",
        "full_pre_commit",
        "full_test_suite",
        "isolated_install_linux",
        "isolated_install_macos",
        "isolated_install_windows",
        "local_simple_registry",
        "path_independence",
        "seven_stage_registration",
        "temporal_extra_install",
        "typing",
        "wheel_sdist_build",
    }
)
REQUIRED_RELEASE_CANDIDATE_RUNTIME_DEPENDENCIES = frozenset(
    {
        "certifi",
        "numpy",
        "polars",
        "pyarrow",
        "pytz",
        "pyyaml",
        "requests",
        "rich",
        "rx",
        "temporalio",
        "tzdata",
    }
)
REQUIRED_RELEASE_CANDIDATE_COMMANDS = frozenset(
    {
        "histdatacom reconstruction cancel-set",
        "histdatacom reconstruction resume-set",
        "histdatacom reconstruction run-set",
        "histdatacom reconstruction schemas --json",
        "histdatacom reconstruction status-set",
        "histdatacom-orchestration-worker",
    }
)
REQUIRED_RELEASE_CANDIDATE_FORBIDDEN_FALLBACKS = frozenset(
    {
        "direct_stage_handler_execution",
        "holdout_driven_selection_or_tuning",
        "mutable_scientific_configuration",
        "notebook_orchestration",
        "repository_relative_undocumented_path",
        "silent_local_runtime_fallback",
        "unregistered_worker_handler",
    }
)

_SHA256 = re.compile(r"[0-9a-f]{64}")
_GIT_OBJECT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})")
_SEMVER = re.compile(
    r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?"
)


@dataclass(frozen=True, slots=True)
class ReleaseCandidateGitIdentityV1:
    """Clean repository state from which one candidate is built."""

    repository_url: str
    ref_name: str
    commit_sha: str
    tree_sha: str
    captured_at_utc: str
    clean_tree: bool = True
    identity_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_GIT_IDENTITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_CANDIDATE_GIT_IDENTITY_SCHEMA_VERSION,
        )
        for name in ("repository_url", "ref_name"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "commit_sha", _git_object_id(self.commit_sha))
        object.__setattr__(self, "tree_sha", _git_object_id(self.tree_sha))
        object.__setattr__(
            self, "captured_at_utc", _timestamp(self.captured_at_utc)
        )
        if self.clean_tree is not True:
            raise ValueError("release candidate requires a clean Git tree")
        expected = _stable_id("release-candidate-git", self.payload())
        if self.identity_id and self.identity_id != expected:
            raise ValueError("release candidate Git identity differs")
        object.__setattr__(self, "identity_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "repository_url": self.repository_url,
            "ref_name": self.ref_name,
            "commit_sha": self.commit_sha,
            "tree_sha": self.tree_sha,
            "captured_at_utc": self.captured_at_utc,
            "clean_tree": True,
            "full_commit_sha_required": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "identity_id": self.identity_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseCandidateGitIdentityV1:
        if data.get("full_commit_sha_required") is not True:
            raise ValueError("release candidate full commit policy differs")
        return cls(
            repository_url=str(data.get("repository_url", "")),
            ref_name=str(data.get("ref_name", "")),
            commit_sha=str(data.get("commit_sha", "")),
            tree_sha=str(data.get("tree_sha", "")),
            captured_at_utc=str(data.get("captured_at_utc", "")),
            clean_tree=data.get("clean_tree") is True,
            identity_id=str(data.get("identity_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateBuildSetV1:
    """Exact wheel and source distribution built from one commit."""

    package_name: str
    package_version: str
    git_commit_sha: str
    artifacts: Mapping[str, ArtifactRef]
    build_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_BUILD_SET_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RELEASE_CANDIDATE_BUILD_SET_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "package_name", _required_text(self.package_name)
        )
        version = _required_text(self.package_version)
        if _SEMVER.fullmatch(version) is None:
            raise ValueError("release candidate package version is not SemVer")
        object.__setattr__(self, "package_version", version)
        object.__setattr__(
            self, "git_commit_sha", _git_object_id(self.git_commit_sha)
        )
        artifacts = _artifact_mapping(self.artifacts)
        if set(artifacts) != {"sdist", "wheel"}:
            raise ValueError(
                "release candidate requires one wheel and one sdist"
            )
        if not artifacts["wheel"].path.endswith(".whl"):
            raise ValueError("release candidate wheel path is invalid")
        if not artifacts["sdist"].path.endswith(".tar.gz"):
            raise ValueError("release candidate sdist path is invalid")
        for distribution_format, ref in artifacts.items():
            if ref.metadata.get("git_commit_sha") != self.git_commit_sha:
                raise ValueError("build artifact commit identity differs")
            if ref.metadata.get("package_version") != self.package_version:
                raise ValueError("build artifact package version differs")
            if ref.metadata.get("distribution_format") != distribution_format:
                raise ValueError("build artifact distribution format differs")
        object.__setattr__(self, "artifacts", artifacts)
        expected = _stable_id("release-candidate-build", self.payload())
        if self.build_id and self.build_id != expected:
            raise ValueError("release candidate build identity differs")
        object.__setattr__(self, "build_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "package_name": self.package_name,
            "package_version": self.package_version,
            "git_commit_sha": self.git_commit_sha,
            "artifacts": {
                key: ref.to_dict() for key, ref in self.artifacts.items()
            },
            "formats": ["sdist", "wheel"],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "build_id": self.build_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReleaseCandidateBuildSetV1:
        if data.get("formats") != ["sdist", "wheel"]:
            raise ValueError("release candidate build formats differ")
        return cls(
            package_name=str(data.get("package_name", "")),
            package_version=str(data.get("package_version", "")),
            git_commit_sha=str(data.get("git_commit_sha", "")),
            artifacts={
                str(key): ArtifactRef.from_dict(_mapping(value))
                for key, value in _mapping(data.get("artifacts")).items()
            },
            build_id=str(data.get("build_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateRuntimeIdentityV1:
    """Exact interpreter, platform, dependency, and compression identity."""

    python_implementation: str
    python_version: str
    python_abi: str
    operating_system: str
    operating_system_release: str
    architecture: str
    machine_class: str
    dependency_versions: Mapping[str, str]
    compression_versions: Mapping[str, str]
    runtime_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_RUNTIME_IDENTITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_CANDIDATE_RUNTIME_IDENTITY_SCHEMA_VERSION,
        )
        for name in (
            "python_implementation",
            "python_version",
            "python_abi",
            "operating_system",
            "operating_system_release",
            "architecture",
            "machine_class",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        dependencies = _text_mapping(self.dependency_versions)
        missing = REQUIRED_RELEASE_CANDIDATE_RUNTIME_DEPENDENCIES - set(
            dependencies
        )
        if missing:
            raise ValueError(
                "release candidate runtime dependencies absent: "
                + ", ".join(sorted(missing))
            )
        compression = _text_mapping(self.compression_versions)
        if "zlib" not in compression:
            raise ValueError("release candidate zlib identity is absent")
        object.__setattr__(self, "dependency_versions", dependencies)
        object.__setattr__(self, "compression_versions", compression)
        expected = _stable_id("release-candidate-runtime", self.payload())
        if self.runtime_id and self.runtime_id != expected:
            raise ValueError("release candidate runtime identity differs")
        object.__setattr__(self, "runtime_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "python_implementation": self.python_implementation,
            "python_version": self.python_version,
            "python_abi": self.python_abi,
            "operating_system": self.operating_system,
            "operating_system_release": self.operating_system_release,
            "architecture": self.architecture,
            "machine_class": self.machine_class,
            "dependency_versions": dict(self.dependency_versions),
            "compression_versions": dict(self.compression_versions),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "runtime_id": self.runtime_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseCandidateRuntimeIdentityV1:
        return cls(
            python_implementation=str(data.get("python_implementation", "")),
            python_version=str(data.get("python_version", "")),
            python_abi=str(data.get("python_abi", "")),
            operating_system=str(data.get("operating_system", "")),
            operating_system_release=str(
                data.get("operating_system_release", "")
            ),
            architecture=str(data.get("architecture", "")),
            machine_class=str(data.get("machine_class", "")),
            dependency_versions=_string_mapping(
                data.get("dependency_versions")
            ),
            compression_versions=_string_mapping(
                data.get("compression_versions")
            ),
            runtime_id=str(data.get("runtime_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateFilesystemRootV1:
    """Qualified absolute filesystem root bound to one machine class."""

    role: str
    path: str
    filesystem_id: str
    filesystem_type: str
    device_id: str
    machine_class: str
    free_bytes: int
    qualification_ref: ArtifactRef
    qualified_at_utc: str
    writable: bool = True
    atomic_replace_verified: bool = True
    durable_write_verified: bool = True
    root_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_FILESYSTEM_ROOT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_CANDIDATE_FILESYSTEM_ROOT_SCHEMA_VERSION,
        )
        role = _required_text(self.role)
        if role not in REQUIRED_RELEASE_CANDIDATE_ROOTS:
            raise ValueError("unsupported release candidate filesystem role")
        object.__setattr__(self, "role", role)
        path = Path(_required_text(self.path)).expanduser()
        if not path.is_absolute():
            raise ValueError("release candidate filesystem path is relative")
        object.__setattr__(self, "path", str(path.resolve()))
        for name in (
            "filesystem_id",
            "filesystem_type",
            "device_id",
            "machine_class",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if (
            self.writable is not True
            or self.atomic_replace_verified is not True
            or self.durable_write_verified is not True
        ):
            raise ValueError("release candidate filesystem is not qualified")
        if isinstance(self.free_bytes, bool) or self.free_bytes <= 0:
            raise ValueError("release candidate filesystem free bytes invalid")
        if not isinstance(self.qualification_ref, ArtifactRef):
            raise TypeError("filesystem qualification reference is invalid")
        _require_strong_ref(self.qualification_ref)
        if self.qualification_ref.metadata.get("role") != role:
            raise ValueError("filesystem qualification role differs")
        if (
            self.qualification_ref.metadata.get("filesystem_id")
            != self.filesystem_id
        ):
            raise ValueError("filesystem qualification identity differs")
        for name in (
            "writable",
            "atomic_replace_verified",
            "durable_write_verified",
        ):
            if self.qualification_ref.metadata.get(name) is not True:
                raise ValueError("filesystem qualification evidence differs")
        if (
            self.qualification_ref.metadata.get("machine_class")
            != self.machine_class
        ):
            raise ValueError("filesystem qualification machine differs")
        object.__setattr__(
            self, "qualified_at_utc", _timestamp(self.qualified_at_utc)
        )
        expected = _stable_id("release-candidate-filesystem", self.payload())
        if self.root_id and self.root_id != expected:
            raise ValueError("release candidate filesystem root differs")
        object.__setattr__(self, "root_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "role": self.role,
            "path": self.path,
            "filesystem_id": self.filesystem_id,
            "filesystem_type": self.filesystem_type,
            "device_id": self.device_id,
            "machine_class": self.machine_class,
            "free_bytes": self.free_bytes,
            "qualification_ref": self.qualification_ref.to_dict(),
            "qualified_at_utc": self.qualified_at_utc,
            "writable": True,
            "atomic_replace_verified": True,
            "durable_write_verified": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "root_id": self.root_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseCandidateFilesystemRootV1:
        return cls(
            role=str(data.get("role", "")),
            path=str(data.get("path", "")),
            filesystem_id=str(data.get("filesystem_id", "")),
            filesystem_type=str(data.get("filesystem_type", "")),
            device_id=str(data.get("device_id", "")),
            machine_class=str(data.get("machine_class", "")),
            free_bytes=_strict_int(data.get("free_bytes"), "free_bytes"),
            qualification_ref=ArtifactRef.from_dict(
                _mapping(data.get("qualification_ref"))
            ),
            qualified_at_utc=str(data.get("qualified_at_utc", "")),
            writable=data.get("writable") is True,
            atomic_replace_verified=(
                data.get("atomic_replace_verified") is True
            ),
            durable_write_verified=(data.get("durable_write_verified") is True),
            root_id=str(data.get("root_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateValidationGateV1:
    """Passing, commit-bound evidence for one release-candidate gate."""

    gate_name: str
    git_commit_sha: str
    platform: str
    command: str
    evidence_ref: ArtifactRef
    completed_at_utc: str
    passed: bool = True
    gate_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_VALIDATION_GATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_CANDIDATE_VALIDATION_GATE_SCHEMA_VERSION,
        )
        gate_name = _required_text(self.gate_name)
        if gate_name not in REQUIRED_RELEASE_CANDIDATE_GATES:
            raise ValueError("unsupported release candidate validation gate")
        object.__setattr__(self, "gate_name", gate_name)
        object.__setattr__(
            self, "git_commit_sha", _git_object_id(self.git_commit_sha)
        )
        for name in ("platform", "command"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.passed is not True:
            raise ValueError("release candidate validation gate did not pass")
        if not isinstance(self.evidence_ref, ArtifactRef):
            raise TypeError("validation gate evidence reference is invalid")
        _require_strong_ref(self.evidence_ref)
        if self.evidence_ref.metadata.get("gate_name") != gate_name:
            raise ValueError("validation gate evidence name differs")
        if (
            self.evidence_ref.metadata.get("git_commit_sha")
            != self.git_commit_sha
        ):
            raise ValueError("validation gate commit identity differs")
        if self.evidence_ref.metadata.get("passed") is not True:
            raise ValueError("validation gate passing evidence differs")
        object.__setattr__(
            self, "completed_at_utc", _timestamp(self.completed_at_utc)
        )
        expected = _stable_id("release-candidate-gate", self.payload())
        if self.gate_id and self.gate_id != expected:
            raise ValueError("release candidate validation gate differs")
        object.__setattr__(self, "gate_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "gate_name": self.gate_name,
            "git_commit_sha": self.git_commit_sha,
            "platform": self.platform,
            "command": self.command,
            "evidence_ref": self.evidence_ref.to_dict(),
            "completed_at_utc": self.completed_at_utc,
            "passed": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "gate_id": self.gate_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseCandidateValidationGateV1:
        return cls(
            gate_name=str(data.get("gate_name", "")),
            git_commit_sha=str(data.get("git_commit_sha", "")),
            platform=str(data.get("platform", "")),
            command=str(data.get("command", "")),
            evidence_ref=ArtifactRef.from_dict(
                _mapping(data.get("evidence_ref"))
            ),
            completed_at_utc=str(data.get("completed_at_utc", "")),
            passed=data.get("passed") is True,
            gate_id=str(data.get("gate_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateBranchGovernanceV1:
    """Protected immutable ref and promotion rules for qualification."""

    immutable_ref: str
    git_commit_sha: str
    required_checks: tuple[str, ...]
    protection_ref: ArtifactRef
    policy_id: str = ""
    protection_enabled: bool = True
    same_checks_as_main: bool = True
    signed_merge_or_tag_required: bool = True
    scientific_commits_after_qualification: bool = False
    operational_fix_requires_new_candidate: bool = True
    schema_version: str = RELEASE_CANDIDATE_BRANCH_GOVERNANCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_CANDIDATE_BRANCH_GOVERNANCE_SCHEMA_VERSION,
        )
        immutable_ref = _required_text(self.immutable_ref)
        if not immutable_ref.startswith(("refs/heads/", "refs/tags/")):
            raise ValueError("release candidate immutable ref is invalid")
        object.__setattr__(self, "immutable_ref", immutable_ref)
        object.__setattr__(
            self, "git_commit_sha", _git_object_id(self.git_commit_sha)
        )
        object.__setattr__(
            self, "required_checks", _text_tuple(self.required_checks)
        )
        if (
            self.protection_enabled is not True
            or self.same_checks_as_main is not True
            or self.signed_merge_or_tag_required is not True
            or self.scientific_commits_after_qualification is not False
            or self.operational_fix_requires_new_candidate is not True
        ):
            raise ValueError("release candidate branch governance differs")
        if not isinstance(self.protection_ref, ArtifactRef):
            raise TypeError("release candidate protection reference invalid")
        _require_strong_ref(self.protection_ref)
        if self.protection_ref.metadata.get("immutable_ref") != immutable_ref:
            raise ValueError("branch protection evidence ref differs")
        if (
            self.protection_ref.metadata.get("git_commit_sha")
            != self.git_commit_sha
        ):
            raise ValueError("branch protection commit identity differs")
        for name in (
            "protection_enabled",
            "same_checks_as_main",
            "signed_merge_or_tag_required",
        ):
            if self.protection_ref.metadata.get(name) is not True:
                raise ValueError("branch protection evidence policy differs")
        expected = _stable_id("release-candidate-branch", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("release candidate branch policy identity differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "immutable_ref": self.immutable_ref,
            "git_commit_sha": self.git_commit_sha,
            "required_checks": list(self.required_checks),
            "protection_ref": self.protection_ref.to_dict(),
            "protection_enabled": True,
            "same_checks_as_main": True,
            "signed_merge_or_tag_required": True,
            "scientific_commits_after_qualification": False,
            "operational_fix_requires_new_candidate": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseCandidateBranchGovernanceV1:
        return cls(
            immutable_ref=str(data.get("immutable_ref", "")),
            git_commit_sha=str(data.get("git_commit_sha", "")),
            required_checks=_string_tuple(data.get("required_checks")),
            protection_ref=ArtifactRef.from_dict(
                _mapping(data.get("protection_ref"))
            ),
            policy_id=str(data.get("policy_id", "")),
            protection_enabled=data.get("protection_enabled") is True,
            same_checks_as_main=data.get("same_checks_as_main") is True,
            signed_merge_or_tag_required=(
                data.get("signed_merge_or_tag_required") is True
            ),
            scientific_commits_after_qualification=(
                data.get("scientific_commits_after_qualification") is True
            ),
            operational_fix_requires_new_candidate=(
                data.get("operational_fix_requires_new_candidate") is True
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateDependencyV1:
    """One named, strong, identity-bearing candidate dependency."""

    name: str
    artifact_id: str
    artifact_ref: ArtifactRef
    dependency_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_DEPENDENCY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RELEASE_CANDIDATE_DEPENDENCY_SCHEMA_VERSION
        )
        name = _required_text(self.name)
        if name not in REQUIRED_RELEASE_CANDIDATE_DEPENDENCIES:
            raise ValueError("unsupported release candidate dependency")
        object.__setattr__(self, "name", name)
        object.__setattr__(
            self, "artifact_id", _required_text(self.artifact_id)
        )
        if not isinstance(self.artifact_ref, ArtifactRef):
            raise TypeError("release candidate dependency reference invalid")
        _require_strong_ref(self.artifact_ref)
        if self.artifact_id not in self.artifact_ref.metadata.values():
            raise ValueError("release candidate dependency identity absent")
        expected = _stable_id("release-candidate-dependency", self.payload())
        if self.dependency_id and self.dependency_id != expected:
            raise ValueError("release candidate dependency identity differs")
        object.__setattr__(self, "dependency_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "artifact_id": self.artifact_id,
            "artifact_ref": self.artifact_ref.to_dict(),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "dependency_id": self.dependency_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReleaseCandidateDependencyV1:
        return cls(
            name=str(data.get("name", "")),
            artifact_id=str(data.get("artifact_id", "")),
            artifact_ref=ArtifactRef.from_dict(
                _mapping(data.get("artifact_ref"))
            ),
            dependency_id=str(data.get("dependency_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionReleaseCandidateV1:
    """One immutable, installable, scientifically frozen candidate."""

    git_identity: ReleaseCandidateGitIdentityV1
    build_set: ReleaseCandidateBuildSetV1
    runtime_identity: ReleaseCandidateRuntimeIdentityV1
    schema_registry_id: str
    dataset_revision: str
    source_partition_hashes: Mapping[str, str]
    experiment_id: str
    selected_engine_id: str
    dependencies: tuple[ReleaseCandidateDependencyV1, ...]
    filesystem_roots: tuple[ReleaseCandidateFilesystemRootV1, ...]
    validation_gates: tuple[ReleaseCandidateValidationGateV1, ...]
    branch_governance: ReleaseCandidateBranchGovernanceV1
    executable_scope: tuple[str, ...]
    scientific_estimand: str
    scientific_nonclaims: tuple[str, ...]
    source_cutoff_ns: int
    permitted_commands: tuple[str, ...]
    runtime_handlers: tuple[str, ...]
    forbidden_fallbacks: tuple[str, ...]
    release_blocking_issues: tuple[int, ...]
    frozen_at_utc: str
    candidate_id: str = ""
    schema_version: str = RECONSTRUCTION_RELEASE_CANDIDATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RECONSTRUCTION_RELEASE_CANDIDATE_SCHEMA_VERSION
        )
        if not isinstance(self.git_identity, ReleaseCandidateGitIdentityV1):
            raise TypeError("release candidate Git identity is invalid")
        if not isinstance(self.build_set, ReleaseCandidateBuildSetV1):
            raise TypeError("release candidate build set is invalid")
        if not isinstance(
            self.runtime_identity, ReleaseCandidateRuntimeIdentityV1
        ):
            raise TypeError("release candidate runtime identity is invalid")
        if self.build_set.git_commit_sha != self.git_identity.commit_sha:
            raise ValueError("release candidate build and Git commits differ")
        object.__setattr__(
            self, "schema_registry_id", _required_text(self.schema_registry_id)
        )
        object.__setattr__(
            self, "dataset_revision", _required_text(self.dataset_revision)
        )
        hashes = {
            _required_text(key): _sha256(value)
            for key, value in sorted(self.source_partition_hashes.items())
        }
        if not hashes or len(hashes) > MAX_RELEASE_CANDIDATE_ITEMS:
            raise ValueError(
                "release candidate source partition hashes invalid"
            )
        object.__setattr__(self, "source_partition_hashes", hashes)
        for name in ("experiment_id", "selected_engine_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        dependencies = _dependency_tuple(self.dependencies)
        if {item.name for item in dependencies} != (
            REQUIRED_RELEASE_CANDIDATE_DEPENDENCIES
        ):
            raise ValueError("release candidate dependency graph is incomplete")
        object.__setattr__(self, "dependencies", dependencies)
        dependency_by_name = {item.name: item for item in dependencies}
        if (
            dependency_by_name["schema_registry"].artifact_id
            != self.schema_registry_id
        ):
            raise ValueError("release candidate schema registry differs")
        if (
            dependency_by_name["experiment_manifest"].artifact_id
            != self.experiment_id
        ):
            raise ValueError("release candidate experiment identity differs")
        if (
            dependency_by_name["dataset_catalog"].artifact_ref.metadata.get(
                "dataset_revision"
            )
            != self.dataset_revision
        ):
            raise ValueError("release candidate dataset revision differs")
        for name in ("selected_engine_config", "selected_engine_fit"):
            if (
                dependency_by_name[name].artifact_ref.metadata.get("engine_id")
                != self.selected_engine_id
            ):
                raise ValueError("release candidate selected engine differs")
        roots = _root_tuple(self.filesystem_roots)
        if {item.role for item in roots} != REQUIRED_RELEASE_CANDIDATE_ROOTS:
            raise ValueError(
                "release candidate filesystem roots are incomplete"
            )
        _require_disjoint_roots(roots)
        if any(
            item.machine_class != self.runtime_identity.machine_class
            for item in roots
        ):
            raise ValueError("release candidate filesystem machine differs")
        object.__setattr__(self, "filesystem_roots", roots)
        gates = _gate_tuple(self.validation_gates)
        if {item.gate_name for item in gates} != (
            REQUIRED_RELEASE_CANDIDATE_GATES
        ):
            raise ValueError(
                "release candidate validation gates are incomplete"
            )
        if any(
            item.git_commit_sha != self.git_identity.commit_sha
            for item in gates
        ):
            raise ValueError("release candidate validation commit differs")
        object.__setattr__(self, "validation_gates", gates)
        if not isinstance(
            self.branch_governance, ReleaseCandidateBranchGovernanceV1
        ):
            raise TypeError("release candidate branch governance is invalid")
        if (
            self.branch_governance.git_commit_sha
            != self.git_identity.commit_sha
        ):
            raise ValueError("release candidate protected ref commit differs")
        object.__setattr__(
            self, "executable_scope", _text_tuple(self.executable_scope)
        )
        object.__setattr__(
            self,
            "scientific_estimand",
            _required_text(self.scientific_estimand),
        )
        object.__setattr__(
            self, "scientific_nonclaims", _text_tuple(self.scientific_nonclaims)
        )
        if (
            isinstance(self.source_cutoff_ns, bool)
            or self.source_cutoff_ns <= 0
        ):
            raise ValueError("release candidate source cutoff is invalid")
        commands = _text_tuple(self.permitted_commands)
        if not REQUIRED_RELEASE_CANDIDATE_COMMANDS.issubset(commands):
            raise ValueError("release candidate permitted commands incomplete")
        object.__setattr__(self, "permitted_commands", commands)
        handlers = _text_tuple(self.runtime_handlers)
        if len(handlers) != 7:
            raise ValueError(
                "release candidate requires seven runtime handlers"
            )
        object.__setattr__(self, "runtime_handlers", handlers)
        fallbacks = _text_tuple(self.forbidden_fallbacks)
        if not REQUIRED_RELEASE_CANDIDATE_FORBIDDEN_FALLBACKS.issubset(
            fallbacks
        ):
            raise ValueError("release candidate forbidden fallbacks incomplete")
        object.__setattr__(self, "forbidden_fallbacks", fallbacks)
        issues = tuple(sorted(set(self.release_blocking_issues)))
        if not issues or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in issues
        ):
            raise ValueError("release candidate blocking issue set is invalid")
        object.__setattr__(self, "release_blocking_issues", issues)
        object.__setattr__(
            self, "frozen_at_utc", _timestamp(self.frozen_at_utc)
        )
        frozen_at = _timestamp_value(self.frozen_at_utc)
        prior_timestamps = (
            self.git_identity.captured_at_utc,
            *(item.qualified_at_utc for item in self.filesystem_roots),
            *(item.completed_at_utc for item in self.validation_gates),
        )
        if any(
            _timestamp_value(value) >= frozen_at for value in prior_timestamps
        ):
            raise ValueError("release candidate evidence is not pre-freeze")
        expected = _stable_id(
            "reconstruction-release-candidate", self.payload()
        )
        if self.candidate_id and self.candidate_id != expected:
            raise ValueError(
                "reconstruction release candidate identity differs"
            )
        object.__setattr__(self, "candidate_id", expected)

    def dependency(self, name: str) -> ReleaseCandidateDependencyV1:
        """Return one required dependency by stable role name."""
        normalized = _required_text(name)
        for item in self.dependencies:
            if item.name == normalized:
                return item
        raise KeyError(normalized)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "git_identity": self.git_identity.to_dict(),
            "build_set": self.build_set.to_dict(),
            "runtime_identity": self.runtime_identity.to_dict(),
            "schema_registry_id": self.schema_registry_id,
            "dataset_revision": self.dataset_revision,
            "source_partition_hashes": dict(self.source_partition_hashes),
            "experiment_id": self.experiment_id,
            "selected_engine_id": self.selected_engine_id,
            "dependencies": [item.to_dict() for item in self.dependencies],
            "filesystem_roots": [
                item.to_dict() for item in self.filesystem_roots
            ],
            "validation_gates": [
                item.to_dict() for item in self.validation_gates
            ],
            "branch_governance": self.branch_governance.to_dict(),
            "executable_scope": list(self.executable_scope),
            "scientific_estimand": self.scientific_estimand,
            "scientific_nonclaims": list(self.scientific_nonclaims),
            "source_cutoff_ns": self.source_cutoff_ns,
            "permitted_commands": list(self.permitted_commands),
            "runtime_handlers": list(self.runtime_handlers),
            "forbidden_fallbacks": list(self.forbidden_fallbacks),
            "release_blocking_issues": list(self.release_blocking_issues),
            "frozen_at_utc": self.frozen_at_utc,
            "scientific_decisions_complete": True,
            "qualification_started": False,
            "fresh_release_holdout_sealed": True,
            "dependency_change_creates_new_candidate": True,
            "cross_candidate_certification_permitted": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "candidate_id": self.candidate_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionReleaseCandidateV1:
        _require_fixed_candidate_policy(data)
        return cls(
            git_identity=ReleaseCandidateGitIdentityV1.from_dict(
                _mapping(data.get("git_identity"))
            ),
            build_set=ReleaseCandidateBuildSetV1.from_dict(
                _mapping(data.get("build_set"))
            ),
            runtime_identity=ReleaseCandidateRuntimeIdentityV1.from_dict(
                _mapping(data.get("runtime_identity"))
            ),
            schema_registry_id=str(data.get("schema_registry_id", "")),
            dataset_revision=str(data.get("dataset_revision", "")),
            source_partition_hashes=_string_mapping(
                data.get("source_partition_hashes")
            ),
            experiment_id=str(data.get("experiment_id", "")),
            selected_engine_id=str(data.get("selected_engine_id", "")),
            dependencies=tuple(
                ReleaseCandidateDependencyV1.from_dict(_mapping(item))
                for item in _sequence(data.get("dependencies"))
            ),
            filesystem_roots=tuple(
                ReleaseCandidateFilesystemRootV1.from_dict(_mapping(item))
                for item in _sequence(data.get("filesystem_roots"))
            ),
            validation_gates=tuple(
                ReleaseCandidateValidationGateV1.from_dict(_mapping(item))
                for item in _sequence(data.get("validation_gates"))
            ),
            branch_governance=ReleaseCandidateBranchGovernanceV1.from_dict(
                _mapping(data.get("branch_governance"))
            ),
            executable_scope=_string_tuple(data.get("executable_scope")),
            scientific_estimand=str(data.get("scientific_estimand", "")),
            scientific_nonclaims=_string_tuple(
                data.get("scientific_nonclaims")
            ),
            source_cutoff_ns=_strict_int(
                data.get("source_cutoff_ns"), "source_cutoff_ns"
            ),
            permitted_commands=_string_tuple(data.get("permitted_commands")),
            runtime_handlers=_string_tuple(data.get("runtime_handlers")),
            forbidden_fallbacks=_string_tuple(data.get("forbidden_fallbacks")),
            release_blocking_issues=tuple(
                _strict_int(value, "release_blocking_issue")
                for value in _sequence(data.get("release_blocking_issues"))
            ),
            frozen_at_utc=str(data.get("frozen_at_utc", "")),
            candidate_id=str(data.get("candidate_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReleaseCandidateArtifactBindingV1:
    """Candidate-scoped evidence that cannot certify another candidate."""

    candidate_id: str
    artifact_role: str
    artifact_ref: ArtifactRef
    issued_at_utc: str
    binding_id: str = ""
    schema_version: str = RELEASE_CANDIDATE_ARTIFACT_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RELEASE_CANDIDATE_ARTIFACT_BINDING_SCHEMA_VERSION,
        )
        for name in ("candidate_id", "artifact_role"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if not isinstance(self.artifact_ref, ArtifactRef):
            raise TypeError("candidate-bound artifact reference is invalid")
        _require_strong_ref(self.artifact_ref)
        if self.artifact_ref.metadata.get("candidate_id") != self.candidate_id:
            raise ValueError("candidate-bound artifact uses another candidate")
        object.__setattr__(
            self, "issued_at_utc", _timestamp(self.issued_at_utc)
        )
        expected = _stable_id("release-candidate-binding", self.payload())
        if self.binding_id and self.binding_id != expected:
            raise ValueError("release candidate artifact binding differs")
        object.__setattr__(self, "binding_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "candidate_id": self.candidate_id,
            "artifact_role": self.artifact_role,
            "artifact_ref": self.artifact_ref.to_dict(),
            "issued_at_utc": self.issued_at_utc,
            "cross_candidate_use_permitted": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "binding_id": self.binding_id}

    def to_json(self) -> str:
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReleaseCandidateArtifactBindingV1:
        if data.get("cross_candidate_use_permitted") is not False:
            raise ValueError("cross-candidate artifact policy differs")
        return cls(
            candidate_id=str(data.get("candidate_id", "")),
            artifact_role=str(data.get("artifact_role", "")),
            artifact_ref=ArtifactRef.from_dict(
                _mapping(data.get("artifact_ref"))
            ),
            issued_at_utc=str(data.get("issued_at_utc", "")),
            binding_id=str(data.get("binding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def inspect_release_candidate_git_identity(
    repository_root: str | Path,
    *,
    captured_at_utc: str | None = None,
) -> ReleaseCandidateGitIdentityV1:
    """Inspect a clean Git checkout and return its exact identity."""
    root = Path(repository_root).expanduser().resolve()
    commit_sha = _git(root, "rev-parse", "HEAD")
    tree_sha = _git(root, "rev-parse", "HEAD^{tree}")
    ref_name = _git_optional(root, "symbolic-ref", "-q", "HEAD")
    if not ref_name:
        tag_name = _git_optional(root, "describe", "--exact-match", "--tags")
        if not tag_name:
            raise ValueError(
                "release candidate checkout is detached without an exact tag"
            )
        ref_name = f"refs/tags/{tag_name}"
    repository_url = _git(root, "config", "--get", "remote.origin.url")
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    return ReleaseCandidateGitIdentityV1(
        repository_url=repository_url,
        ref_name=ref_name,
        commit_sha=commit_sha,
        tree_sha=tree_sha,
        captured_at_utc=captured_at_utc or _utc_now(),
        clean_tree=not bool(status),
    )


def capture_release_candidate_runtime_identity(
    *, machine_class: str
) -> ReleaseCandidateRuntimeIdentityV1:
    """Capture exact critical package and local interpreter versions."""
    distribution_names = {
        "certifi": "certifi",
        "numpy": "numpy",
        "polars": "polars",
        "pyarrow": "pyarrow",
        "pytz": "pytz",
        "pyyaml": "PyYAML",
        "requests": "requests",
        "rich": "rich",
        "rx": "Rx",
        "temporalio": "temporalio",
        "tzdata": "tzdata",
    }
    versions: dict[str, str] = {}
    for key, distribution_name in distribution_names.items():
        try:
            versions[key] = metadata.version(distribution_name)
        except metadata.PackageNotFoundError as err:
            raise ValueError(
                f"release candidate dependency is not installed: {key}"
            ) from err
    return ReleaseCandidateRuntimeIdentityV1(
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
        python_abi=str(sys.implementation.cache_tag or "unknown"),
        operating_system=platform.system(),
        operating_system_release=platform.release(),
        architecture=platform.machine(),
        machine_class=machine_class,
        dependency_versions=versions,
        compression_versions={
            "bz2": "stdlib",
            "gzip": "stdlib",
            "lzma": "stdlib",
            "zlib": zlib.ZLIB_RUNTIME_VERSION,
        },
    )


def freeze_reconstruction_release_candidate(
    *,
    git_identity: ReleaseCandidateGitIdentityV1,
    build_set: ReleaseCandidateBuildSetV1,
    runtime_identity: ReleaseCandidateRuntimeIdentityV1,
    schema_registry_id: str,
    dataset_revision: str,
    source_partition_hashes: Mapping[str, str],
    experiment_id: str,
    selected_engine_id: str,
    dependencies: Sequence[ReleaseCandidateDependencyV1],
    filesystem_roots: Sequence[ReleaseCandidateFilesystemRootV1],
    validation_gates: Sequence[ReleaseCandidateValidationGateV1],
    branch_governance: ReleaseCandidateBranchGovernanceV1,
    executable_scope: Sequence[str],
    scientific_estimand: str,
    scientific_nonclaims: Sequence[str],
    source_cutoff_ns: int,
    permitted_commands: Sequence[str],
    runtime_handlers: Sequence[str],
    forbidden_fallbacks: Sequence[str],
    release_blocking_issues: Sequence[int],
    frozen_at_utc: str,
) -> ReconstructionReleaseCandidateV1:
    """Freeze and deeply verify one exact release-candidate graph."""
    from histdatacom import __version__
    from histdatacom.reconstruction_schema import (
        reconstruction_schema_registry,
    )
    from histdatacom.synthetic.reconstruction_plan import (
        FIRST_PARTY_RECONSTRUCTION_HANDLERS,
    )

    if build_set.package_version != __version__:
        raise ValueError("release candidate package version differs from API")
    if schema_registry_id != reconstruction_schema_registry().registry_id:
        raise ValueError("release candidate schema registry is stale")
    if set(runtime_handlers) != set(
        FIRST_PARTY_RECONSTRUCTION_HANDLERS.values()
    ):
        raise ValueError("release candidate runtime handler registry differs")
    candidate = ReconstructionReleaseCandidateV1(
        git_identity=git_identity,
        build_set=build_set,
        runtime_identity=runtime_identity,
        schema_registry_id=schema_registry_id,
        dataset_revision=dataset_revision,
        source_partition_hashes=source_partition_hashes,
        experiment_id=experiment_id,
        selected_engine_id=selected_engine_id,
        dependencies=tuple(dependencies),
        filesystem_roots=tuple(filesystem_roots),
        validation_gates=tuple(validation_gates),
        branch_governance=branch_governance,
        executable_scope=tuple(executable_scope),
        scientific_estimand=scientific_estimand,
        scientific_nonclaims=tuple(scientific_nonclaims),
        source_cutoff_ns=source_cutoff_ns,
        permitted_commands=tuple(permitted_commands),
        runtime_handlers=tuple(runtime_handlers),
        forbidden_fallbacks=tuple(forbidden_fallbacks),
        release_blocking_issues=tuple(release_blocking_issues),
        frozen_at_utc=frozen_at_utc,
    )
    verify_reconstruction_release_candidate(candidate)
    graph = read_release_candidate_freeze(
        candidate.dependency("candidate_graph").artifact_ref.path
    )
    holdout = read_protected_release_holdout_manifest(
        candidate.dependency("protected_release_holdout").artifact_ref.path
    )
    if graph.graph_id != candidate.dependency("candidate_graph").artifact_id:
        raise ValueError("release candidate graph identity differs")
    if (
        holdout.manifest_id
        != candidate.dependency("protected_release_holdout").artifact_id
    ):
        raise ValueError("protected release holdout identity differs")
    if graph.manifest_id != holdout.manifest_id:
        raise ValueError("release candidate graph uses another holdout")
    if holdout.source_cutoff_ns != candidate.source_cutoff_ns:
        raise ValueError("release candidate source cutoff differs")
    if _timestamp_value(graph.frozen_at_utc) >= _timestamp_value(
        candidate.frozen_at_utc
    ):
        raise ValueError("release candidate manifest predates candidate graph")
    return candidate


def verify_reconstruction_release_candidate(
    candidate: ReconstructionReleaseCandidateV1,
    *,
    expected_candidate_id: str | None = None,
) -> None:
    """Verify all locally available strong references and candidate identity."""
    if not isinstance(candidate, ReconstructionReleaseCandidateV1):
        raise TypeError("release candidate contract is invalid")
    if (
        expected_candidate_id
        and candidate.candidate_id != expected_candidate_id
    ):
        raise ValueError("unexpected release candidate identity")
    current = ReconstructionReleaseCandidateV1.from_dict(candidate.to_dict())
    if current != candidate:
        raise ValueError("release candidate round-trip differs")
    refs = [
        *candidate.build_set.artifacts.values(),
        *(item.artifact_ref for item in candidate.dependencies),
        *(item.qualification_ref for item in candidate.filesystem_roots),
        *(item.evidence_ref for item in candidate.validation_gates),
        candidate.branch_governance.protection_ref,
    ]
    for ref in refs:
        _verify_artifact_ref(ref)


def bind_release_candidate_artifact(
    candidate: ReconstructionReleaseCandidateV1,
    *,
    artifact_role: str,
    artifact_ref: ArtifactRef,
    issued_at_utc: str,
) -> ReleaseCandidateArtifactBindingV1:
    """Bind qualification, campaign, or certification evidence to a candidate."""
    _verify_artifact_ref(artifact_ref)
    return ReleaseCandidateArtifactBindingV1(
        candidate_id=candidate.candidate_id,
        artifact_role=artifact_role,
        artifact_ref=artifact_ref,
        issued_at_utc=issued_at_utc,
    )


def write_reconstruction_release_candidate(
    candidate: ReconstructionReleaseCandidateV1,
    output_directory: str | Path,
) -> ArtifactRef:
    """Write one content-addressed immutable release-candidate manifest."""
    return _write_contract(
        candidate.to_json(),
        output_directory,
        prefix="reconstruction-release-candidate",
        kind="reconstruction_release_candidate_v1",
        metadata={"candidate_id": candidate.candidate_id},
    )


def read_reconstruction_release_candidate(
    path: str | Path,
) -> ReconstructionReleaseCandidateV1:
    """Hash-verify and restore one release-candidate manifest."""
    return ReconstructionReleaseCandidateV1.from_dict(
        _read_contract(path, "reconstruction-release-candidate")
    )


def write_release_candidate_artifact_binding(
    binding: ReleaseCandidateArtifactBindingV1,
    output_directory: str | Path,
) -> ArtifactRef:
    """Write one content-addressed candidate-scoped evidence binding."""
    return _write_contract(
        binding.to_json(),
        output_directory,
        prefix="release-candidate-artifact-binding",
        kind="release_candidate_artifact_binding_v1",
        metadata={
            "binding_id": binding.binding_id,
            "candidate_id": binding.candidate_id,
        },
    )


def read_release_candidate_artifact_binding(
    path: str | Path,
) -> ReleaseCandidateArtifactBindingV1:
    """Hash-verify and restore a candidate-scoped evidence binding."""
    return ReleaseCandidateArtifactBindingV1.from_dict(
        _read_contract(path, "release-candidate-artifact-binding")
    )


def _git(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode:
        raise ValueError(
            "release candidate Git inspection failed: "
            + completed.stderr.strip()
        )
    return completed.stdout.strip()


def _git_optional(root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(root), *arguments),
        capture_output=True,
        check=False,
        text=True,
    )
    if completed.returncode:
        return ""
    return completed.stdout.strip()


def _dependency_tuple(
    values: Sequence[ReleaseCandidateDependencyV1],
) -> tuple[ReleaseCandidateDependencyV1, ...]:
    result = tuple(sorted(values, key=lambda item: item.name))
    if len(result) > MAX_RELEASE_CANDIDATE_ITEMS or len(
        {item.name for item in result}
    ) != len(result):
        raise ValueError("release candidate dependency names differ")
    return result


def _root_tuple(
    values: Sequence[ReleaseCandidateFilesystemRootV1],
) -> tuple[ReleaseCandidateFilesystemRootV1, ...]:
    result = tuple(sorted(values, key=lambda item: item.role))
    if len({item.role for item in result}) != len(result):
        raise ValueError("release candidate filesystem root roles differ")
    return result


def _gate_tuple(
    values: Sequence[ReleaseCandidateValidationGateV1],
) -> tuple[ReleaseCandidateValidationGateV1, ...]:
    result = tuple(sorted(values, key=lambda item: item.gate_name))
    if len({item.gate_name for item in result}) != len(result):
        raise ValueError("release candidate validation gate names differ")
    return result


def _require_disjoint_roots(
    roots: Sequence[ReleaseCandidateFilesystemRootV1],
) -> None:
    paths = tuple((item.role, Path(item.path)) for item in roots)
    for index, (left_role, left) in enumerate(paths):
        for right_role, right in paths[index + 1 :]:
            if left == right or left in right.parents or right in left.parents:
                raise ValueError(
                    "release candidate filesystem roots overlap: "
                    f"{left_role}, {right_role}"
                )


def _artifact_mapping(
    values: Mapping[str, ArtifactRef],
) -> dict[str, ArtifactRef]:
    result: dict[str, ArtifactRef] = {}
    for key, ref in sorted(values.items()):
        normalized = _required_text(key)
        if not isinstance(ref, ArtifactRef):
            raise TypeError("release candidate artifact reference is invalid")
        _require_strong_ref(ref)
        result[normalized] = ref
    return result


def _require_strong_ref(ref: ArtifactRef) -> None:
    _required_text(ref.kind)
    path = Path(_required_text(ref.path)).expanduser()
    if not path.is_absolute():
        raise ValueError("release candidate artifact path is relative")
    if isinstance(ref.size_bytes, bool) or not isinstance(ref.size_bytes, int):
        raise ValueError("release candidate artifact size is absent")
    if ref.size_bytes < 0:
        raise ValueError("release candidate artifact size is negative")
    _sha256(ref.sha256)


def _verify_artifact_ref(ref: ArtifactRef) -> Path:
    _require_strong_ref(ref)
    path = Path(ref.path).expanduser()
    if not path.is_file():
        raise ValueError(f"release candidate artifact is missing: {path}")
    if path.stat().st_size != ref.size_bytes:
        raise ValueError(f"release candidate artifact size differs: {path}")
    if _file_sha256(path) != ref.sha256:
        raise ValueError(f"release candidate artifact hash differs: {path}")
    return path


def _write_contract(
    text: str,
    output_directory: str | Path,
    *,
    prefix: str,
    kind: str,
    metadata: Mapping[str, JSONValue],
) -> ArtifactRef:
    payload = (text + "\n").encode("utf-8")
    if len(payload) > MAX_RELEASE_CANDIDATE_ARTIFACT_BYTES:
        raise ValueError("release candidate artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    directory = Path(output_directory).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{prefix}-{digest}.json"
    if path.exists():
        if path.read_bytes() != payload:
            raise ValueError("release candidate artifact collision")
    else:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o644,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            path.unlink(missing_ok=True)
            raise
    return ArtifactRef(
        kind=kind,
        path=str(path),
        size_bytes=len(payload),
        sha256=digest,
        metadata=dict(metadata),
    )


def _read_contract(path: str | Path, prefix: str) -> Mapping[str, Any]:
    target = Path(path).expanduser()
    payload = target.read_bytes()
    if len(payload) > MAX_RELEASE_CANDIDATE_ARTIFACT_BYTES:
        raise ValueError("release candidate artifact exceeds size limit")
    digest = hashlib.sha256(payload).hexdigest()
    if target.name != f"{prefix}-{digest}.json":
        raise ValueError(
            "release candidate artifact name is not content addressed"
        )
    data = json.loads(payload)
    return _mapping(data)


def _require_fixed_candidate_policy(data: Mapping[str, Any]) -> None:
    expected = {
        "scientific_decisions_complete": True,
        "qualification_started": False,
        "fresh_release_holdout_sealed": True,
        "dependency_change_creates_new_candidate": True,
        "cross_candidate_certification_permitted": False,
    }
    for name, value in expected.items():
        if data.get(name) is not value:
            raise ValueError(f"release candidate {name} differs")


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _timestamp(value: str) -> str:
    normalized = _required_text(value)
    parsed = _timestamp_value(normalized)
    return parsed.isoformat().replace("+00:00", "Z")


def _timestamp_value(value: str) -> datetime:
    normalized = _required_text(value)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as err:
        raise ValueError("release candidate timestamp is invalid") from err
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("release candidate timestamp must include timezone")
    return parsed.astimezone(timezone.utc)


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported release candidate schema: {actual!r}")


def _required_text(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("release candidate text value is required")
    return normalized


def _sha256(value: str) -> str:
    normalized = _required_text(value).lower()
    if _SHA256.fullmatch(normalized) is None:
        raise ValueError("release candidate SHA-256 value is invalid")
    return normalized


def _git_object_id(value: str) -> str:
    normalized = _required_text(value).lower()
    if _GIT_OBJECT_ID.fullmatch(normalized) is None:
        raise ValueError("release candidate full Git object ID is invalid")
    return normalized


def _text_tuple(values: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({_required_text(value) for value in values}))
    if not normalized or len(normalized) > MAX_RELEASE_CANDIDATE_ITEMS:
        raise ValueError("release candidate text set is invalid")
    return normalized


def _text_mapping(values: Mapping[str, str]) -> dict[str, str]:
    result = {
        _required_text(key): _required_text(value)
        for key, value in sorted(values.items())
    }
    if not result or len(result) > MAX_RELEASE_CANDIDATE_ITEMS:
        raise ValueError("release candidate text mapping is invalid")
    return result


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("release candidate object is invalid")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, list):
        raise ValueError("release candidate sequence is invalid")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _string_mapping(value: Any) -> dict[str, str]:
    return {str(key): str(item) for key, item in _mapping(value).items()}


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"release candidate {name} is invalid")
    return value


__all__ = [
    "RECONSTRUCTION_RELEASE_CANDIDATE_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_ARTIFACT_BINDING_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_BRANCH_GOVERNANCE_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_BUILD_SET_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_DEPENDENCY_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_FILESYSTEM_ROOT_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_GIT_IDENTITY_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_RUNTIME_IDENTITY_SCHEMA_VERSION",
    "RELEASE_CANDIDATE_VALIDATION_GATE_SCHEMA_VERSION",
    "REQUIRED_RELEASE_CANDIDATE_COMMANDS",
    "REQUIRED_RELEASE_CANDIDATE_DEPENDENCIES",
    "REQUIRED_RELEASE_CANDIDATE_FORBIDDEN_FALLBACKS",
    "REQUIRED_RELEASE_CANDIDATE_GATES",
    "REQUIRED_RELEASE_CANDIDATE_ROOTS",
    "REQUIRED_RELEASE_CANDIDATE_RUNTIME_DEPENDENCIES",
    "ReconstructionReleaseCandidateV1",
    "ReleaseCandidateArtifactBindingV1",
    "ReleaseCandidateBranchGovernanceV1",
    "ReleaseCandidateBuildSetV1",
    "ReleaseCandidateDependencyV1",
    "ReleaseCandidateFilesystemRootV1",
    "ReleaseCandidateGitIdentityV1",
    "ReleaseCandidateRuntimeIdentityV1",
    "ReleaseCandidateValidationGateV1",
    "bind_release_candidate_artifact",
    "capture_release_candidate_runtime_identity",
    "freeze_reconstruction_release_candidate",
    "inspect_release_candidate_git_identity",
    "read_reconstruction_release_candidate",
    "read_release_candidate_artifact_binding",
    "verify_reconstruction_release_candidate",
    "write_reconstruction_release_candidate",
    "write_release_candidate_artifact_binding",
]
