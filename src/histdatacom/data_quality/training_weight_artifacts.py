"""Research-native shard persistence with mandatory complete reexecution.

These are not legacy reconstruction product manifests or qualification
receipts. Components are immutable, content-named and individually bounded;
the root manifest is published last. Interrupted writes can leave unreferenced
components, never a complete shard. Readers always verify bytes and regenerate
the complete scheduled shard from fresh source/model evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import tempfile
from typing import ClassVar, TypeVar

from .training_contracts import (
    MAX_TRAINING_BYTES,
    TrainingContract,
    TrainingOwnershipV1,
    TrainingSourceV1,
)
from .training_lineage import read_training_regular
from .training_weight_approval import (
    TrainingWeightExecutionApprovalV1,
    require_training_weight_approval,
)
from .training_weight_candidates import (
    TrainingWeightCandidateDayV1,
    TrainingWeightCandidateRefusalV1,
    TrainingWeightCandidateShard,
    TrainingWeightFileV1,
    TrainingWeightModelV1,
    replay_training_weight_candidate_shard,
)
from .training_weight_lineage import (
    TrainingWeightDayBridgeV1,
    TrainingWeightDegradation,
    TrainingWeightRefusedDayV1,
    TrainingWeightSourcePlanV1,
    WeightEvidenceKind,
)

MAX_SHARD_COMPONENTS = 64
MAX_SHARD_TOTAL_BYTES = 128 * 1024 * 1024
MAX_SHARD_DAYS = 8
_KINDS = {
    "source-plan": "training-weight-source-plan",
    "parent-ownership": "training-ownership",
    "subset-source": "training-source",
    "subset-ownership": "training-ownership",
    "bridge": "training-weight-day-bridge",
    "model": "training-weight-model",
    "day": "training-weight-candidate-day",
}
_T = TypeVar("_T", bound=TrainingContract)


@dataclass(frozen=True, slots=True)
class TrainingWeightComponentV1(TrainingContract):
    KIND: ClassVar[str] = "weight-component"
    kind: str
    component_id: str
    file: TrainingWeightFileV1

    def _validate(self) -> None:
        if (
            self.kind not in _KINDS
            or re.fullmatch(
                re.escape(_KINDS[self.kind]) + r":sha256:[0-9a-f]{64}",
                self.component_id,
            )
            is None
        ):
            raise ValueError(
                "unknown or mismatched research component identity"
            )
        if (
            self.file.path != f"weight-{self.kind}-{self.file.sha256}.json"
            or self.file.size_bytes > MAX_TRAINING_BYTES
        ):
            raise ValueError("research component locator or byte bound differs")


@dataclass(frozen=True, slots=True)
class TrainingWeightCandidateShardManifestV1(TrainingContract):
    """Complete inventory binding, never an assertion of production admission."""

    KIND: ClassVar[str] = "weight-candidate-shard-manifest"
    evidence_kind: WeightEvidenceKind
    shard_id: str
    source_plan: TrainingWeightComponentV1
    parent_ownership: TrainingWeightComponentV1
    subset_source: TrainingWeightComponentV1 | None
    subset_ownership: TrainingWeightComponentV1 | None
    bridges: tuple[TrainingWeightComponentV1, ...]
    source_refusals: tuple[TrainingWeightRefusedDayV1, ...]
    model: TrainingWeightComponentV1
    days: tuple[TrainingWeightComponentV1, ...]
    refusals: tuple[TrainingWeightCandidateRefusalV1, ...]

    def _validate(self) -> None:
        if (
            re.fullmatch(r"(?:201001|201101|201102)-[1-3]", self.shard_id)
            is None
        ):
            raise ValueError("research shard is outside frozen schedule")
        if (
            len(self.days) + len(self.refusals) > MAX_SHARD_DAYS
            or not self.days
            and not self.refusals
            or len(self.bridges) + len(self.source_refusals) > 42
            or (self.subset_source is None) != (self.subset_ownership is None)
            or (self.subset_source is None) != (not self.bridges)
        ):
            raise ValueError("research shard inventory exceeds or omits scope")
        for kind, values in (
            ("source-plan", (self.source_plan,)),
            ("parent-ownership", (self.parent_ownership,)),
            ("model", (self.model,)),
            ("bridge", self.bridges),
            ("day", self.days),
            (
                "subset-source",
                () if self.subset_source is None else (self.subset_source,),
            ),
            (
                "subset-ownership",
                (
                    ()
                    if self.subset_ownership is None
                    else (self.subset_ownership,)
                ),
            ),
        ):
            if any(item.kind != kind for item in values):
                raise ValueError(
                    "research component kind differs from manifest slot"
                )
        files = self.components
        if (
            len(files) > MAX_SHARD_COMPONENTS
            or len({item.file.path for item in files}) != len(files)
            or sum(item.file.size_bytes for item in files) + len(self.to_json())
            > MAX_SHARD_TOTAL_BYTES
        ):
            raise ValueError("research shard aggregate work budget exceeded")

    @property
    def components(self) -> tuple[TrainingWeightComponentV1, ...]:
        return (
            self.source_plan,
            self.parent_ownership,
            self.model,
            *(() if self.subset_source is None else (self.subset_source,)),
            *(
                ()
                if self.subset_ownership is None
                else (self.subset_ownership,)
            ),
            *self.bridges,
            *self.days,
        )


def _manifest(
    shard: TrainingWeightCandidateShard,
) -> tuple[TrainingWeightCandidateShardManifestV1, dict[str, bytes]]:
    if type(shard) is not TrainingWeightCandidateShard:
        raise TypeError("persistence requires a typed research candidate shard")
    if (
        len(shard.days) + len(shard.refusals) > MAX_SHARD_DAYS
        or len(shard.degradation.bridges) > 42
    ):
        raise ValueError("research shard inventory exceeds bounded schedule")
    payloads: dict[str, bytes] = {}
    total = 0

    def component(
        kind: str, value: TrainingContract
    ) -> TrainingWeightComponentV1:
        nonlocal total
        payload = value.to_json().encode("ascii")
        total += len(payload)
        if (
            len(payload) > MAX_TRAINING_BYTES
            or total > MAX_SHARD_TOTAL_BYTES
            or len(payloads) >= MAX_SHARD_COMPONENTS
        ):
            raise ValueError("research shard aggregate work budget exceeded")
        digest = hashlib.sha256(payload).hexdigest()
        path = f"weight-{kind}-{digest}.json"
        if path in payloads:
            raise ValueError("research shard repeats a component")
        payloads[path] = payload
        return TrainingWeightComponentV1(
            kind,
            value.artifact_id,
            TrainingWeightFileV1(path, len(payload), digest),
        )

    degradation = shard.degradation
    manifest = TrainingWeightCandidateShardManifestV1(
        degradation.source_plan.evidence_kind,
        shard.shard_id,
        component("source-plan", degradation.source_plan),
        component("parent-ownership", degradation.parent_ownership),
        (
            None
            if degradation.subset_source is None
            else component("subset-source", degradation.subset_source)
        ),
        (
            None
            if degradation.subset_ownership is None
            else component("subset-ownership", degradation.subset_ownership)
        ),
        tuple(component("bridge", value) for value in degradation.bridges),
        degradation.refusals,
        component("model", shard.model),
        tuple(component("day", value) for value in shard.days),
        shard.refusals,
    )
    if manifest.evidence_kind is not shard.model.evidence_kind:
        raise ValueError("research shard mixes fixture and empirical evidence")
    return manifest, payloads


def _sync(directory: Path) -> None:
    if os.name != "nt":
        descriptor = os.open(
            directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _publish(path: Path, payload: bytes) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=".weight-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            if stream.write(payload) != len(payload):
                raise OSError("short research component write")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            if read_training_regular(path, len(payload)) != payload:
                raise ValueError(
                    "existing research artifact differs from canonical bytes"
                )
        else:
            _sync(path.parent)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def write_training_weight_candidate_shard(
    shard: TrainingWeightCandidateShard,
    directory: str | Path,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> Path:
    """Preflight and replay, then publish no-clobber components/root last.

    Existing exact components are reusable; mismatches and symlinks refuse.
    No successful root manifest is published for an interrupted component set.
    """
    manifest, payloads = _manifest(shard)
    replay_training_weight_candidate_shard(shard, approval=approval)
    root = Path(directory)
    if root.is_symlink():
        raise ValueError("research artifact directory cannot be a symlink")
    existed = root.exists()
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("research artifact directory is not a real directory")
    if not existed:
        _sync(root.parent)
    for name, payload in sorted(payloads.items()):
        _publish(root / name, payload)
    payload = manifest.to_json().encode("ascii")
    digest = hashlib.sha256(payload).hexdigest()
    target = root / f"weight-candidate-shard-{digest}.json"
    _publish(target, payload)
    return target


def read_training_weight_candidate_shard(
    path: str | Path,
    *,
    approval: TrainingWeightExecutionApprovalV1 | None = None,
) -> TrainingWeightCandidateShard:
    """Bound all component reads, verify canonical bytes, then replay all inputs."""
    source = Path(path)
    if source.parent.is_symlink():
        raise ValueError("research artifact directory cannot be a symlink")
    payload = read_training_regular(source, MAX_TRAINING_BYTES)
    digest = hashlib.sha256(payload).hexdigest()
    if source.name != f"weight-candidate-shard-{digest}.json":
        raise ValueError("research shard filename/content digest differs")
    manifest: TrainingWeightCandidateShardManifestV1 = (
        TrainingWeightCandidateShardManifestV1.from_json(
            payload.decode("ascii")
        )
    )
    if manifest.to_json().encode("ascii") != payload:
        raise ValueError("research shard manifest is not canonical")
    if manifest.evidence_kind is WeightEvidenceKind.PREREGISTERED:
        require_training_weight_approval(approval, "candidate-generation")

    # Manifest validation preflights all declared sizes before the first read.
    def component(
        reference: TrainingWeightComponentV1, contract: type[_T]
    ) -> _T:
        data = read_training_regular(
            source.parent / reference.file.path, reference.file.size_bytes
        )
        if (
            len(data) != reference.file.size_bytes
            or hashlib.sha256(data).hexdigest() != reference.file.sha256
        ):
            raise ValueError("research component differs from exact bytes")
        value: _T = contract.from_json(data.decode("ascii"))
        if (
            value.to_json().encode("ascii") != data
            or value.artifact_id != reference.component_id
        ):
            raise ValueError(
                "research component is noncanonical or identity differs"
            )
        return value

    plan = component(manifest.source_plan, TrainingWeightSourcePlanV1)
    model = component(manifest.model, TrainingWeightModelV1)
    if (
        plan.evidence_kind is not manifest.evidence_kind
        or model.evidence_kind is not manifest.evidence_kind
    ):
        raise ValueError("research shard cannot promote fixture evidence")
    degradation = TrainingWeightDegradation(
        plan,
        component(manifest.parent_ownership, TrainingOwnershipV1),
        (
            None
            if manifest.subset_source is None
            else component(manifest.subset_source, TrainingSourceV1)
        ),
        (
            None
            if manifest.subset_ownership is None
            else component(manifest.subset_ownership, TrainingOwnershipV1)
        ),
        tuple(
            component(item, TrainingWeightDayBridgeV1)
            for item in manifest.bridges
        ),
        manifest.source_refusals,
    )
    shard = TrainingWeightCandidateShard(
        degradation,
        model,
        manifest.shard_id,
        tuple(
            component(item, TrainingWeightCandidateDayV1)
            for item in manifest.days
        ),
        manifest.refusals,
    )
    return replay_training_weight_candidate_shard(shard, approval=approval)
