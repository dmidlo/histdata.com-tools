"""Native-receipted wide children and value-free no-clobber layout storage.

Projection skips only unselected persisted value groups. Every zero-column
control is read through the native reader, which replays all declared sources
and current rights. Recomputed content is not a claim that unopened persisted
groups or their receipts were verified. Full reads open every native group.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from .training_join_artifacts import (
    read_training_join_artifact,
    write_training_join_artifact,
)
from .training_join_contracts import JoinInformationMode, TrainingJoinBatchV1
from .training_lineage import read_training_regular
from .training_provider_policy import (
    training_provider_subject,
    verify_training_policy_receipt,
)
from .training_wide_contracts import TrainingWideChildV1, TrainingWideManifestV1
from .training_wide_views import (
    TrainingWideViewV1,
    _child,
    _current,
    _project_verified,
    _projection_names,
    _restore_plan,
    materialize_training_wide_view,
    replay_training_wide_view,
)


@dataclass(frozen=True, slots=True)
class TrainingWideProjectionV1:
    """Fresh value result and honest persisted-I/O scope, not an authority."""

    manifest: TrainingWideManifestV1
    columns: tuple[str, ...]
    records: tuple[dict[str, object], ...]
    verified_control_files: tuple[str, ...]
    verified_value_files: tuple[str, ...]
    verified_native_bytes: int
    layout_bytes: int

    def storage_report(self) -> dict[str, object]:
        total = self.layout_bytes + self.verified_native_bytes
        return {
            "schema_version": "histdatacom.training-wide-storage-report.v1",
            "logical_columns": len(self.manifest.columns),
            "projected_columns": len(self.columns),
            "rows": len(self.records),
            "layout_bytes": self.layout_bytes,
            "verified_native_bytes": self.verified_native_bytes,
            "verified_bytes_per_output_row": (
                total / len(self.records) if self.records else None
            ),
            "verified_control_files": list(self.verified_control_files),
            "verified_value_files": list(self.verified_value_files),
            "all_value_files_verified": len(self.verified_value_files)
            == len(self.manifest.groups),
            "all_declared_sources_replayed": True,
            "scope": "actual_opened_canonical_files_excluding_native_policy_sidecars_and_source_replay_io",
            "nonclaims": [
                "not_peak_memory_or_wall_time",
                "not_unopened_group_integrity",
                "not_compression_estimate",
            ],
        }


def write_training_wide_artifact(
    view: TrainingWideViewV1, directory: str | Path
) -> Path:
    """Publish unchanged native children first, then their value-free layout.

    A failure may leave valid native children; it never repairs missing native
    receipts or overwrites conflicting bytes. Layout hashes confer no rights.
    """
    expected = replay_training_wide_view(view)
    batches = expected.controls + expected.groups
    _current(batches, retention=True)
    root = Path(directory)
    for batch in batches:
        _current(batches, retention=True)
        target = write_training_join_artifact(batch, root)
        if target.name != _child(batch).filename:
            raise ValueError(
                "native writer returned an unexpected child identity"
            )
    data = expected.manifest.to_json().encode()
    target = (
        root / f"training-wide-manifest-{hashlib.sha256(data).hexdigest()}.json"
    )
    temporary: Path | None = None
    try:
        _current(batches, retention=True)
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=root, prefix=".wide-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            _current(batches, retention=True)
            stream.write(data)
            stream.flush()
            _current(batches, retention=True)
            os.fsync(stream.fileno())
        _current(batches, retention=True)
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            if read_training_regular(target) != data:
                raise ValueError(
                    "existing wide layout conflicts with canonical bytes"
                )
        else:
            if os.name != "nt":
                _current(batches, retention=True)
                descriptor = os.open(
                    root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                )
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink()
    _current(batches, retention=True)
    return target


def _read_child(
    root: Path, child: TrainingWideChildV1, mode: JoinInformationMode
) -> TrainingJoinBatchV1:
    # Enforce the declared aggregate budget before parsing/replaying a child.
    # The subsequent native read independently checks bytes/receipt/rights;
    # exact post-read equality closes a replacement between these two reads.
    path = root / child.filename
    data = read_training_regular(path, child.byte_count)
    if (
        len(data) != child.byte_count
        or hashlib.sha256(data).hexdigest() != child.file_sha256
    ):
        raise ValueError("wide child byte size/hash differs from layout")
    batch = read_training_join_artifact(path, information_mode=mode)
    if _child(batch) != child:
        raise ValueError("wide child identity changed during native replay")
    # This is a new container, never a historical receipt-free native artifact.
    # Keep standalone V1 reader compatibility, but require every provider child
    # here to retain the receipt that its unchanged native writer published.
    verify_training_policy_receipt(
        training_provider_subject(batch), path, required=True
    )
    return batch


def read_training_wide_artifact(
    path: str | Path,
    *,
    information_mode: JoinInformationMode,
    columns: tuple[str, ...] | None = None,
) -> TrainingWideProjectionV1:
    """Verify selected persisted groups plus *all* source plans/current rights.

    Passing no projection opens all group bytes and their native receipts.
    Passing () still verifies all controls and sources but opens no value file.
    Source replay remains a cost even for narrow column projections.
    """
    if type(information_mode) is not JoinInformationMode:
        raise TypeError(
            "wide artifact reader requires explicit information mode"
        )
    source = Path(path)
    data = read_training_regular(source)
    if (
        source.name
        != f"training-wide-manifest-{hashlib.sha256(data).hexdigest()}.json"
    ):
        raise ValueError("wide layout filename does not bind actual bytes")
    manifest = TrainingWideManifestV1.from_json(data.decode())
    if manifest.to_json().encode() != data:
        raise ValueError("wide layout is not canonical")
    selected = _projection_names(manifest, columns)
    controls = tuple(
        _read_child(source.parent, c.child, information_mode)
        for c in manifest.controls
    )
    expected = materialize_training_wide_view(_restore_plan(manifest, controls))
    if expected.manifest != manifest:
        raise ValueError(
            "wide layout differs from complete native source replay"
        )
    opened = []
    native_bytes = sum(c.child.byte_count for c in manifest.controls)
    for group, batch in zip(manifest.groups, expected.groups):
        if not set(group.columns).intersection(selected):
            continue
        actual = _read_child(source.parent, group.child, information_mode)
        if actual != batch or _child(actual) != group.child:
            raise ValueError(
                "wide persisted value group differs from exact replay"
            )
        opened.append(group.child.filename)
        native_bytes += group.child.byte_count
    records = _project_verified(expected, selected)
    _current(expected.controls + expected.groups)
    return TrainingWideProjectionV1(
        manifest,
        selected,
        records,
        tuple(c.child.filename for c in manifest.controls),
        tuple(opened),
        native_bytes,
        len(data),
    )
